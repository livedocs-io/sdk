from __future__ import annotations

import re
from typing import Callable

import polars as pl
import pyarrow as pa
from databricks import sql as databricks_sql

from livedocs.types import (
    DBSaveConfig,
    ElementDataSource,
    LivedocsResult,
    QueryResult,
    QueryResultMetadata,
)
from livedocs.utils.common import sanitize_sensitive_data
from livedocs.utils.databricks import write_df_to_databricks


def _get_workspace_connection(
    parsed_credentials: dict,
) -> tuple[databricks_sql.Connection, str | None]:
    token, host, port, http_path, default_catalog = _extract_connection_details(
        parsed_credentials
    )
    connection = databricks_sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
        port=port,
    )
    return connection, default_catalog


def _extract_connection_details(
    parsed_credentials: dict,
) -> tuple[str, str, int, str, str | None]:
    token = parsed_credentials.get("token")
    jdbc_url = parsed_credentials.get("jdbc_url")

    if not token:
        raise ValueError("Missing Databricks token in connector credentials")
    if not jdbc_url:
        raise ValueError("Missing Databricks jdbc_url in connector credentials")

    host, port, http_path, default_catalog = _parse_jdbc_url(jdbc_url)
    return token, host, port, http_path, default_catalog


def _parse_jdbc_url(jdbc_url: str) -> tuple[str, int, str, str | None]:
    if not jdbc_url.startswith("jdbc:"):
        raise ValueError("Unsupported Databricks JDBC URL format")

    without_prefix = jdbc_url[len("jdbc:") :]

    if "://" not in without_prefix:
        raise ValueError("Invalid Databricks JDBC URL")

    scheme, remainder = without_prefix.split("://", 1)
    if scheme not in {"spark", "databricks"}:
        raise ValueError("Unsupported Databricks JDBC driver")

    host_section, _, params_str = remainder.partition(";")
    host_segment, default_catalog = _split_host_and_catalog(host_section)

    hostname, port = _split_host_and_port(host_segment)

    http_path = None
    for segment in params_str.split(";"):
        if "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        if key.lower() == "httppath":
            http_path = value
            break

    if not http_path:
        raise ValueError("Databricks JDBC URL missing httpPath parameter")

    if not http_path.startswith("/"):
        http_path = f"/{http_path}"

    normalized_path = http_path.rstrip("/")
    if "/warehouses/" not in normalized_path:
        raise ValueError("Unable to determine warehouse id from Databricks JDBC URL")

    return hostname, port, http_path, default_catalog


def _split_host_and_catalog(host_section: str) -> tuple[str, str | None]:
    host_port, _, catalog_part = host_section.partition("/")
    catalog = catalog_part or None
    if catalog:
        catalog = catalog.strip() or None
        if catalog and catalog.lower() == "default":
            catalog = None
    return host_port, catalog


def _split_host_and_port(host_port: str) -> tuple[str, int]:
    if ":" in host_port:
        host, port_str = host_port.split(":", 1)
        try:
            port = int(port_str)
        except ValueError as exc:
            raise ValueError("Invalid port in Databricks JDBC URL") from exc
    else:
        host = host_port
        port = 443

    host = host.strip()
    if host.startswith("https://"):
        host = host[len("https://") :]
    if host.endswith("/"):
        host = host[:-1]

    return host, port


def _build_table_identifier(catalog: str | None, schema: str | None, table: str) -> str:
    if not table:
        raise ValueError("Table name is required for Databricks operations")
    parts = [part for part in (catalog, schema, table) if part]
    return ".".join(f"`{part.replace('`', '``')}`" for part in parts)


_DECIMAL_PATTERN = re.compile(r"DECIMAL\((\d+)\s*,\s*(\d+)\)", re.IGNORECASE)


def _databricks_type_to_arrow(type_name: str) -> pa.DataType:
    normalized = (type_name or "").upper()

    if normalized in {"BOOLEAN"}:
        return pa.bool_()
    if normalized in {"TINYINT", "BYTE"}:
        return pa.int8()
    if normalized in {"SMALLINT", "SHORT"}:
        return pa.int16()
    if normalized in {"INT", "INTEGER"}:
        return pa.int32()
    if normalized in {"BIGINT", "LONG"}:
        return pa.int64()
    if normalized in {"FLOAT", "REAL", "FLOAT4"}:
        return pa.float32()
    if normalized in {"DOUBLE", "FLOAT8"}:
        return pa.float64()
    if normalized.startswith("DECIMAL") or normalized.startswith("NUMERIC"):
        match = _DECIMAL_PATTERN.match(normalized)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2))
            return pa.decimal128(precision, scale)
        return pa.decimal128(38, 18)
    if normalized in {"DATE"}:
        return pa.date32()
    if normalized in {"TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"}:
        return pa.timestamp("us")
    if normalized in {"TIME"}:
        return pa.time64("us")
    if normalized in {"BINARY", "VARBINARY"}:
        return pa.binary()

    return pa.string()


def _rows_to_arrow_table(rows: list, description: tuple) -> pa.Table | None:
    if not description:
        return None

    arrays = []
    column_names = []

    for index, desc in enumerate(description):
        column_name = desc[0]
        type_name = str(desc[1]) if len(desc) > 1 else ""
        arrow_type = _databricks_type_to_arrow(type_name)
        column_values = [row[index] for row in rows] if rows else []
        arrays.append(pa.array(column_values, type=arrow_type))
        column_names.append(column_name)

    return pa.Table.from_arrays(arrays, names=column_names)


def query(
    query: str,
    datasource: ElementDataSource,
    get_database_details: Callable[[str], tuple[object, dict]],
) -> tuple[pl.DataFrame, tuple]:
    try:
        db_connector_id = datasource["database_info"]["database_connector_id"]
        _, parsed_credentials = get_database_details(db_connector_id)
    except KeyError as error:
        raise ValueError(f"Missing required information: {error}")

    connection = None
    cursor = None
    try:
        connection, default_catalog = _get_workspace_connection(parsed_credentials)
        catalog = default_catalog

        cursor = connection.cursor()
        if catalog:
            cursor.execute(f"USE CATALOG `{catalog.replace('`', '``')}`")
        cursor.execute(query)

        description = cursor.description or ()
        columns = [desc[0] for desc in description]

        rows = cursor.fetchall() or []

        arrow_table: pa.Table | None = None
        if description:
            try:
                arrow_table = _rows_to_arrow_table(rows, description)
            except Exception:
                arrow_table = None

        if arrow_table is not None:
            df = pl.from_arrow(arrow_table)
        elif rows:
            df = pl.DataFrame(rows, schema=columns if columns else None)
        elif columns:
            df = pl.DataFrame([], schema=columns)
        else:
            df = pl.DataFrame()

        raw_schema = tuple((desc[0], str(desc[1])) for desc in description)

        return df, raw_schema
    except Exception as error:
        raise RuntimeError(
            sanitize_sensitive_data(f"Error querying Databricks: {error}")
        )
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def write_to_databricks(
    df: pl.DataFrame,
    save_config: DBSaveConfig,
    get_database_details: Callable[[str], tuple[object, dict]],
) -> LivedocsResult:
    try:
        db_connector_id = save_config["database_id"]
        _, parsed_credentials = get_database_details(db_connector_id)
    except KeyError as error:
        raise ValueError(f"Missing required information: {error}")

    connection = None
    try:
        connection, default_catalog = _get_workspace_connection(parsed_credentials)
        catalog = default_catalog

        table_identifier = _build_table_identifier(
            catalog, save_config.get("schema_name"), save_config["table_name"]
        )

        def _run(statement: str) -> None:
            cursor = connection.cursor()
            try:
                if catalog:
                    cursor.execute(f"USE CATALOG `{catalog.replace('`', '``')}`")
                cursor.execute(statement)
            finally:
                cursor.close()

        result = write_df_to_databricks(
            df,
            _run,
            table_identifier,
            create_table=save_config["table_is_new"],
            write_mode=save_config["write_mode"],
        )

        if result["error"]:
            raise RuntimeError(
                sanitize_sensitive_data(
                    f"Error writing to Databricks: {result['error']['message']}"
                )
            )

        output = QueryResult(
            data=result["result"],
            metadata=QueryResultMetadata(
                limit=50,
                offset=0,
                total_rows=result["rows_written"],
                run_date=result["run_date"],
                cache_info=None,
            ),
        )
        return LivedocsResult(output)
    except Exception as error:
        raise RuntimeError(sanitize_sensitive_data(f"DBSave Error: {error}"))
    finally:
        if connection is not None:
            connection.close()
