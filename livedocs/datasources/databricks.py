from __future__ import annotations

import re
import traceback
import uuid
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable, Sequence
from uuid import UUID

import polars as pl
import pyarrow as pa
from databricks import sql as databricks_sql
from typing_extensions import Literal

from livedocs.datasources.base import BaseDatasourceConnector
from livedocs.types import (
    DBSaveConfig,
    ElementDataSource,
    LivedocsResult,
    QueryResult,
    QueryResultMetadata,
    SchemaNode,
    SchemaNodeType,
)
from livedocs.utils.lib.internals import (
    livedocs_internal_sanitize_sensitive_data as sanitize_sensitive_data,
)


class DatabricksDatasourceConnector(BaseDatasourceConnector):
    """
    Datasource connector for Databricks.
    """

    _DECIMAL_PATTERN = re.compile(r"DECIMAL\((\d+)\s*,\s*(\d+)\)", re.IGNORECASE)

    def read(
        self,
        query: str,
        datasource: ElementDataSource,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
    ) -> tuple[pl.DataFrame, tuple[tuple[str, str], ...]]:
        try:
            if datasource["database_info"] is None:
                raise ValueError("Missing required information: 'database_info'")
            db_connector_id = datasource["database_info"]["database_connector_id"]
            _, parsed_credentials = get_database_details(db_connector_id)
        except KeyError as error:
            raise ValueError(f"Missing required information: {error}")

        connection = None
        cursor = None
        try:
            connection, default_catalog = self._get_workspace_connection(
                parsed_credentials
            )
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
                    arrow_table = self._rows_to_arrow_table(rows, description)
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

    def write(
        self,
        df: pl.DataFrame,
        save_config: DBSaveConfig,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
    ) -> LivedocsResult:
        try:
            db_connector_id = save_config["database_id"]
            _, parsed_credentials = get_database_details(db_connector_id)
        except KeyError as error:
            raise ValueError(f"Missing required information: {error}")

        connection = None
        try:
            connection, default_catalog = self._get_workspace_connection(
                parsed_credentials
            )
            catalog = default_catalog

            table_identifier = self._build_table_identifier(
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

            result = self._write_df_to_databricks(
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
                metadata=QueryResultMetadata(  # type: ignore[typeddict-item]
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

    def teardown(self) -> None:
        pass

    def _get_workspace_connection(
        self, parsed_credentials: dict[str, Any]
    ) -> tuple[databricks_sql.Connection, str | None]:
        token, host, port, http_path, default_catalog = (
            self._extract_connection_details(parsed_credentials)
        )
        connection = databricks_sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
            port=port,
        )
        return connection, default_catalog

    def _extract_connection_details(
        self, parsed_credentials: dict[str, Any]
    ) -> tuple[str, str, int, str, str | None]:
        token = parsed_credentials.get("token")
        jdbc_url = parsed_credentials.get("jdbc_url")

        if not token:
            raise ValueError("Missing Databricks token in connector credentials")
        if not jdbc_url:
            raise ValueError("Missing Databricks jdbc_url in connector credentials")

        host, port, http_path, default_catalog = self._parse_jdbc_url(jdbc_url)
        return token, host, port, http_path, default_catalog

    def _parse_jdbc_url(self, jdbc_url: str) -> tuple[str, int, str, str | None]:
        if not jdbc_url.startswith("jdbc:"):
            raise ValueError("Unsupported Databricks JDBC URL format")

        without_prefix = jdbc_url[len("jdbc:") :]

        if "://" not in without_prefix:
            raise ValueError("Invalid Databricks JDBC URL")

        scheme, remainder = without_prefix.split("://", 1)
        if scheme not in {"spark", "databricks"}:
            raise ValueError("Unsupported Databricks JDBC driver")

        host_section, _, params_str = remainder.partition(";")
        host_segment, default_catalog = self._split_host_and_catalog(host_section)

        hostname, port = self._split_host_and_port(host_segment)

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
            raise ValueError(
                "Unable to determine warehouse id from Databricks JDBC URL"
            )

        return hostname, port, http_path, default_catalog

    def _split_host_and_catalog(self, host_section: str) -> tuple[str, str | None]:
        host_port, _, catalog_part = host_section.partition("/")
        catalog = catalog_part or None
        if catalog:
            catalog = catalog.strip() or None
            if catalog and catalog.lower() == "default":
                catalog = None
        return host_port, catalog

    def _split_host_and_port(self, host_port: str) -> tuple[str, int]:
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

    def _build_table_identifier(
        self, catalog: str | None, schema: str | None, table: str
    ) -> str:
        if not table:
            raise ValueError("Table name is required for Databricks operations")
        parts = [part for part in (catalog, schema, table) if part]
        return ".".join(f"`{part.replace('`', '``')}`" for part in parts)

    def _databricks_type_to_arrow(self, type_name: str) -> pa.DataType:
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
            match = self._DECIMAL_PATTERN.match(normalized)
            if match:
                precision = int(match.group(1))
                scale = int(match.group(2))
                return pa.decimal128(precision, scale)
            return pa.decimal128(38, 18)
        if normalized in {"DATE"}:
            return pa.date32()
        if normalized in {
            "TIMESTAMP",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_TZ",
        }:
            return pa.timestamp("us")
        if normalized in {"TIME"}:
            return pa.time64("us")
        if normalized in {"BINARY", "VARBINARY"}:
            return pa.binary()

        return pa.string()

    def _rows_to_arrow_table(self, rows: list, description: tuple) -> pa.Table | None:
        if not description:
            return None

        arrays = []
        column_names = []

        for index, desc in enumerate(description):
            column_name = desc[0]
            type_name = str(desc[1]) if len(desc) > 1 else ""
            arrow_type = self._databricks_type_to_arrow(type_name)
            column_values = [row[index] for row in rows] if rows else []
            arrays.append(pa.array(column_values, type=arrow_type))
            column_names.append(column_name)

        return pa.Table.from_arrays(arrays, names=column_names)

    def _write_df_to_databricks(
        self,
        df: pl.DataFrame,
        statement_runner: Callable[[str], None],
        table_identifier: str,
        *,
        create_table: bool = False,
        write_mode: Literal["append", "overwrite"] = "append",
        chunk_size: int = 100,
    ) -> dict[str, Any]:
        """
        Write a Polars DataFrame to a Databricks SQL warehouse using INSERT statements.

        Parameters
        ----------
        df:
            The Polars DataFrame to write.
        statement_runner:
            Callable accepting a SQL statement string and executing it using the Databricks
            Statement Execution API. It must raise an exception when the statement fails.
        table_identifier:
            Fully-qualified Databricks table identifier, e.g. `catalog.schema.table`.
        create_table:
            Whether the target table should be created if it does not exist.
        write_mode:
            Either `append` or `overwrite`.
        chunk_size:
            Number of rows to include per INSERT statement batch.

        Returns
        -------
        dict
            Structure compatible with the other datasource write helpers used in vm-lib.
        """
        output = {
            "result": pl.DataFrame(),
            "error": {},
            "rows_written": 0,
            "run_date": datetime.now(timezone.utc),
        }

        try:
            if write_mode not in ("append", "overwrite"):
                raise ValueError('write_mode must be either "append" or "overwrite"')

            if df.is_empty():
                return output

            quoted_columns = [self._quote_identifier(col) for col in df.columns]
            databricks_types = [
                self._map_polars_to_databricks_type(dtype) for dtype in df.dtypes
            ]

            if create_table:
                columns_with_types = [
                    f"{name} {dtype}"
                    for name, dtype in zip(quoted_columns, databricks_types)
                ]
                create_sql = (
                    f"CREATE TABLE IF NOT EXISTS {table_identifier} "
                    f"({', '.join(columns_with_types)})"
                )
                statement_runner(create_sql)

            if write_mode == "overwrite":
                truncate_sql = f"TRUNCATE TABLE {table_identifier}"
                statement_runner(truncate_sql)

            rows = df.to_dicts()
            for chunk in self._chunked(rows, chunk_size):
                values_clause = ", ".join(
                    "("
                    + ", ".join(self._format_literal(row[col]) for col in df.columns)
                    + ")"
                    for row in chunk
                )
                insert_sql = (
                    f"INSERT INTO {table_identifier} "
                    f"({', '.join(quoted_columns)}) VALUES {values_clause}"
                )
                statement_runner(insert_sql)

            types_row = {
                column: databricks_type
                for column, databricks_type in zip(df.columns, databricks_types)
            }
            types_df = pl.DataFrame([types_row])
            preview_df = df.head(19).select(
                [pl.col(column).cast(pl.Utf8) for column in df.columns]
            )

            output["result"] = pl.concat([types_df, preview_df], how="vertical")
            output["rows_written"] = df.height
            return output
        except Exception as error:
            output["error"] = {
                "message": str(error),
                "stacktrace": traceback.format_exc(),
            }
            return output

    def process_schema(
        self, schema: Sequence[tuple[str, str]] | None
    ) -> dict[str, str]:
        """
        Processes Databricks schema information returned from the query endpoint and
        normalizes it to the Livedocs schema format.

        Parameters
        ----------
        schema:
            Sequence of `(column_name, type_text)` tuples (may be `None` when the query
            returned no manifest information).

        Returns
        -------
        dict[str, str]
            Mapping of column name to one of the Livedocs logical types:
            `NUMBER`, `DATE`, or `STRING`.
        """
        if not schema:
            return {}

        processed_schema: dict[str, str] = {}
        for column_name, type_hint in schema:
            normalized_type = self._normalize_databricks_type(type_hint)
            processed_schema[column_name] = normalized_type

        return processed_schema

    def _normalize_databricks_type(self, raw_type: str | None) -> str:
        if not raw_type:
            return "STRING"

        type_upper = raw_type.upper()

        if any(
            token in type_upper
            for token in (
                "INT",
                "LONG",
                "SHORT",
                "BYTE",
                "DOUBLE",
                "FLOAT",
                "DECIMAL",
                "NUMERIC",
            )
        ):
            return "NUMBER"
        if any(token in type_upper for token in ("DATE", "TIMESTAMP", "TIME")):
            return "DATE"
        if "BOOLEAN" in type_upper:
            return "NUMBER"
        return "STRING"

    def _quote_identifier(self, identifier: str) -> str:
        escaped = identifier.replace("`", "``")
        return f"`{escaped}`"

    def _chunked(self, iterable: Iterable[dict], size: int) -> Iterable[list[dict]]:
        chunk: list[dict] = []
        for item in iterable:
            chunk.append(item)
            if len(chunk) == size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    def _map_polars_to_databricks_type(self, polars_type: pl.DataType) -> str:
        type_str = str(polars_type).lower()

        if "int" in type_str:
            return "BIGINT"
        if "float" in type_str:
            return "DOUBLE"
        if "decimal" in type_str:
            return "DECIMAL(38, 18)"
        if "date" in type_str and "time" not in type_str:
            return "DATE"
        if "datetime" in type_str or "time" in type_str:
            return "TIMESTAMP"
        if "bool" in type_str:
            return "BOOLEAN"
        if "binary" in type_str:
            return "BINARY"
        return "STRING"

    def _format_literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float, Decimal)):
            if isinstance(value, float) and (value != value):  # NaN check
                return "NULL"
            if isinstance(value, float) and value in (float("inf"), float("-inf")):
                return "NULL"
            return str(value)
        if isinstance(value, datetime):
            return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='microseconds')}'"
        if isinstance(value, date):
            return f"DATE '{value.isoformat()}'"
        if isinstance(value, time):
            return f"TIME '{value.isoformat(timespec='microseconds')}'"
        if isinstance(value, bytes):
            hex_str = value.hex()
            return f"X'{hex_str}'"
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def _get_livedocs_type(self, databricks_type: str) -> str:
        """
        Map Databricks type to LivedocsStandardType.
        Returns: "NUMBER", "DATE", "BOOLEAN", or "STRING"
        """
        type_upper = (databricks_type or "").upper()

        numeric_types = [
            "TINYINT",
            "BYTE",
            "SMALLINT",
            "SHORT",
            "INT",
            "INTEGER",
            "BIGINT",
            "LONG",
            "FLOAT",
            "REAL",
            "FLOAT4",
            "DOUBLE",
            "FLOAT8",
            "DECIMAL",
            "NUMERIC",
        ]

        date_types = [
            "DATE",
            "TIMESTAMP",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_TZ",
            "TIME",
        ]

        boolean_types = ["BOOLEAN", "BOOL"]

        if any(nt in type_upper for nt in numeric_types):
            return "NUMBER"
        elif any(dt in type_upper for dt in date_types):
            return "DATE"
        elif any(bt in type_upper for bt in boolean_types):
            return "BOOLEAN"
        else:
            return "STRING"

    def get_schema(
        self, connector_id: str, connection_details: dict[str, Any]
    ) -> list[SchemaNode]:
        """
        Fetch schema information from Databricks and return as list of schema nodes.

        Args:
            connector_id: The connector ID to use for schema nodes
            connection_details: Dictionary containing connection details

        Returns:
            List of SchemaNode objects
        """
        nodes: list[SchemaNode] = []
        now = datetime.now(timezone.utc)

        # Get connection details
        connection, default_catalog = self._get_workspace_connection(connection_details)
        catalog_name = default_catalog or "hive_metastore"

        # Create catalog node (level 0) - Databricks uses catalogs as the top level
        catalog_node_id = uuid.uuid4()
        catalog_path = catalog_name
        nodes.append(
            SchemaNode(
                id=catalog_node_id,
                connector_id=UUID(connector_id),
                parent_id=None,
                path=catalog_path,
                type=SchemaNodeType.DATABASE,
                name=catalog_name,
                data_type=None,
                livedocs_type=None,
                description=None,
                level=0,
                metadata={},
                created_at=now,
                updated_at=now,
            )
        )

        cursor = None
        try:
            cursor = connection.cursor()
            if catalog_name:
                cursor.execute(f"USE CATALOG `{catalog_name.replace('`', '``')}`")

            # Query schema details from information_schema
            # Databricks uses INFORMATION_SCHEMA similar to standard SQL
            schema_details_query = """
                SELECT 
                    table_catalog,
                    table_schema,
                    table_name,
                    column_name,
                    data_type,
                    table_type,
                    ordinal_position
                FROM 
                    information_schema.columns
                WHERE 
                    table_catalog = ?
                    AND table_schema NOT IN ('information_schema')
                ORDER BY 
                    table_schema, table_name, ordinal_position;
            """

            cursor.execute(schema_details_query, [catalog_name])
            result_rows = cursor.fetchall()

            schema_node_ids: dict[str, UUID] = {}  # "schemaName" -> nodeId
            table_node_ids: dict[str, UUID] = {}  # "schemaName.tableName" -> nodeId

            for row in result_rows:
                table_catalog = row[0]
                table_schema = row[1]
                table_name = row[2]
                column_name = row[3]
                data_type = row[4]
                table_type = row[5]

                # Create or get schema node (level 1)
                schema_node_id = schema_node_ids.get(table_schema)
                schema_path = f"{catalog_path}/{table_schema}"
                if not schema_node_id:
                    schema_node_id = uuid.uuid4()
                    nodes.append(
                        SchemaNode(
                            id=schema_node_id,
                            connector_id=UUID(connector_id),
                            parent_id=catalog_node_id,
                            path=schema_path,
                            type=SchemaNodeType.SCHEMA,
                            name=table_schema,
                            data_type=None,
                            livedocs_type=None,
                            description=None,
                            level=1,
                            metadata={},
                            created_at=now,
                            updated_at=now,
                        )
                    )
                    schema_node_ids[table_schema] = schema_node_id

                # Create or get table/view node (level 2)
                table_key = f"{table_schema}.{table_name}"
                table_node_id = table_node_ids.get(table_key)
                table_path = f"{schema_path}/{table_name}"
                node_type = (
                    SchemaNodeType.VIEW
                    if table_type == "VIEW"
                    else SchemaNodeType.TABLE
                )

                if not table_node_id:
                    table_node_id = uuid.uuid4()
                    nodes.append(
                        SchemaNode(
                            id=table_node_id,
                            connector_id=UUID(connector_id),
                            parent_id=schema_node_id,
                            path=table_path,
                            type=node_type,
                            name=table_name,
                            data_type=None,
                            livedocs_type=None,
                            description=None,
                            level=2,
                            metadata={
                                "table_type": table_type,
                                "database_type": "databricks",
                                "schema_name": table_schema,
                                "database_name": catalog_name,
                            },
                            created_at=now,
                            updated_at=now,
                        )
                    )
                    table_node_ids[table_key] = table_node_id

                # Create column node (level 3)
                column_node_id = uuid.uuid4()
                column_path = f"{table_path}/{column_name}"
                nodes.append(
                    SchemaNode(
                        id=column_node_id,
                        connector_id=UUID(connector_id),
                        parent_id=table_node_id,
                        path=column_path,
                        type=SchemaNodeType.COLUMN,
                        name=column_name,
                        data_type=data_type if data_type else None,
                        livedocs_type=(
                            self._get_livedocs_type(data_type) if data_type else None
                        ),
                        description=None,
                        level=3,
                        metadata={},
                        created_at=now,
                        updated_at=now,
                    )
                )

        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error fetching Databricks schema: {e}")
            )
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

        return nodes
