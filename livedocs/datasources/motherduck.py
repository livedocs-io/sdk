from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Callable
from urllib.parse import quote_plus
from contextlib import contextmanager

import duckdb
import polars as pl

from livedocs.types import (
    DBSaveConfig,
    ElementDataSource,
    LivedocsResult,
    QueryResult,
    QueryResultMetadata,
)
from livedocs.utils.common import sanitize_sensitive_data

_INPUT_VIEW_NAME = "_livedocs_motherduck_input"


def _extract_details(connection_details: dict) -> dict:
    database = connection_details.get("database")
    token = connection_details.get("token")

    if not token:
        raise ValueError("Missing Motherduck connection detail: 'token'")

    return {
        "database": database,
        "token": token,
        "default_schema": connection_details.get("default_schema"),
    }


def _build_connection_uri(details: dict) -> str:
    database = details.get("database") or ""
    token = quote_plus(details["token"])
    base = f"md:{database}" if database else "md:"
    separator = "&" if "?" in base else "?"
    return f"{base}{separator}motherduck_token={token}"


@contextmanager
def _motherduck_connection(details: dict):
    try:
        conn = duckdb.connect(_build_connection_uri(details))
        try:
            conn.execute("LOAD motherduck")
        except Exception:
            pass
        yield conn
    finally:
        conn.close()


def _quote_identifier(identifier: str) -> str:
    return f'"{identifier.replace("\"", "\"\"")}"'


def _format_table_name(schema_name: str, table_name: str) -> str:
    if schema_name:
        return f"{_quote_identifier(schema_name)}.{_quote_identifier(table_name)}"
    return _quote_identifier(table_name)


def _table_exists(
    conn: duckdb.DuckDBPyConnection, schema_name: str, table_name: str
) -> bool:
    schema = schema_name or "main"
    result = conn.execute(
        """
        SELECT COUNT(*) > 0
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_name = ?
        """,
        [schema, table_name],
    ).fetchone()
    return bool(result[0]) if result else False


def _prepare_write_result(
    conn: duckdb.DuckDBPyConnection, qualified_table: str
) -> pl.DataFrame:
    schema_relation = conn.sql(f"SELECT * FROM {qualified_table} LIMIT 0")
    columns = schema_relation.columns or []
    dtypes = [str(dtype) for dtype in schema_relation.dtypes or []]

    preview_relation = conn.sql(f"SELECT * FROM {qualified_table} LIMIT 20")
    preview_df = preview_relation.pl()

    if not columns:
        return preview_df

    types_row = {column: dtype for column, dtype in zip(columns, dtypes)}
    types_schema = {column: pl.Utf8 for column in columns}
    types_df = pl.DataFrame([types_row], schema=types_schema)

    if preview_df.is_empty():
        return types_df

    return pl.concat([types_df, preview_df.select(columns)], how="vertical_relaxed")


def query(
    query: str,
    datasource: ElementDataSource,
    get_database_details: Callable[[str], tuple[object, dict]],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    try:
        db_connector_id = datasource["database_info"]["database_connector_id"]
        _, parsed_credentials = get_database_details(db_connector_id)
    except KeyError as e:
        raise ValueError(f"Missing required information: {e}")

    details = _extract_details(parsed_credentials)

    try:
        with _motherduck_connection(details) as conn:
            relation = conn.sql(query)
            result_df = relation.pl()
            columns = relation.columns or []
            dtypes = [str(dtype) for dtype in relation.dtypes or []]
    except Exception as e:
        raise RuntimeError(sanitize_sensitive_data(f"Error querying Motherduck: {e}"))

    schema_rows = [
        {"column_name": column, "data_type": dtype}
        for column, dtype in zip(columns, dtypes)
    ]
    schema_df = (
        pl.DataFrame(schema_rows)
        if schema_rows
        else pl.DataFrame({"column_name": [], "data_type": []})
    )

    return result_df, schema_df


def write_to_motherduck(
    df: pl.DataFrame,
    save_config: DBSaveConfig,
    get_database_details: Callable[[str], tuple[object, dict]],
) -> LivedocsResult:
    try:
        db_connector_id = save_config["database_id"]
        _, parsed_credentials = get_database_details(db_connector_id)
    except KeyError as e:
        raise ValueError(f"Missing required information: {e}")

    details = _extract_details(parsed_credentials)

    write_mode = save_config["write_mode"]
    if write_mode not in {"append", "overwrite"}:
        raise ValueError('write_mode must be either "append" or "overwrite"')

    schema_name = save_config["schema_name"] or details.get("default_schema") or ""
    table_name = save_config["table_name"]
    qualified_table = _format_table_name(schema_name, table_name)
    has_rows = df.height > 0

    try:
        with _motherduck_connection(details) as conn:
            if schema_name:
                conn.execute(
                    f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema_name)}"
                )

            try:
                conn.register(_INPUT_VIEW_NAME, df.to_pandas())
            except Exception as e:
                raise RuntimeError(
                    sanitize_sensitive_data(
                        f"Failed to stage DataFrame for Motherduck write: {e}"
                    )
                )

            try:
                table_exists = _table_exists(conn, schema_name, table_name)

                if write_mode == "overwrite":
                    conn.execute(
                        f"CREATE OR REPLACE TABLE {qualified_table} AS SELECT * FROM {_INPUT_VIEW_NAME}"
                    )
                else:
                    if not table_exists and not save_config["table_is_new"]:
                        raise ValueError(
                            f"Table {qualified_table} does not exist and table_is_new is False"
                        )

                    if not table_exists:
                        conn.execute(
                            f"CREATE TABLE {qualified_table} AS SELECT * FROM {_INPUT_VIEW_NAME} LIMIT 0"
                        )

                    if has_rows:
                        conn.execute(
                            f"INSERT INTO {qualified_table} SELECT * FROM {_INPUT_VIEW_NAME}"
                        )
                result_df = _prepare_write_result(conn, qualified_table)
            finally:
                try:
                    conn.unregister(_INPUT_VIEW_NAME)
                except Exception:
                    pass
    except Exception as e:
        raise RuntimeError(sanitize_sensitive_data(f"DBSave Error: {e}"))

    output = QueryResult(
        data=result_df,
        metadata=QueryResultMetadata(
            limit=50,
            offset=0,
            total_rows=df.height,
            run_date=datetime.now(timezone.utc),
            cache_info=None,
        ),
    )
    return LivedocsResult(output)
