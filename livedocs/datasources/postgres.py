from __future__ import annotations

from typing import Callable

import polars as pl
import psycopg
from psycopg.abc import Query
from psycopg.rows import dict_row

from livedocs.datasources.base import BaseDatasourceConnector
from livedocs.types import (
    DBSaveConfig,
    ElementDataSource,
    LivedocsResult,
    QueryResult,
    QueryResultMetadata,
)
from livedocs.utils.common import sanitize_sensitive_data
from livedocs.utils.postgres import (
    _create_postgres_connection_url,
    _schema_from_description,
    _write_df_to_postgres,
)


class PostgresDatasourceConnector(BaseDatasourceConnector):
    """
    Datasource connector for PostgreSQL.
    """

    def init(
        self, get_database_details: Callable[[str], tuple[object, dict[str, str]]]
    ) -> Any:
        return self._connection

    def read(
        self,
        query: Query,
        datasource: ElementDataSource,
        get_database_details: Callable[[str], tuple[object, dict]],
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        try:
            connection_string = get_connection_string(datasource, get_database_details)
        except ValueError as e:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error building PostgreSQL connection: {e}")
            )

        schema_rows: list[dict[str, str]] = []

        try:
            with psycopg.connect(connection_string) as conn:
                with conn.cursor(row_factory=dict_row) as cursor:
                    _ = cursor.execute(query)
                    rows = cursor.fetchall()
                    description = cursor.description or ()
                    column_names = [desc.name for desc in description]

                schema_rows = _schema_from_description(conn, description)
        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error executing PostgreSQL query: {e}")
            )

        schema_df = (
            pl.DataFrame(schema_rows)
            if schema_rows
            else pl.DataFrame({"column_name": [], "data_type": []})
        )

        if rows:
            result_df = pl.DataFrame(rows)
            if column_names:
                result_df = result_df.select(column_names)
        elif column_names:
            result_df = pl.DataFrame({column: [] for column in column_names})
        else:
            result_df = pl.DataFrame()

        return result_df, schema_df

    def write(
        self,
        df: pl.DataFrame,
        save_config: DBSaveConfig,
        get_database_details: Callable[[str], tuple[object, dict[str, str]]],
    ) -> LivedocsResult:
        pass

    def teardown(self) -> None:
        pass


def build_connection_string(parsed_credentials: dict) -> str:
    try:
        if parsed_credentials.get("connect_using") == "url":
            return parsed_credentials["connection_url"]
        return _create_postgres_connection_url(parsed_credentials)
    except KeyError as e:
        raise ValueError(f"Missing required database connection detail: {e}")


def get_connection_string(
    datasource: ElementDataSource,
    get_database_details: Callable[[str], tuple[object, dict]],
) -> str:
    try:
        db_connector_id = datasource["database_info"]["database_connector_id"]
        _, parsed_credentials = get_database_details(db_connector_id)
    except KeyError as e:
        raise ValueError(f"Missing required information: {e}")

    return build_connection_string(parsed_credentials)


def write_to_postgres(
    df: pl.DataFrame,
    save_config: DBSaveConfig,
    get_database_details: Callable[[str], tuple[object, dict]],
) -> LivedocsResult:
    try:
        db_connector_id = save_config["database_id"]
        _, parsed_credentials = get_database_details(db_connector_id)
    except KeyError as e:
        raise ValueError(f"Missing required information: {e}")

    try:
        connection_string = build_connection_string(parsed_credentials)
    except ValueError as e:
        raise RuntimeError(
            sanitize_sensitive_data(f"Error building PostgreSQL connection: {e}")
        )

    try:
        qualified_table_name = (
            f"{save_config['schema_name']}.{save_config['table_name']}"
        )
        result = _write_df_to_postgres(
            df,
            connection_string,
            qualified_table_name,
            save_config["table_is_new"],
            save_config["write_mode"],
        )

        if result["error"]:
            raise RuntimeError(
                sanitize_sensitive_data(
                    f"Error writing to PostgreSQL: {result['error']}"
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
        payload = LivedocsResult(output)
        return payload
    except Exception as e:
        raise RuntimeError(sanitize_sensitive_data(f"DBSave Error: {e}"))
