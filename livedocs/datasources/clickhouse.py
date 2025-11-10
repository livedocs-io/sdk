from __future__ import annotations

from typing import Callable

import clickhouse_connect
import polars as pl

from livedocs.types import (
    DBSaveConfig,
    ElementDataSource,
    LivedocsResult,
    QueryResult,
    QueryResultMetadata,
)
from livedocs.utils.clickhouse import write_df_to_clickhouse
from livedocs.utils.common import sanitize_sensitive_data


def query(
    query: str,
    datasource: ElementDataSource,
    get_database_details: Callable[[str], tuple[object, dict]],
) -> tuple[pl.DataFrame, tuple]:
    try:
        db_connector_id = datasource["database_info"]["database_connector_id"]
        _, parsed_credentials = get_database_details(db_connector_id)
    except KeyError as e:
        raise ValueError(f"Missing required information: {e}")

    try:
        client = clickhouse_connect.get_client(
            host=parsed_credentials["host"],
            port=parsed_credentials["port"],
            user=parsed_credentials["user_name"],
            password=parsed_credentials["password"],
            secure=True,
        )
        result = client.query(query)
        if result.result_set:
            columns = result.column_names
            df = pl.DataFrame(result.result_set, schema=columns)
        else:
            df = pl.DataFrame()

        return df, tuple(zip(result.column_names, result.column_types))

    except Exception as e:
        raise RuntimeError(sanitize_sensitive_data(f"Error querying Clickhouse: {e}"))


def write_to_clickhouse(
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
        qualified_table_name = (
            f"{save_config['schema_name']}.{save_config['table_name']}"
        )
        client = clickhouse_connect.get_client(
            host=parsed_credentials["host"],
            port=parsed_credentials["port"],
            user=parsed_credentials["user_name"],
            password=parsed_credentials["password"],
            secure=True,
        )

        result = write_df_to_clickhouse(
            df,
            client,
            qualified_table_name,
            save_config["table_is_new"],
            save_config["write_mode"],
        )

        if result["error"]:
            raise RuntimeError(
                sanitize_sensitive_data(
                    f"Error writing to Clickhouse: {result['error']}"
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
