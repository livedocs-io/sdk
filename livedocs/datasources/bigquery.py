from __future__ import annotations

import json
from typing import Callable

import polars as pl
from google.cloud import bigquery
from google.oauth2 import service_account

from livedocs.types import (
    DBSaveConfig,
    ElementDataSource,
    LivedocsResult,
    QueryResult,
    QueryResultMetadata,
)
from livedocs.utils.bigquery import write_df_to_bigquery
from livedocs.utils.common import sanitize_sensitive_data


def _get_outer_credentials(
    datasource: ElementDataSource,
    get_database_details: Callable[[str], tuple[object, dict]],
) -> dict:
    try:
        db_connector_id = datasource["database_info"]["database_connector_id"]
        _, outer_parsed = get_database_details(db_connector_id)
    except KeyError as e:
        raise ValueError(f"Missing required information: {e}")
    return outer_parsed


def _parse_service_account(outer_parsed: dict) -> dict:
    try:
        return json.loads(outer_parsed["service_account_key"])
    except json.JSONDecodeError as e:
        raise ValueError(f"Error parsing connection details: {e}")


def query(
    query: str,
    datasource: ElementDataSource,
    get_database_details: Callable[[str], tuple[object, dict]],
) -> tuple[pl.DataFrame, object]:
    outer_parsed = _get_outer_credentials(datasource, get_database_details)
    service_account_parsed = _parse_service_account(outer_parsed)

    try:
        credentials = service_account.Credentials.from_service_account_info(
            service_account_parsed
        )
        client = bigquery.Client(
            credentials=credentials, project=outer_parsed["project_id"]
        )

        query_job = client.query(query)
        schema = query_job.result().schema
        df_pointer = query_job.to_dataframe(create_bqstorage_client=True)
        df_polars = pl.from_pandas(df_pointer)
    except Exception as e:
        raise RuntimeError(sanitize_sensitive_data(f"Error querying BigQuery: {e}"))

    return df_polars, schema


def write_to_bigquery(
    df: pl.DataFrame,
    save_config: DBSaveConfig,
    get_database_details: Callable[[str], tuple[object, dict]],
) -> LivedocsResult:
    try:
        db_connector_id = save_config["database_id"]
        _, outer_parsed = get_database_details(db_connector_id)
    except KeyError as e:
        raise ValueError(f"Missing required information: {e}")

    service_account_parsed = _parse_service_account(outer_parsed)

    try:
        qualified_table_name = f"{outer_parsed['project_id']}.{save_config['schema_name']}.{save_config['table_name']}"

        credentials = service_account.Credentials.from_service_account_info(
            service_account_parsed
        )
        client = bigquery.Client(
            credentials=credentials, project=outer_parsed["project_id"]
        )

        result = write_df_to_bigquery(
            df,
            client,
            qualified_table_name,
            save_config["table_is_new"],
            save_config["write_mode"],
        )

        if result["error"]:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error writing to BigQuery: {result['error']}")
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
