from __future__ import annotations

from typing import Callable

import polars as pl
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from livedocs.types import (
    DBSaveConfig,
    ElementDataSource,
    LivedocsResult,
    QueryResult,
    QueryResultMetadata,
)
from livedocs.utils.common import sanitize_sensitive_data
from livedocs.utils.snowflake import write_df_to_snowflake


def _create_connection(parsed_credentials: dict):
    if parsed_credentials.get("auth_type") == "username_password":
        return snowflake.connector.connect(
            user=parsed_credentials["username"],
            password=parsed_credentials["password"],
            account=parsed_credentials["host"],
            database=parsed_credentials["database"],
            session_parameters={
                "QUERY_TAG": "LivedocsQuery",
            },
        )
    if parsed_credentials.get("auth_type") == "service_account_key":
        pem_key_from_ui = parsed_credentials["service_account_key"].strip()

        private_key = serialization.load_pem_private_key(
            pem_key_from_ui.encode("utf-8"),
            password=None,
            backend=default_backend(),
        )

        private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        account = (
            parsed_credentials["host"]
            .replace(".snowflakecomputing.com", "")
            .replace("https://", "")
            .replace("http://", "")
        )

        return snowflake.connector.connect(
            account=account,
            user=parsed_credentials["service_account_username"],
            private_key=private_key_der,
            database=parsed_credentials["database"],
        )

    raise ValueError("Unsupported authentication type")


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

    connection = None
    try:
        connection = _create_connection(parsed_credentials)
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()

        if result:
            columns = [desc[0] for desc in cursor.description]
            df = pl.DataFrame(result, schema=columns)
        else:
            df = pl.DataFrame()

        return df, cursor.description
    except Exception as e:
        raise RuntimeError(sanitize_sensitive_data(f"Error querying Snowflake: {e}"))
    finally:
        if connection is not None:
            connection.close()


def write_to_snowflake(
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
        qualified_table_name = f"{parsed_credentials['database']}.{save_config['schema_name']}.{save_config['table_name']}"
        connection = _create_connection(parsed_credentials)

        result = write_df_to_snowflake(
            df,
            connection,
            qualified_table_name,
            save_config["table_is_new"],
            save_config["write_mode"],
        )

        if result["error"]:
            raise RuntimeError(
                sanitize_sensitive_data(
                    f"Error writing to Snowflake: {result['error']}"
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
