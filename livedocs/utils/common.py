import os
from datetime import date, datetime, time
from typing import Any, Callable


import polars as pl
import requests
from tqdm.auto import tqdm
import base64
import decimal
import uuid
import json

from IPython.display import display

from livedocs.types import (
    DatabaseType,
    ElementDataSource,
    ElementDatasourceType,
)


def get_run_context() -> str:
    current_run_context = "edit_mode"
    match os.getenv("LIVEDOCS_RUN_CONTEXT"):
        case "logic":
            current_run_context = "edit_mode"
        case "scheduled":
            current_run_context = "scheduled_runs"
        case "webhook":
            current_run_context = "webhook_runs"
        case _:
            current_run_context = "unknown_run_context"
    return current_run_context


def middleman_debug(label: str, data=None):
    """Sends data to the middleman for pretty-printing in its logs."""
    try:
        content_str = json.dumps(data, indent=2, default=str, ensure_ascii=False)
        mime_type = "application/json"
    except Exception:
        content_str = str(data)
        mime_type = "text/plain"
    display(
        {mime_type: content_str},
        metadata={
            "middleman_debug": True,
            "middleman_debug_label": label,
        },
        raw=True,
    )


def serializer(obj):
    """
    Serializes an object to a JSON-compatible format.
    """

    if obj is None:
        return None
    elif isinstance(obj, bool):
        return obj
    elif isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return str(obj)
    elif isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode("utf-8")
    elif isinstance(obj, dict):
        return {k: serializer(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [serializer(item) for item in obj]
    elif isinstance(obj, (set, tuple, frozenset)):
        return [serializer(item) for item in obj]
    elif isinstance(obj, complex):
        return {"real": obj.real, "imag": obj.imag}
    else:
        return str(obj)


def get_xlsx_sheet_names(local_path: str) -> list[str]:
    """
    Get sheet names from an xlsx file.

    Args:
        local_path: Path to the local xlsx file

    Returns:
        List of sheet names, or empty list if file can't be read
    """
    try:
        from openpyxl import load_workbook

        wb = load_workbook(local_path, read_only=True, data_only=True)
        sheet_names = wb.sheetnames
        wb.close()
        return sheet_names
    except Exception:
        return []


def get_query_for_datasource(
    datasource: ElementDataSource,
    limit: int | None = 10,
    file_path: str | None = None,
    get_database_details: Callable[[str], tuple[object, dict[str, Any]]] | None = None,
) -> str | None:
    """
    Prepares the DuckDB query for each datasource

    Args:
        datasource: The datasource configuration
        limit: Optional limit for the query results
        file_path: Optional file path for file datasources. When provided, uses
                   direct file querying syntax (SELECT * FROM <filepath>).
                   Supported extensions: .csv, .tsv, .txt, .gz, .parquet, .json,
                   .duckdb, .ddb, .xls, .xlsx, .sqlite
        get_database_details: Optional callable to retrieve database credentials.
                             Required for Snowflake datasources to get database name.

    Returns:
        SQL query string or None
    """
    # Supported file extensions for direct querying in DuckDB
    SUPPORTED_FILE_EXTENSIONS = {
        ".csv",
        ".tsv",
        ".gz",
        ".parquet",
        ".json",
        ".duckdb",
        ".ddb",
        ".xls",
        ".xlsx",
        ".sqlite",
    }

    limit_clause = f" LIMIT {limit}" if limit is not None else ""

    match datasource["source_type"]:
        case ElementDatasourceType.dataframe.value:
            if datasource["dataframe_info"] is None:
                raise ValueError("Dataframe info is required")
            return f"SELECT * FROM {datasource['dataframe_info']['df_name']}{limit_clause};"
        case ElementDatasourceType.database_table.value:
            if datasource["database_info"] is None:
                raise ValueError("Database info is required")
            if datasource["database_table_info"] is None:
                raise ValueError("Database table info is required")

            database_type = DatabaseType(datasource["database_info"]["database_type"])

            # Handle Snowflake special case - needs database name from credentials
            if database_type == DatabaseType.Snowflake:
                if get_database_details is None:
                    raise ValueError(
                        "get_database_details is required for Snowflake datasources"
                    )
                try:
                    db_connector_id = datasource["database_info"][
                        "database_connector_id"
                    ]
                    _, parsed_credentials = get_database_details(db_connector_id)
                    database_name = parsed_credentials.get("database")
                    if database_name is None:
                        raise ValueError(
                            "Database name not found in Snowflake credentials"
                        )
                    return (
                        f'SELECT * FROM "{database_name}".'
                        f'"{datasource["database_table_info"]["schema_name"]}".'
                        f'"{datasource["database_table_info"]["table_name"]}"'
                        f"{limit_clause};"
                    )
                except KeyError as e:
                    raise ValueError(f"Missing required information: {e}")

            if database_type == DatabaseType.Bigquery:
                return f"SELECT * FROM {datasource['database_table_info']['schema_name']}.{datasource['database_table_info']['table_name']}{limit_clause};"
            elif database_type == DatabaseType.Clickhouse:
                return f'SELECT * FROM "{datasource["database_table_info"]["schema_name"]}"."{datasource["database_table_info"]["table_name"]}"{limit_clause};'
            elif datasource["database_info"]["database_type"] in {
                DatabaseType.Postgres.value,
                DatabaseType.Motherduck.value,
            }:
                return f'SELECT * FROM "{datasource["database_table_info"]["schema_name"]}"."{datasource["database_table_info"]["table_name"]}"{limit_clause};'
            elif database_type == DatabaseType.Databricks:
                return (
                    "SELECT * FROM "
                    f"{datasource['database_table_info']['catalog_name']}."
                    f"{datasource['database_table_info']['schema_name']}."
                    f"{datasource['database_table_info']['table_name']}"
                    f"{limit_clause};"
                )
            else:
                return (
                    "SELECT * FROM "
                    f'"{datasource["database_info"]["database_name"]}".'
                    f'"{datasource["database_table_info"]["schema_name"]}".'
                    f'"{datasource["database_table_info"]["table_name"]}"'
                    f"{limit_clause};"
                )

        case ElementDatasourceType.file.value:
            if datasource["file_info"] is None:
                raise ValueError("File info is required")

            # If file_path is provided, use direct file querying
            if file_path is not None:
                # Extract file extension
                file_extension = None
                if "." in file_path:
                    file_extension = "." + file_path.rsplit(".", 1)[1].lower()

                # Validate file extension
                if (
                    file_extension is None
                    or file_extension not in SUPPORTED_FILE_EXTENSIONS
                ):
                    supported_exts = ", ".join(sorted(SUPPORTED_FILE_EXTENSIONS))
                    raise ValueError(
                        f"Unsupported file extension for direct querying: {file_extension or 'no extension'}. "
                        f"Supported extensions: {supported_exts}"
                    )

                # Escape single quotes in file path to prevent SQL injection
                escaped_file_path = file_path.replace("'", "''")

                # Handle xlsx with layer_name (sheet selection)
                # Use ignore_errors=true to handle cells that can't be cast (replaces with NULL)
                if file_extension == ".xlsx":
                    layer = datasource["file_info"].get("layer_name")
                    if layer:
                        return f"SELECT * FROM read_xlsx('{escaped_file_path}', sheet='{layer}', ignore_errors=true){limit_clause};"
                    return f"SELECT * FROM read_xlsx('{escaped_file_path}', ignore_errors=true){limit_clause};"

                return f"SELECT * FROM '{escaped_file_path}'{limit_clause};"

            # Fallback to old method if file_path is not provided
            file_name = datasource["file_info"]["file_name"]

            # Extract and validate file extension
            file_extension = None
            if "." in file_name:
                file_extension = "." + file_name.rsplit(".", 1)[1].lower()

            # Validate file extension
            if (
                file_extension is None
                or file_extension not in SUPPORTED_FILE_EXTENSIONS
            ):
                supported_exts = ", ".join(sorted(SUPPORTED_FILE_EXTENSIONS))
                raise ValueError(
                    f"Unsupported file extension for querying: {file_extension or 'no extension'}. "
                    f"Supported extensions: {supported_exts}"
                )

            # Use appropriate query method based on file type
            if datasource["file_info"]["file_type"] == "csv":
                return f"SELECT * FROM read_csv_auto('{file_name}'){limit_clause};"
            elif datasource["file_info"]["file_type"] == "xlsx":
                # Use ignore_errors=true to handle cells that can't be cast (replaces with NULL)
                layer = datasource["file_info"].get("layer_name")
                if layer:
                    return f"SELECT * FROM read_xlsx('{file_name}', sheet='{layer}', ignore_errors=true){limit_clause};"
                return f"SELECT * FROM read_xlsx('{file_name}', ignore_errors=true){limit_clause};"
            else:
                # For other supported file types, use direct querying
                escaped_file_name = file_name.replace("'", "''")
                return f"SELECT * FROM '{escaped_file_name}'{limit_clause};"
        case ElementDatasourceType.dataframe.value:
            if datasource["dataframe_info"] is None:
                raise ValueError("Dataframe info is required")
            return f"SELECT * FROM {datasource['dataframe_info']['df_name']}{limit_clause};"
        case _:
            raise ValueError(
                f"Unsupported datasource type: {datasource['source_type']}"
            )


def _get_dataframe_schema(df: pl.DataFrame) -> dict[str, str]:
    date_formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%B %d, %Y",
        "%d %b %Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y/%m/%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
    ]

    def is_date(value: str) -> bool:
        for fmt in date_formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
        return False

    def is_number(value: str) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False

    def map_column_type(series: pl.Series) -> str:
        # Drop nulls
        non_empty_series = series.drop_nulls()

        # Only filter out empty strings if the series is of string type
        if series.dtype == pl.Utf8:
            non_empty_series = non_empty_series.filter(non_empty_series != "")

        sample_values = non_empty_series.to_list()

        if not sample_values:
            return "STRING"

        # Check if all non-empty sample values can be numbers
        if all(
            isinstance(val, (int, float)) or (isinstance(val, str) and is_number(val))
            for val in sample_values
        ):
            return "NUMBER"

        # Check if all non-empty sample values can be dates
        if all(
            isinstance(val, datetime) or (isinstance(val, str) and is_date(val))
            for val in sample_values
        ):
            return "DATE"

        # Check the series' inherent type if it's not a string series
        if series.dtype in {pl.Float32, pl.Float64, pl.Int32, pl.Int64}:
            return "NUMBER"
        elif series.dtype in {pl.Date, pl.Datetime}:
            return "DATE"
        else:
            return "STRING"

    column_types = {col: map_column_type(df[col]) for col in df.columns}

    return column_types


def _setup_dirs():
    """
    Sets up the user files directory if it doesn't exist.
    """
    user_files_dir = os.getenv("LIVEDOCS_FILES_PATH")
    if not user_files_dir:
        raise ValueError("LIVEDOCS_FILES_PATH environment variable not set")

    os.makedirs(user_files_dir, exist_ok=True)


def _get_chunk_size(file_size_bytes: int | None) -> int:
    """
    Determines a dynamic chunk size based on the file size.
    """
    MB = 1024 * 1024
    if file_size_bytes is None:
        return 128 * 1024  # Default to 128KB if size is unknown

    if file_size_bytes < 1 * MB:  # Files < 1MB
        return 64 * 1024  # 64KB chunks
    elif file_size_bytes < 10 * MB:  # Files < 10MB
        return 128 * 1024  # 128KB chunks
    elif file_size_bytes < 100 * MB:  # Files < 100MB
        return 256 * 1024  # 256KB chunks
    elif file_size_bytes < 500 * MB:  # Files < 500MB
        return 512 * 1024  # 512KB chunks
    else:  # Files >= 500MB
        return 1 * MB  # 1MB chunks


def _download_file(
    signed_url: str,
    local_path: str,
    file_description: str,
    expected_size_bytes: int | None,
):
    """
    Downloads a file from a signed URL to a local path.
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    try:
        response = requests.get(signed_url, stream=True)
        response.raise_for_status()

        total_size = expected_size_bytes
        chunk_size = _get_chunk_size(total_size)

        with (
            open(local_path, "wb") as f,
            tqdm(
                desc=f"Downloading {file_description}",
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                disable=total_size is None or total_size == 0,
                leave=True,
            ) as bar,
        ):
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

    except requests.exceptions.HTTPError as e:
        if os.path.exists(local_path):
            os.remove(local_path)
        error_detail = f"HTTP status {e.response.status_code}."
        error_detail += f" Response: {e.response.text}"
        raise RuntimeError(
            f"Failed to download {file_description}. {error_detail}"
        ) from e
    except requests.exceptions.RequestException as e:
        if os.path.exists(local_path):
            os.remove(local_path)
        raise RuntimeError(
            f"Network error while downloading {file_description}: {e}"
        ) from e
    except Exception as e:
        if os.path.exists(local_path):
            os.remove(local_path)
        raise RuntimeError(
            f"An error occurred during download of {file_description}: {e}"
        ) from e
