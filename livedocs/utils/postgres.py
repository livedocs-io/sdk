from typing import Dict
import polars as pl


def create_postgres_connection_url(details: Dict[str, str]) -> str:
    user = details["user_name"]
    password = details.get("password", "")
    host = details["host"]
    port = details.get("port", 5432)
    database = details["database"]

    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    else:
        return f"postgresql://{user}@{host}:{port}/{database}"


def map_postgres_type(pg_type: str) -> str:
    # Numeric types
    if pg_type in (
        "smallint",
        "integer",
        "bigint",
        "decimal",
        "numeric",
        "real",
        "double precision",
        "serial",
        "bigserial",
    ):
        return "NUMBER"
    # Date/Time types
    elif pg_type in ("date", "time", "timestamp", "timestamptz", "interval"):
        return "DATE"
    # String and character types
    else:
        return "STRING"


def process_postgres_schema(schema_data: pl.DataFrame) -> dict:
    required_columns = ["column_name", "udt_name"]
    if not all(col in schema_data.columns for col in required_columns):
        raise ValueError(
            f"DataFrame must contain columns: {', '.join(required_columns)}"
        )

    processed_schema = {}
    for row in schema_data.iter_rows(named=True):
        processed_schema[row["column_name"]] = map_postgres_type(row["udt_name"])
    return processed_schema
