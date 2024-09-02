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


"""
    Mapped most (all?) types from DuckDB to Livedocs types

    https://duckdb.org/docs/sql/data_types/overview.html
"""
def map_duckdb_type(column_type: str) -> str:
    column_type = column_type.upper()

    # Mapping to NUMBER
    if column_type in (
        "BIGINT", "INT8", "LONG", "DECIMAL", "NUMERIC",
        "DOUBLE", "FLOAT8", "HUGEINT", "INTEGER", "INT4", 
        "INT", "SIGNED", "SMALLINT", "INT2", "SHORT", "TINYINT", 
        "INT1", "UBIGINT", "UHUGEINT", "UINTEGER", "USMALLINT", 
        "UTINYINT", "FLOAT", "FLOAT4", "REAL"
    ):
        return "NUMBER"
    
    # Mapping to DATE
    elif column_type in (
        "DATE", "TIME", "TIMESTAMP", "DATETIME", "TIMESTAMPTZ", 
        "INTERVAL", "TIMESTAMP WITH TIME ZONE"
    ):
        return "DATE"
    
    # Mapping to STRING
    elif column_type in (
        "BIT", "BITSTRING", "BLOB", "BYTEA", "BINARY", 
        "VARBINARY", "BOOLEAN", "BOOL", "LOGICAL", "UUID", 
        "VARCHAR", "CHAR", "BPCHAR", "TEXT", "STRING", "ARRAY", 
        "LIST", "MAP", "STRUCT", "UNION"
    ):
        return "STRING"
    
    # Default to STRING for any unknown types
    else:
        return "STRING"


def process_postgres_schema(schema_data: pl.DataFrame) -> dict:
    required_columns = ["column_name", "column_type"]
    if not all(col in schema_data.columns for col in required_columns):
        raise ValueError(
            f"DataFrame must contain columns: {', '.join(required_columns)}"
        )

    processed_schema = {}
    for row in schema_data.iter_rows(named=True):
        processed_schema[row["column_name"]] = map_duckdb_type(row["column_type"])
    return processed_schema
