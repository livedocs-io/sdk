from typing import Dict, List
import polars as pl

from livedocs.types import Schema


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
    elif pg_type in ("char", "varchar", "text"):
        return "STRING"
    # Boolean type
    elif pg_type == "boolean":
        return "STRING"  # You might want to map this differently
    # Binary data types
    elif pg_type in ("bytea", "bit", "bit varying"):
        return "STRING"  # Typically stored as hex strings
    # Network address types
    elif pg_type in ("cidr", "inet", "macaddr"):
        return "STRING"
    # Geometric types
    elif pg_type in ("point", "line", "lseg", "box", "path", "polygon", "circle"):
        return "STRING"  # These are typically stored as text representations
    # JSON types
    elif pg_type in ("json", "jsonb"):
        return "STRING"
    # Arrays
    elif pg_type.endswith("[]"):
        return "STRING"  # Arrays are typically stored as text representations
    # UUID type
    elif pg_type == "uuid":
        return "STRING"
    # XML type
    elif pg_type == "xml":
        return "STRING"
    # Range types
    elif pg_type.startswith("range"):
        return "STRING"
    # Everything else is user-defined
    else:
        return "USER-DEFINED"


def process_postgres_schema(schema_data: pl.DataFrame) -> List[Schema]:
    required_columns = ["column_name", "udt_name"]
    if not all(col in schema_data.columns for col in required_columns):
        raise ValueError(
            f"DataFrame must contain columns: {', '.join(required_columns)}"
        )

    processed_schema = []
    for row in schema_data.iter_rows(named=True):
        column_name = row["column_name"]
        udt_name = row["udt_name"]

        mapped_type = map_postgres_type(udt_name)

        if mapped_type != "USER-DEFINED":
            schema_entry: Schema = {
                "name": column_name,
                "type": mapped_type,
                "children": [],
            }
            processed_schema.append(schema_entry)

    return processed_schema
