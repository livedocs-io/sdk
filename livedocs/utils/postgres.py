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

def write_df_to_table(df: pl.DataFrame, duckdb_conn, table_name: str):
    """
    Aligns a Polars DataFrame to match a DuckDB table schema, handling missing columns,
    null values, and date/timestamp conversions, and writes the DataFrame to the table.
    
    Args:
        df: Polars DataFrame to align
        duckdb_conn: DuckDB connection
        table_name: Name of the target DuckDB table
        
    Returns:
        Polars DataFrame with aligned schema
        
    Raises:
        ValueError: If required columns are missing
    """
    """
    Aligns a Polars DataFrame to match a DuckDB table schema, handling missing columns,
    null values, and date/timestamp conversions.
    """
    # Get DuckDB table schema
    schema_query = f"DESCRIBE {table_name}"
    schema_df = duckdb_conn.sql(schema_query).pl()
    
    # Create mapping of column name to data type and nullability
    duck_schema = {}
    for row in schema_df.iter_rows():
        col_name, col_type, nullable = row[0], row[1], str(row[2]) == "YES"
        duck_schema[col_name] = {"type": col_type, "nullable": nullable}

    # Check for missing required columns
    missing_required = [
        col for col, info in duck_schema.items() if not info["nullable"] and col not in df.columns
    ]

    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")
    

    expressions = []
    for col_name, info in duck_schema.items():
        duck_type = info["type"]
        nullable = info["nullable"]
        target_type = duckdb_to_polars_type(duck_type)
        
        if col_name in df.columns:
            expr = pl.col(col_name)
            
            # Special handling for timestamps
            if 'TIMESTAMP' in duck_type.upper():
                # Convert to nanoseconds by default since it's the highest precision
                expr = (
                    expr.cast(pl.Datetime)
                    .cast(pl.Datetime, time_unit='ns')
                )
            else:
                expr = expr.cast(target_type)
            
            # Handle null values for non-nullable columns
            if not nullable:
                default_val = get_default_value(duck_type)
                expr = expr.fill_null(default_val)
                
            expressions.append(expr.alias(col_name))
        else:
            # Add missing columns with default values
            if nullable:
                expressions.append(pl.lit(None).cast(target_type).alias(col_name))
            else:
                default_val = get_default_value(duck_type)
                expressions.append(pl.lit(default_val).cast(target_type).alias(col_name))
    
    print(expressions)
    # Select and cast columns
    aligned_df = df.select(expressions)

    print("alignment done...")
    print(aligned_df)
    query = f"INSERT INTO {table_name} SELECT * FROM aligned_df"

    duckdb_conn.sql(query)
    
    return aligned_df

def get_default_value(duck_type):
    if 'INT' in duck_type.upper():
        return 0
    elif 'DOUBLE' in duck_type.upper() or 'FLOAT' in duck_type.upper():
        return 0.0
    elif 'BOOL' in duck_type.upper():
        return False
    elif 'DATE' in duck_type.upper() or 'TIMESTAMP' in duck_type.upper():
        return pl.datetime(1970, 1, 1)
    else:
        return ""

def duckdb_to_polars_type(duck_type):
    type_mapping = {
        'INTEGER': pl.Int64,
        'BIGINT': pl.Int64,
        'SMALLINT': pl.Int32,
        'TINYINT': pl.Int8,
        'DOUBLE': pl.Float64,
        'REAL': pl.Float32,
        'BOOLEAN': pl.Boolean,
        'VARCHAR': pl.Utf8,
        'TEXT': pl.Utf8,
        'CHAR': pl.Utf8,
        'DATE': pl.Date,
        'TIMESTAMP': pl.Datetime,
    }
    
    base_type = duck_type.split('(')[0].upper()
    if base_type in type_mapping:
        return type_mapping[base_type]
        
    return pl.Utf8