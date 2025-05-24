import traceback
from datetime import datetime, timezone

import polars as pl
import snowflake.connector
from typing_extensions import Literal
import pandas as pd



def process_clickhouse_schema(schema: tuple) -> dict:
    """
    Processes Clickhouse schema and returns a mapping of column names
    to their Livedocs types (NUMBER, DATE, STRING).

    :param schema: Tuple of (column_name, column_type) pairs from Clickhouse
    :return: Dictionary {column_name: livedocs_type}
    """

    # Get schema information
    processed_schema = []

    for col_name, col_type in schema:
        type_name = type(col_type).__name__
        
        # Handle numeric types
        if any(t in type_name for t in [
            'Int', 'UInt', 'Float', 'Decimal', 'FixedString',  # Basic numeric types
            'Int8', 'Int16', 'Int32', 'Int64', 'Int128', 'Int256',  # Signed integers
            'UInt8', 'UInt16', 'UInt32', 'UInt64', 'UInt128', 'UInt256',  # Unsigned integers
            'Float32', 'Float64',  # Floating point
            'Decimal32', 'Decimal64', 'Decimal128', 'Decimal256',  # Decimal types
            'Money', 'Money64'  # Money types
        ]):
            col_type = "NUMBER"
            
        # Handle datetime types
        elif any(t in type_name for t in [
            'DateTime', 'DateTime32', 'DateTime64',  # DateTime types
            'Date', 'Date32',  # Date types
            'Time', 'Time32', 'Time64',  # Time types
            'Timestamp', 'Timestamp32', 'Timestamp64'  # Timestamp types
        ]):
            col_type = "DATE"
            
        # Handle string types
        elif any(t in type_name for t in [
            'String', 'FixedString',  # String types
            'Enum', 'Enum8', 'Enum16',  # Enum types
            'UUID', 'IPv4', 'IPv6',  # Special string types
            'LowCardinality',  # Low cardinality types
            'Nullable'  # Nullable types
        ]):
            col_type = "STRING"
            
        # Handle boolean types
        elif 'Bool' in type_name:
            col_type = "NUMBER"  # Map boolean to NUMBER as it's typically used for 0/1
            
        # Handle array types
        elif 'Array' in type_name:
            col_type = "STRING"  # Map arrays to STRING as they'll be serialized
            
        # Handle map types
        elif 'Map' in type_name:
            col_type = "STRING"  # Map types to STRING as they'll be serialized
            
        # Handle tuple types
        elif 'Tuple' in type_name:
            col_type = "STRING"  # Map tuples to STRING as they'll be serialized
            
        # Handle nested types
        elif 'Nested' in type_name:
            col_type = "STRING"  # Map nested types to STRING as they'll be serialized
            
        # Default to STRING for any other types
        else:
            col_type = "STRING"
            
        processed_schema.append({
            "name": col_name,
            "livedocs_type": col_type,
            "children": []
        })

    return processed_schema


def write_df_to_clickhouse(
    df: pl.DataFrame,
    client,
    table_name: str,
    create_table: bool = False,
    write_mode: Literal["append", "overwrite"] = "append",
) -> dict:
    """
    Write a Polars DataFrame to a ClickHouse table with schema alignment.

    Args:
        df: Polars DataFrame to write
        client: ClickHouse client connection
        table_name: Fully-qualified table name (database.table)
        create_table: If True, create the table if it doesn't exist
        write_mode: Either "append" or "overwrite"

    Returns:
        dict with keys:
            result: DataFrame with types as first row + first 19 rows of data
            error: Dict with error message and stack trace, or empty dict on success
            rows_written: Number of rows written (0 if failed)
            run_date: UTC timestamp of operation
    """
    output = {
        "result": pl.DataFrame(),
        "error": {},
        "rows_written": 0,
        "run_date": datetime.now(timezone.utc),
    }

    try:
        if write_mode not in ["append", "overwrite"]:
            raise ValueError('write_mode must be either "append" or "overwrite"')

        # Split the fully qualified table name
        db_table = table_name.split('.')
        if len(db_table) == 2:
            database, table = db_table
        else:
            table = table_name

        # Check if table exists and get schema
        try:
            # Get table schema
            schema = client.query(f'DESCRIBE TABLE {table}')
            ch_schema = {col[0]: {"type": col[1], "nullable": "Nullable" in col[1]} for col in schema}

        except Exception as e:
            if not create_table:
                raise ValueError(f"Table {table_name} does not exist and create_table is False")
            
            # Create new table
            if len(db_table) == 2:
                database, table = db_table
                client.query(f'USE {database}')
            else:
                table = table_name

            # Create schema from DataFrame
            columns = []
            for col_name, dtype in zip(df.columns, df.dtypes):
                ch_type = map_polars_to_clickhouse_type(dtype)
                columns.append(f'{col_name} {ch_type}')
            
            create_table_sql = f'CREATE TABLE {table} ({", ".join(columns)}) ENGINE = MergeTree() ORDER BY tuple()'
            client.query(create_table_sql)

            # Get the schema of the newly created table
            schema = client.query(f'DESCRIBE TABLE {table}')
            ch_schema = {col[0]: {"type": col[1], "nullable": "Nullable" in col[1]} for col in schema}

        # Check for missing non-nullable columns
        missing_required = [
            col
            for col, info in ch_schema.items()
            if not info["nullable"] and col not in df.columns
        ]

        if missing_required:
            raise ValueError(
                f"DataFrame is missing required (non-nullable) columns from the table: {missing_required}"
            )

        # Check if there are any matching columns between DataFrame and table
        matching_columns = [col for col in df.columns if col in ch_schema]
        if not matching_columns:
            output["rows_written"] = 0
            return output

        # Check if DataFrame is empty
        if len(df) == 0:
            output["rows_written"] = 0
            return output

        # Prepare expressions for schema alignment
        expressions = []
        for col_name, info in ch_schema.items():
            ch_type = info["type"]
            nullable = info["nullable"]
            target_type = map_clickhouse_to_polars_type(ch_type)

            if col_name in df.columns:
                expr = pl.col(col_name)

                # Handle type casting and datetime formatting
                if "DateTime" in ch_type:
                    expr = expr.cast(pl.Datetime).dt.strftime('%Y-%m-%d %H:%M:%S')
                elif "Date" in ch_type:
                    expr = expr.cast(pl.Date).dt.strftime('%Y-%m-%d')
                else:
                    expr = expr.cast(target_type)

                # Handle null values
                if not nullable:
                    default_val = get_default_value(ch_type)
                    expr = expr.fill_null(default_val)

                expressions.append(expr.alias(col_name))
            else:
                # Add missing columns with default values
                if nullable:
                    expressions.append(pl.lit(None).cast(target_type).alias(col_name))
                else:
                    default_val = get_default_value(ch_type)
                    expressions.append(
                        pl.lit(default_val).cast(target_type).alias(col_name)
                    )

        # Align DataFrame schema
        aligned_df = df.select(expressions)

        # Convert to pandas for ClickHouse upload
        pd_df = aligned_df.to_pandas()
        
        # Prepare the write operation
        if write_mode == "overwrite":
            client.query(f'TRUNCATE TABLE {table}')

        # Write data to ClickHouse
        client.insert_dataframe(f'INSERT INTO {table} VALUES', pd_df)

        # Prepare output
        try:
            # Convert aligned DataFrame to strings for consistent output
            result_df = aligned_df.head(19).select(
                [pl.col(col).cast(pl.Utf8) for col in aligned_df.columns]
            )

            # Create types row
            types_row = {col_name: info["type"] for col_name, info in ch_schema.items()}
            types_df = pl.DataFrame([types_row])

            # Concatenate types with data
            output["result"] = pl.concat([types_df, result_df])

            output["rows_written"] = len(df)

            return output

        except Exception as e:
            # If there's an error in preparing the output but after successful upload
            output["error"] = {
                "message": f"Data written successfully but error in preparing output: {str(e)}",
                "stacktrace": traceback.format_exc(),
            }
            output["rows_written"] = len(df)
            return output

    except Exception as e:
        output["error"] = {"message": str(e), "stacktrace": traceback.format_exc()}
        return output

def map_polars_to_clickhouse_type(pol_type: str) -> str:
    """Convert Polars dtype string to ClickHouse type string"""
    type_str = str(pol_type).lower()

    if "datetime" in type_str:
        return "DateTime"
    elif "date" in type_str:
        return "Date"
    elif "int8" in type_str or "int16" in type_str:
        return "Int16"
    elif "int32" in type_str:
        return "Int32"
    elif "int64" in type_str:
        return "Int64"
    elif "float32" in type_str:
        return "Float32"
    elif "float64" in type_str:
        return "Float64"
    elif "bool" in type_str:
        return "UInt8"
    elif "utf8" in type_str or "string" in type_str:
        return "String"
    else:
        return "String"

def map_clickhouse_to_polars_type(ch_type: str) -> pl.DataType:
    """Convert ClickHouse type string to Polars dtype"""
    type_mapping = {
        "Int8": pl.Int8,
        "Int16": pl.Int16,
        "Int32": pl.Int32,
        "Int64": pl.Int64,
        "UInt8": pl.UInt8,
        "UInt16": pl.UInt16,
        "UInt32": pl.UInt32,
        "UInt64": pl.UInt64,
        "Float32": pl.Float32,
        "Float64": pl.Float64,
        "String": pl.Utf8,
        "Date": pl.Date,
        "DateTime": pl.Datetime,
        "DateTime64": pl.Datetime,
    }

    # Handle Nullable types
    if ch_type.startswith("Nullable("):
        base_type = ch_type[9:-1]  # Remove "Nullable(" and ")"
        return type_mapping.get(base_type, pl.Utf8)

    return type_mapping.get(ch_type, pl.Utf8)

def get_default_value(ch_type: str):
    """Get default value for a ClickHouse type"""
    if "Int" in ch_type:
        return 0
    elif "Float" in ch_type:
        return 0.0
    elif "DateTime" in ch_type:
        return datetime.now(timezone.utc)
    elif "Date" in ch_type:
        return datetime.now(timezone.utc).date()
    else:
        return ""
