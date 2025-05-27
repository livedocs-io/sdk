import traceback
from datetime import datetime, timezone

import polars as pl
import snowflake.connector
from typing_extensions import Literal
import pandas as pd

def map_snowflake_type(snowflake_type: str) -> str:
    """
    Maps Snowflake types to Livedocs types: NUMBER, DATE, STRING.
    """
    snowflake_type = snowflake_type.upper()

    # Mapping to NUMBER
    if snowflake_type in (
        "NUMBER",
        "DECIMAL",
        "NUMERIC",
        "INT",
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "TINYINT",
        "BYTEINT",
        "FLOAT",
        "FLOAT4",
        "FLOAT8",
        "DOUBLE",
        "DOUBLE PRECISION",
        "REAL",
    ):
        return "NUMBER"

    # Mapping to DATE
    elif snowflake_type in ("DATE", "DATETIME", "TIME", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ"):
        return "DATE"

    # Mapping to STRING (default case)
    else:
        return "STRING"


def process_snowflake_schema(schema) -> dict:
    """
    Processes Snowflake schema and returns a mapping of column names
    to their Livedocs types (NUMBER, DATE, STRING).

    :param schema: List of column descriptions from Snowflake cursor
    :return: Dictionary {column_name: livedocs_type}
    """
    processed_schema = {}

    for col in schema:
        # Snowflake type codes:
        # 0 = NUMBER
        # 1 = FLOAT
        # 2 = STRING
        # 3 = BOOLEAN
        # 4 = DATE
        # 5 = TIMESTAMP
        # 6 = VARIANT
        # 7 = OBJECT
        # 8 = ARRAY
        # 9 = BINARY
        # 10 = TIME
        # 11 = GEOGRAPHY
        # 12 = GEOMETRY
        # 13 = INTERVAL
        
        type_code = [1]
        if type_code in [0, 1]:  # NUMBER or FLOAT
            livedocs_type = "NUMBER"
        elif type_code in [4, 5, 10]:  # DATE, TIMESTAMP, or TIME
            livedocs_type = "DATE"
        else:
            livedocs_type = "STRING"
        
        processed_schema[col.name] = livedocs_type

    return processed_schema


def map_polars_to_snowflake_type(pol_type: str) -> str:
    """Convert Polars dtype string to Snowflake type string"""
    type_str = str(pol_type).lower()

    if "datetime" in type_str:
        return "TIMESTAMP"
    elif "date" in type_str:
        return "DATE"
    elif "int8" in type_str or "int16" in type_str:
        return "NUMBER"
    elif "int32" in type_str or "int64" in type_str:
        return "NUMBER"
    elif "float32" in type_str or "float64" in type_str:
        return "FLOAT"
    elif "bool" in type_str:
        return "BOOLEAN"
    elif "utf8" in type_str or "string" in type_str:
        return "VARCHAR"
    else:
        return "VARCHAR"


def map_snowflake_to_polars_type(sf_type: str) -> pl.DataType:
    """Convert Snowflake type string to Polars dtype"""
    type_mapping = {
        "NUMBER": pl.Int64,
        "DECIMAL": pl.Float64,
        "NUMERIC": pl.Float64,
        "INT": pl.Int64,
        "INTEGER": pl.Int64,
        "BIGINT": pl.Int64,
        "SMALLINT": pl.Int64,
        "TINYINT": pl.Int64,
        "BYTEINT": pl.Int64,
        "FLOAT": pl.Float64,
        "FLOAT4": pl.Float64,
        "FLOAT8": pl.Float64,
        "DOUBLE": pl.Float64,
        "DOUBLE PRECISION": pl.Float64,
        "REAL": pl.Float64,
        "BOOLEAN": pl.Boolean,
        "VARCHAR": pl.Utf8,
        "CHAR": pl.Utf8,
        "STRING": pl.Utf8,
        "TEXT": pl.Utf8,
        "DATE": pl.Date,
        "DATETIME": pl.Datetime,
        "TIMESTAMP": pl.Datetime,
        "TIMESTAMP_LTZ": pl.Datetime,
        "TIMESTAMP_NTZ": pl.Datetime,
        "TIMESTAMP_TZ": pl.Datetime,
        "TIME": pl.Time,
    }

    return type_mapping.get(sf_type.upper(), pl.Utf8)


def get_default_value(sf_type: str):
    """Get default value for a Snowflake type"""
    if sf_type.upper() in ("NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"):
        return 0
    elif sf_type.upper() in ("FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"):
        return 0.0
    elif sf_type.upper() in ("BOOLEAN"):
        return False
    elif sf_type.upper() in ("TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ", "DATETIME"):
        return datetime.now(timezone.utc)
    elif sf_type.upper() == "DATE":
        return datetime.now(timezone.utc).date()
    else:
        return ""


def write_df_to_snowflake(
    df: pl.DataFrame,
    connection: snowflake.connector.SnowflakeConnection,
    table_name: str,
    create_table: bool = False,
    write_mode: Literal["append", "overwrite"] = "append",
) -> dict:
    """
    Write a Polars DataFrame to a Snowflake table with schema alignment.

    Args:
        df: Polars DataFrame to write
        connection: Snowflake connection
        table_name: Fully-qualified table name (database.schema.table)
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

        cursor = connection.cursor()
        
        # Check if table exists and get schema
        try:
            # Split the fully qualified table name
            db_schema_table = table_name.split('.')
            if len(db_schema_table) == 3:
                database, schema, table = db_schema_table
                # Use the database and schema
                cursor.execute(f'USE DATABASE "{database}"')
                cursor.execute(f'USE SCHEMA "{schema}"')
                current_table = table
            else:
                current_table = table_name

            # Get table schema
            cursor.execute(f'DESCRIBE TABLE "{current_table}"')
            sf_schema = {col[0]: {"type": col[1], "nullable": col[3] == "Y"} for col in cursor.fetchall()}

        except Exception as e:
            if not create_table:
                raise ValueError(f"Table {table_name} does not exist and create_table is False")
            
            # Create new table
            if len(db_schema_table) == 3:
                database, schema, table = db_schema_table
                # Use the database and schema
                cursor.execute(f'USE DATABASE "{database}"')
                cursor.execute(f'USE SCHEMA "{schema}"')
                current_table = table
            else:
                current_table = table_name

            # Create schema from DataFrame
            columns = []
            for col_name, dtype in zip(df.columns, df.dtypes):
                sf_type = map_polars_to_snowflake_type(dtype)
                columns.append(f'"{col_name}" {sf_type}')
            
            create_table_sql = f'CREATE TABLE "{current_table}" ({", ".join(columns)})'
            cursor.execute(create_table_sql)
            connection.commit()

            # Get the schema of the newly created table
            cursor.execute(f'DESCRIBE TABLE "{current_table}"')
            sf_schema = {col[0]: {"type": col[1], "nullable": col[3] == "Y"} for col in cursor.fetchall()}

        # Check for missing non-nullable columns
        missing_required = [
            col
            for col, info in sf_schema.items()
            if not info["nullable"] and col not in df.columns
        ]

        if missing_required:
            raise ValueError(
                f"DataFrame is missing required (non-nullable) columns from the table: {missing_required}"
            )

        # Check if there are any matching columns between DataFrame and table
        matching_columns = [col for col in df.columns if col in sf_schema]
        if not matching_columns:
            output["rows_written"] = 0
            return output

        # Check if DataFrame is empty
        if len(df) == 0:
            output["rows_written"] = 0
            return output

        # Prepare expressions for schema alignment
        expressions = []
        for col_name, info in sf_schema.items():
            sf_type = info["type"]
            nullable = info["nullable"]
            target_type = map_snowflake_to_polars_type(sf_type)

            if col_name in df.columns:
                expr = pl.col(col_name)

                # Handle type casting and datetime formatting
                if "TIMESTAMP" in sf_type.upper():
                    expr = expr.cast(pl.Datetime).dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                elif "DATE" in sf_type.upper():
                    expr = expr.cast(pl.Date).dt.strftime('%Y-%m-%d')
                else:
                    expr = expr.cast(target_type)

                # Handle null values
                if not nullable:
                    default_val = get_default_value(sf_type)
                    expr = expr.fill_null(default_val)
                # For nullable columns, we want to keep nulls as nulls
                # No need to explicitly fill with None as that's the default behavior

                expressions.append(expr.alias(col_name))
            else:
                # Add missing columns with default values
                if nullable:
                    expressions.append(pl.lit(None).cast(target_type).alias(col_name))
                else:
                    default_val = get_default_value(sf_type)
                    expressions.append(
                        pl.lit(default_val).cast(target_type).alias(col_name)
                    )

        # Align DataFrame schema
        aligned_df = df.select(expressions)

        # Convert to pandas for Snowflake upload
        pd_df = aligned_df.to_pandas()
        
        # Prepare the write operation
        if write_mode == "overwrite":
            cursor.execute(f'TRUNCATE TABLE "{current_table}"')

        # Write data to Snowflake
        quoted_columns = [f'"{col}"' for col in aligned_df.columns]
        placeholders = ["%s"] * len(aligned_df.columns)
        insert_sql = f'INSERT INTO "{current_table}" ({", ".join(quoted_columns)}) VALUES ({", ".join(placeholders)})'
        cursor.executemany(insert_sql, pd_df.values.tolist())
        connection.commit()

        # Prepare output
        try:
            # Convert aligned DataFrame to strings for consistent output
            result_df = aligned_df.head(19).select(
                [pl.col(col).cast(pl.Utf8) for col in aligned_df.columns]
            )

            # Create types row
            types_row = {col_name: info["type"] for col_name, info in sf_schema.items()}
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
