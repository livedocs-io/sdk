from datetime import datetime, timezone
from typing import Dict
from typing_extensions import Literal
import polars as pl
import uuid

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

def write_df_to_table(df: pl.DataFrame, duckdb_conn, table_name: str, create_table: bool = False, write_mode: Literal["append", "overwrite"] = "append"):
    """
    Aligns a Polars DataFrame to match a DuckDB table schema and writes the DataFrame to the table.
    If create_table is True, creates the table based on DataFrame schema before writing.
    
    Args:
        df: Polars DataFrame to align
        duckdb_conn: DuckDB connection
        table_name: Name of the target DuckDB table
        create_table: If True, create the table before writing
        
    Returns:
        Polars DataFrame with aligned schema
        
    Raises:
        ValueError: If required columns are missing
    """

    if write_mode not in ["append", "overwrite"]:
        raise ValueError('write_mode must be either "append" or "overwrite"')
    
    # Begin transaction
    duckdb_conn.sql("BEGIN TRANSACTION")

    try:
        if create_table:
            # Check if table exists
            check_table_query = f"""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_name = '{table_name.lower()}'
            );
            """
            table_exists = duckdb_conn.sql(check_table_query).fetchone()[0]
            
            if not table_exists:
                # CREATE TABLE based on DataFrame schema
                columns = []
                for col_name, dtype in zip(df.columns, df.dtypes):
                    print(col_name, dtype)
                    duck_type = polars_to_duckdb_type(dtype)
                    columns.append(f'"{col_name}" {duck_type}')
                
                create_stmt = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {','.join(columns)}
                )
                """
                duckdb_conn.sql(create_stmt)
            
            # Get schema for DF casting regardless of whether we created the table
            schema_query = f"DESCRIBE {table_name}"
            schema_df = duckdb_conn.sql(schema_query).pl()
        else:
            # If table already exists, get its schema
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
                
                if 'ENUM' in duck_type.upper():
                    # Get valid enum values
                    enum_values = get_enum_values(duck_type)
                    # Validate enum values before casting
                    if not nullable:
                        expr = (
                            expr.cast(pl.Utf8)
                            .map_elements(lambda x: x if x in enum_values else enum_values[0])
                        )
                    else:
                        expr = (
                            expr.cast(pl.Utf8)
                            .map_elements(lambda x: x if x in enum_values else None)
                        )
                elif 'UUID' in duck_type.upper():
                    expr = expr.cast(pl.Utf8)
                elif 'TIMESTAMP' in duck_type.upper():
                    expr = expr.cast(pl.Datetime).dt.cast_time_unit('ns')
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
        
        # Select and cast columns
        aligned_df = df.select(expressions)

        print("alignment done...")
        print(aligned_df)

        if write_mode == "overwrite":
            duckdb_conn.sql(f"TRUNCATE TABLE {table_name}")
        
        query = f"INSERT INTO {table_name} SELECT * FROM aligned_df"
        duckdb_conn.sql(query)

        # If we got here without errors, commit the transaction
        duckdb_conn.sql("COMMIT")
        
        return aligned_df
    except Exception as e:
        duckdb_conn.sql("ROLLBACK")
        raise e

def get_default_value(duck_type):
    """
    Get default value for a DuckDB type
    """
    if 'INT' in duck_type.upper():
        return 0
    elif 'DOUBLE' in duck_type.upper() or 'FLOAT' in duck_type.upper():
        return 0.0
    elif 'BOOL' in duck_type.upper():
        return False
    elif 'DATE' in duck_type.upper() or 'TIMESTAMP' in duck_type.upper():
        return datetime.now(timezone.utc)
    elif 'UUID' in duck_type.upper():
        return str(uuid.uuid4())
    else:
        return ""

def duckdb_to_polars_type(duck_type):
    """
    Convert DuckDB type string to Polars type
    """
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
        # Handle UUIDs and ENUMs as strings
        'UUID': pl.Utf8, 
        'ENUM': pl.Utf8,  
    }
    
    base_type = duck_type.split('(')[0].upper()
    if base_type in type_mapping:
        return type_mapping[base_type]
        
    return pl.Utf8

def polars_to_duckdb_type(pol_type: str) -> str:
    """Convert Polars dtype string to DuckDB type string"""
    type_str = str(pol_type).lower()
    
    if 'datetime' in type_str:
        return 'TIMESTAMP WITH TIME ZONE'
    elif 'date' in type_str:
        return 'DATE'
    elif 'int8' in type_str:
        return 'TINYINT'
    elif 'int16' in type_str:
        return 'SMALLINT'
    elif 'int32' in type_str:
        return 'INTEGER'
    elif 'int64' in type_str:
        return 'BIGINT'
    elif 'float32' in type_str:
        return 'REAL'
    elif 'float64' in type_str:
        return 'DOUBLE'
    elif 'bool' in type_str:
        return 'BOOLEAN'
    elif 'utf8' in type_str or 'string' in type_str:
        return 'VARCHAR'
    else:
        return 'VARCHAR'

def get_enum_values(duck_type):
    """Extract valid enum values from DuckDB enum type definition"""
    if 'ENUM' not in duck_type.upper():
        return None
    # Extract values from ENUM('val1', 'val2', ...) format
    enum_str = duck_type[duck_type.find("(")+1:duck_type.rfind(")")]
    return [val.strip("'") for val in enum_str.split(',')]