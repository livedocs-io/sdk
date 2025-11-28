from __future__ import annotations

import json
import re
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import UUID

import polars as pl
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pydantic import BaseModel, Field, field_validator
from typing_extensions import Literal, Union

from livedocs.datasources.base import BaseDatasourceConnector
from livedocs.types import (
    DBSaveConfig,
    ElementDataSource,
    LivedocsResult,
    QueryResult,
    QueryResultMetadata,
    SchemaNode,
    SchemaNodeType,
)
from livedocs.utils.lib.internals import (
    livedocs_internal_sanitize_sensitive_data as sanitize_sensitive_data,
)


class SnowflakeConnectionUsernamePassword(BaseModel):
    """
    Pydantic model for Snowflake connection using username/password authentication.
    Matches the Zod schema validation from the client.

    Expected structure:
    {
        name: string,                    // Required, non-empty string
        host: string,                    // Required, non-empty string
        database: string,                // Required, non-empty string
        auth_type: "username_password",  // Literal "username_password"
        username: string,                // Required, starts with letter/underscore
        password: string,                // Required, non-empty string
        service_account_key?: string,     // Optional
        service_account_username?: string; // Optional
    }
    """

    name: str = Field(
        ...,
        min_length=1,
        description="Connector name (required, non-empty string)",
    )
    host: str = Field(
        ...,
        min_length=1,
        description="Host (required, non-empty string)",
    )
    database: str = Field(
        ...,
        min_length=1,
        description="Database (required, non-empty string)",
    )
    auth_type: Literal["username_password"] = Field(
        ..., description="Authentication type: 'username_password'"
    )
    username: str = Field(
        ...,
        min_length=1,
        description="Username (required, starts with letter/underscore, alphanumeric + underscores)",
    )
    password: str = Field(
        ...,
        min_length=1,
        description="Password (required, non-empty string)",
    )
    service_account_key: str | None = Field(
        None, description="Service account key (optional)"
    )
    service_account_username: str | None = Field(
        None, description="Service account username (optional)"
    )

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: str) -> str:
        """
        Validate username: must start with letter or underscore,
        and contain only letters, numbers, and underscores.
        Matches Zod validation from the client.
        """
        if not v:
            raise ValueError("Username is required")
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError(
                "Username must start with a letter or underscore and contain only letters, numbers, and underscores"
            )
        return v

    @field_validator("password")
    @classmethod
    def validate_password(cls, v: str) -> str:
        """
        Validate password: non-empty string.
        Matches Zod validation from the client.
        """
        if not v:
            raise ValueError("Password is required")
        return v


class SnowflakeConnectionServiceAccount(BaseModel):
    """
    Pydantic model for Snowflake connection using service account key authentication.
    Matches the Zod schema validation from the client.

    Expected structure:
    {
        name: string,                    // Required, non-empty string
        host: string,                    // Required, non-empty string
        database: string,                // Required, non-empty string
        auth_type: "service_account_key", // Literal "service_account_key"
        service_account_key: string,      // Required, valid JSON with private_key field
        service_account_username: string, // Required, starts with letter/underscore
        username?: string,                // Optional
        password?: string,                // Optional
    }
    """

    name: str = Field(
        ...,
        min_length=1,
        description="Connector name (required, non-empty string)",
    )
    host: str = Field(
        ...,
        min_length=1,
        description="Host (required, non-empty string)",
    )
    database: str = Field(
        ...,
        min_length=1,
        description="Database (required, non-empty string)",
    )
    auth_type: Literal["service_account_key"] = Field(
        ..., description="Authentication type: 'service_account_key'"
    )
    service_account_key: str = Field(
        ...,
        min_length=1,
        description="Service account key (required, valid JSON with private_key field)",
    )
    service_account_username: str = Field(
        ...,
        min_length=1,
        description="Service account username (required, starts with letter/underscore, alphanumeric + underscores)",
    )
    username: str | None = Field(None, description="Username (optional)")
    password: str | None = Field(None, description="Password (optional)")

    @field_validator("service_account_key")
    @classmethod
    def validate_service_account_key(cls, v: str) -> str:
        """
        Validate service account key: must be valid JSON with private_key field.
        Matches Zod validation from the client.
        """
        if not v:
            raise ValueError("Service account key is required")
        try:
            parsed = json.loads(v)
        except json.JSONDecodeError:
            raise ValueError(
                "Service account key must be valid JSON with a private_key field"
            )

        if not isinstance(parsed, dict):
            raise ValueError(
                "Service account key must be valid JSON with a private_key field"
            )

        if "private_key" not in parsed:
            raise ValueError(
                "Service account key must be valid JSON with a private_key field"
            )

        if not isinstance(parsed["private_key"], str):
            raise ValueError(
                "Service account key must be valid JSON with a private_key field"
            )

        return v

    @field_validator("service_account_username")
    @classmethod
    def validate_service_account_username(cls, v: str) -> str:
        """
        Validate service account username: must start with letter or underscore,
        and contain only letters, numbers, and underscores.
        Matches Zod validation from the client.
        """
        if not v:
            raise ValueError("Service account username is required")
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError(
                "Service account username must start with a letter or underscore and contain only letters, numbers, and underscores"
            )
        return v


# Discriminated union type for Snowflake connection details
SnowflakeConnectionDetails = Union[
    SnowflakeConnectionUsernamePassword, SnowflakeConnectionServiceAccount
]


class SnowflakeDatasourceConnector(BaseDatasourceConnector):
    """
    Datasource connector for Snowflake.
    """

    def read(
        self,
        query: str,
        datasource: ElementDataSource,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
    ) -> tuple[pl.DataFrame, tuple]:
        try:
            if datasource["database_info"] is None:
                raise ValueError("Missing required information: 'database_info'")
            db_connector_id = datasource["database_info"]["database_connector_id"]
            _, parsed_credentials = get_database_details(db_connector_id)
        except KeyError as e:
            raise ValueError(f"Missing required information: {e}")

        # Validate connection details using Pydantic
        try:
            connection_details = self._validate_connection_details(parsed_credentials)
        except Exception as e:
            raise ValueError(
                f"Invalid Snowflake connection details: {e}. "
                f"Expected: auth_type ('username_password' or 'service_account_key'), "
                f"name (non-empty string), host (non-empty string), database (non-empty string), "
                f"and either username+password (for 'username_password') or "
                f"service_account_key+service_account_username (for 'service_account_key')"
            )

        connection = None
        try:
            connection = self._create_connection(connection_details)
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
            raise RuntimeError(
                sanitize_sensitive_data(f"Error querying Snowflake: {e}")
            )
        finally:
            if connection is not None:
                connection.close()

    def write(
        self,
        df: pl.DataFrame,
        save_config: DBSaveConfig,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
    ) -> LivedocsResult:
        try:
            db_connector_id = save_config["database_id"]
            _, parsed_credentials = get_database_details(db_connector_id)
        except KeyError as e:
            raise ValueError(f"Missing required information: {e}")

        # Validate connection details using Pydantic
        try:
            connection_details = self._validate_connection_details(parsed_credentials)
        except Exception as e:
            raise ValueError(
                f"Invalid Snowflake connection details: {e}. "
                f"Expected: auth_type ('username_password' or 'service_account_key'), "
                f"name (non-empty string), host (non-empty string), database (non-empty string), "
                f"and either username+password (for 'username_password') or "
                f"service_account_key+service_account_username (for 'service_account_key')"
            )

        connection = None
        try:
            qualified_table_name = f"{connection_details.database}.{save_config['schema_name']}.{save_config['table_name']}"
            connection = self._create_connection(connection_details)

            result = self._write_df_to_snowflake(
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
        finally:
            if connection is not None:
                connection.close()

    def teardown(self) -> None:
        pass

    def _validate_connection_details(
        self, parsed_credentials: dict[str, Any]
    ) -> SnowflakeConnectionUsernamePassword | SnowflakeConnectionServiceAccount:
        """
        Validate connection details using Pydantic models.
        Returns the validated connection details model.
        """
        auth_type = parsed_credentials.get("auth_type")

        if auth_type == "username_password":
            return SnowflakeConnectionUsernamePassword(**parsed_credentials)
        elif auth_type == "service_account_key":
            return SnowflakeConnectionServiceAccount(**parsed_credentials)
        else:
            raise ValueError(
                f"Invalid auth_type value: {auth_type}. "
                f"Must be either 'username_password' or 'service_account_key'"
            )

    def _create_connection(
        self,
        connection_details: SnowflakeConnectionUsernamePassword
        | SnowflakeConnectionServiceAccount,
    ) -> Any:
        """
        Create Snowflake connection from validated Pydantic model.
        Handles both username/password and service account key authentication.
        """
        if isinstance(connection_details, SnowflakeConnectionUsernamePassword):
            # Username/password authentication
            return snowflake.connector.connect(
                user=connection_details.username,
                password=connection_details.password,
                account=connection_details.host,
                database=connection_details.database,
                session_parameters={
                    "QUERY_TAG": "LivedocsQuery",
                },
            )
        else:
            # Service account key authentication
            # Parse the service account key JSON to get the private_key
            service_account_key_json = json.loads(
                connection_details.service_account_key
            )
            pem_key_from_ui = service_account_key_json["private_key"].strip()

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
                connection_details.host.replace(".snowflakecomputing.com", "")
                .replace("https://", "")
                .replace("http://", "")
            )

            return snowflake.connector.connect(
                account=account,
                user=connection_details.service_account_username,
                private_key=private_key_der,
                database=connection_details.database,
            )

    def _write_df_to_snowflake(
        self,
        df: pl.DataFrame,
        connection: snowflake.connector.SnowflakeConnection,
        table_name: str,
        create_table: bool = False,
        write_mode: Literal["append", "overwrite"] = "append",
    ) -> dict[str, Any]:
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
            db_schema_table = table_name.split(".")
            try:
                # Split the fully qualified table name
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
                sf_schema = {
                    col[0]: {"type": col[1], "nullable": col[3] == "Y"}
                    for col in cursor.fetchall()
                }

            except Exception as e:
                if not create_table:
                    raise ValueError(
                        f"Table {table_name} does not exist and create_table is False"
                    )

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
                    sf_type = self._map_polars_to_snowflake_type(dtype)
                    columns.append(f'"{col_name}" {sf_type}')

                create_table_sql = (
                    f'CREATE TABLE "{current_table}" ({", ".join(columns)})'
                )
                cursor.execute(create_table_sql)
                connection.commit()

                # Get the schema of the newly created table
                cursor.execute(f'DESCRIBE TABLE "{current_table}"')
                sf_schema = {
                    col[0]: {"type": col[1], "nullable": col[3] == "Y"}
                    for col in cursor.fetchall()
                }

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
                sf_type: str = str(info["type"])
                nullable: bool = bool(info["nullable"])
                target_type = self._map_snowflake_to_polars_type(sf_type)

                if col_name in df.columns:
                    expr = pl.col(col_name)

                    # Handle type casting and datetime formatting
                    if "TIMESTAMP" in sf_type.upper():
                        expr = expr.cast(pl.Datetime).dt.strftime(
                            "%Y-%m-%d %H:%M:%S.%f"
                        )
                    elif "DATE" in sf_type.upper():
                        expr = expr.cast(pl.Date).dt.strftime("%Y-%m-%d")
                    else:
                        expr = expr.cast(target_type)

                    # Handle null values
                    if not nullable:
                        default_val = self._get_default_value(sf_type)
                        expr = expr.fill_null(default_val)

                    expressions.append(expr.alias(col_name))
                else:
                    # Add missing columns with default values
                    if nullable:
                        expressions.append(
                            pl.lit(None).cast(target_type).alias(col_name)
                        )
                    else:
                        default_val = self._get_default_value(sf_type)
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
                types_row = {
                    col_name: info["type"] for col_name, info in sf_schema.items()
                }
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

    def process_schema(self, schema: Any) -> dict[str, str]:
        """
        Processes Snowflake schema and returns a mapping of column names
        to their Livedocs types (NUMBER, DATE, STRING).

        :param schema: List of column descriptions from Snowflake cursor
        :return: Dictionary {column_name: livedocs_type}
        """
        processed_schema: dict[str, str] = {}

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

            # Get type code from column description (index 1)
            type_code = col[1] if len(col) > 1 else 2
            if type_code in [0, 1]:  # NUMBER or FLOAT
                livedocs_type = "NUMBER"
            elif type_code in [4, 5, 10]:  # DATE, TIMESTAMP, or TIME
                livedocs_type = "DATE"
            else:
                livedocs_type = "STRING"

            # Use column name from description (index 0) or col.name attribute
            col_name = (
                col[0]
                if isinstance(col, (tuple, list))
                else getattr(col, "name", str(col))
            )
            processed_schema[col_name] = livedocs_type

        return processed_schema

    def _map_snowflake_type(self, snowflake_type: str) -> str:
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
        elif snowflake_type in (
            "DATE",
            "DATETIME",
            "TIME",
            "TIMESTAMP",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_TZ",
        ):
            return "DATE"

        # Mapping to STRING (default case)
        else:
            return "STRING"

    def _map_polars_to_snowflake_type(self, pol_type: str | pl.DataType) -> str:
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

    def _map_snowflake_to_polars_type(self, sf_type: str) -> type[pl.DataType]:
        """Convert Snowflake type string to Polars dtype"""
        type_mapping: dict[str, type[pl.DataType]] = {
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

    def _get_default_value(self, sf_type: str) -> Any:
        """Get default value for a Snowflake type"""
        if sf_type.upper() in (
            "NUMBER",
            "DECIMAL",
            "NUMERIC",
            "INT",
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "BYTEINT",
        ):
            return 0
        elif sf_type.upper() in (
            "FLOAT",
            "FLOAT4",
            "FLOAT8",
            "DOUBLE",
            "DOUBLE PRECISION",
            "REAL",
        ):
            return 0.0
        elif sf_type.upper() in ("BOOLEAN",):
            return False
        elif sf_type.upper() in (
            "TIMESTAMP",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_TZ",
            "DATETIME",
        ):
            return datetime.now(timezone.utc)
        elif sf_type.upper() == "DATE":
            return datetime.now(timezone.utc).date()
        else:
            return ""

    def _get_livedocs_type(self, snowflake_type: str) -> str:
        """
        Map Snowflake type to LivedocsStandardType.
        Returns: "NUMBER", "DATE", "BOOLEAN", or "STRING"
        """
        type_upper = (snowflake_type or "").upper()

        numeric_types = {
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
        }

        date_types = {
            "DATE",
            "TIME",
            "TIMESTAMP",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_TZ",
            "DATETIME",
        }

        boolean_types = {"BOOLEAN"}

        if type_upper in numeric_types:
            return "NUMBER"
        elif type_upper in date_types:
            return "DATE"
        elif type_upper in boolean_types:
            return "BOOLEAN"
        else:
            return "STRING"

    def get_schema(
        self, connector_id: str, connection_details: dict[str, Any]
    ) -> list[SchemaNode]:
        """
        Fetch schema information from Snowflake database and return as list of schema nodes.

        Args:
            connector_id: The connector ID to use for schema nodes
            connection_details: Dictionary containing connection details

        Returns:
            List of SchemaNode objects
        """
        nodes: list[SchemaNode] = []
        now = datetime.now(timezone.utc)

        # Validate connection details using Pydantic
        try:
            validated_connection_details = self._validate_connection_details(
                connection_details
            )
        except Exception as e:
            raise ValueError(
                f"Invalid Snowflake connection details: {e}. "
                f"Expected: auth_type ('username_password' or 'service_account_key'), "
                f"name (non-empty string), host (non-empty string), database (non-empty string), "
                f"and either username+password (for 'username_password') or "
                f"service_account_key+service_account_username (for 'service_account_key')"
            )

        # Get database name (Snowflake is case-insensitive for unquoted by default, but stores in upper)
        database_name = validated_connection_details.database.upper()
        if not database_name:
            raise ValueError("Database name is required in connection details")

        # Create database node (level 0)
        db_node_id = uuid.uuid4()
        db_path = database_name
        nodes.append(
            SchemaNode(
                id=db_node_id,
                connector_id=UUID(connector_id),
                parent_id=None,
                path=db_path,
                type=SchemaNodeType.DATABASE,
                name=database_name,
                data_type=None,
                livedocs_type=None,
                description=None,
                level=0,
                metadata={},
                created_at=now,
                updated_at=now,
            )
        )

        # Build connection
        connection = None
        try:
            connection = self._create_connection(validated_connection_details)
            cursor = connection.cursor()

            # Query schema details
            schema_details_query = f"""
                SELECT
                  S.SCHEMA_NAME,
                  S.COMMENT AS SCHEMA_COMMENT,
                  T.TABLE_NAME,
                  T.TABLE_TYPE,
                  T.COMMENT AS TABLE_COMMENT,
                  C.COLUMN_NAME,
                  C.DATA_TYPE,
                  C.COMMENT AS COLUMN_COMMENT,
                  C.ORDINAL_POSITION
                FROM {database_name}.INFORMATION_SCHEMA.SCHEMATA S
                JOIN {database_name}.INFORMATION_SCHEMA.TABLES T
                  ON S.CATALOG_NAME = T.TABLE_CATALOG AND S.SCHEMA_NAME = T.TABLE_SCHEMA
                JOIN {database_name}.INFORMATION_SCHEMA.COLUMNS C
                  ON T.TABLE_CATALOG = C.TABLE_CATALOG AND T.TABLE_SCHEMA = C.TABLE_SCHEMA AND T.TABLE_NAME = C.TABLE_NAME
                WHERE S.SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
                  AND T.TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
                ORDER BY S.SCHEMA_NAME, T.TABLE_NAME, C.ORDINAL_POSITION;
            """

            cursor.execute(schema_details_query)
            result_rows = cursor.fetchall()

            schema_node_ids: dict[str, UUID] = {}  # "SCHEMA_NAME" -> nodeId
            table_node_ids: dict[str, UUID] = {}  # "SCHEMA_NAME.TABLE_NAME" -> nodeId

            for row in result_rows:
                schema_name = row[0]
                schema_comment = row[1]
                table_name = row[2]
                table_type = row[3]
                table_comment = row[4]
                column_name = row[5]
                data_type = row[6]
                column_comment = row[7]

                # Create or get schema node (level 1)
                schema_node_id = schema_node_ids.get(schema_name)
                schema_path = f"{db_path}/{schema_name}"
                if not schema_node_id:
                    schema_node_id = uuid.uuid4()
                    nodes.append(
                        SchemaNode(
                            id=schema_node_id,
                            connector_id=UUID(connector_id),
                            parent_id=db_node_id,
                            path=schema_path,
                            type=SchemaNodeType.SCHEMA,
                            name=schema_name,
                            data_type=None,
                            livedocs_type=None,
                            description=schema_comment if schema_comment else None,
                            level=1,
                            metadata={},
                            created_at=now,
                            updated_at=now,
                        )
                    )
                    schema_node_ids[schema_name] = schema_node_id

                # Create or get table/view node (level 2)
                table_key = f"{schema_name}.{table_name}"
                table_node_id = table_node_ids.get(table_key)
                table_path = f"{schema_path}/{table_name}"
                node_type = (
                    SchemaNodeType.VIEW
                    if table_type == "VIEW"
                    else SchemaNodeType.TABLE
                )

                if not table_node_id:
                    table_node_id = uuid.uuid4()
                    nodes.append(
                        SchemaNode(
                            id=table_node_id,
                            connector_id=UUID(connector_id),
                            parent_id=schema_node_id,
                            path=table_path,
                            type=node_type,
                            name=table_name,
                            data_type=None,
                            livedocs_type=None,
                            description=table_comment if table_comment else None,
                            level=2,
                            metadata={
                                "table_type": table_type,
                                "database_type": "snowflake",
                                "schema_name": schema_name,
                                "database_name": database_name,
                            },
                            created_at=now,
                            updated_at=now,
                        )
                    )
                    table_node_ids[table_key] = table_node_id

                # Create column node (level 3)
                column_node_id = uuid.uuid4()
                column_path = f"{table_path}/{column_name}"
                nodes.append(
                    SchemaNode(
                        id=column_node_id,
                        connector_id=UUID(connector_id),
                        parent_id=table_node_id,
                        path=column_path,
                        type=SchemaNodeType.COLUMN,
                        name=column_name,
                        data_type=data_type if data_type else None,
                        livedocs_type=(
                            self._get_livedocs_type(data_type) if data_type else None
                        ),
                        description=column_comment if column_comment else None,
                        level=3,
                        metadata={},
                        created_at=now,
                        updated_at=now,
                    )
                )

        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error fetching Snowflake schema: {e}")
            )
        finally:
            if connection is not None:
                connection.close()

        return nodes
