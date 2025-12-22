from __future__ import annotations

import re
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import UUID

import clickhouse_connect
import polars as pl
from pydantic import BaseModel, Field, field_validator
from typing_extensions import Literal

from livedocs.datasources.base import BaseDatasourceConnector
from livedocs.types import (
    CacheInfo,
    CacheStatus,
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


class ClickHouseConnectionDetails(BaseModel):
    """
    Pydantic model for ClickHouse connection details.
    Matches the Zod schema validation from the client.

    Expected structure (from form submission):
    {
        name: string,        // Required, non-empty string
        host: string,       // Required, valid hostname or IP address (IPv4/IPv6)
        port: number,       // Required, integer between 1 and 65535
        user_name: string,  // Required, starts with letter/underscore, alphanumeric + underscores
        password: string,   // Required, non-empty string
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
        description="Host (required, valid hostname or IP address)",
    )
    port: int = Field(
        ...,
        ge=1,
        le=65535,
        description="Port (required, integer between 1 and 65535)",
    )
    user_name: str = Field(
        ...,
        min_length=1,
        description="Username (required, starts with letter/underscore, alphanumeric + underscores)",
    )
    password: str = Field(
        ...,
        min_length=1,
        description="Password (required, non-empty string)",
    )

    @field_validator("host")
    @classmethod
    def validate_host(cls, v: str) -> str:
        """
        Validate host format: valid hostname or IP address (IPv4/IPv6), or "localhost".
        Matches Zod validation from the client.
        """
        if not v:
            raise ValueError("Host is required")

        # Allow localhost
        if v == "localhost":
            return v

        # Valid hostname pattern: alphanumeric, dots, hyphens, underscores
        # Must start and end with alphanumeric
        hostname_pattern = r"^[a-zA-Z0-9]([a-zA-Z0-9\-_.]{0,61}[a-zA-Z0-9])?$"

        # IPv4 pattern
        ipv4_pattern = r"^(\d{1,3}\.){3}\d{1,3}$"

        # IPv6 pattern - matches IPv6 addresses including compressed notation (::)
        # This is a simplified pattern that covers most common IPv6 formats
        ipv6_pattern = (
            r"^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|"
            r"([0-9a-fA-F]{1,4}:){1,7}:|"
            r"([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|"
            r"([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|"
            r"([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|"
            r"([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|"
            r"([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|"
            r"[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|"
            r":((:[0-9a-fA-F]{1,4}){1,7}|:)|"
            r"fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]+|"
            r"::(ffff(:0{1,4}){0,1}:){0,1}((\d{1,3}\.){3}\d{1,3})|"
            r"([0-9a-fA-F]{1,4}:){1,4}:((\d{1,3}\.){3}\d{1,3}))$"
        )

        if (
            re.match(hostname_pattern, v)
            or re.match(ipv4_pattern, v)
            or re.match(ipv6_pattern, v, re.IGNORECASE)
        ):
            return v

        raise ValueError("Host must be a valid hostname or IP address")

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """
        Validate port: integer between 1 and 65535.
        Matches Zod validation from the client.
        Note: Type validation (int) and range (1-65535) are handled by Field constraints above.
        """
        # Additional validation for clearer error messages matching Zod
        if v < 1 or v > 65535:
            raise ValueError("Port must be between 1 and 65535")
        return v

    @field_validator("user_name")
    @classmethod
    def validate_user_name(cls, v: str) -> str:
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


class ClickHouseDatasourceConnector(BaseDatasourceConnector):
    """
    Datasource connector for ClickHouse.
    """

    def read(
        self,
        query: str,
        datasource: ElementDataSource,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
    ) -> tuple[pl.DataFrame, tuple[tuple[str, Any], ...]]:
        try:
            if datasource["database_info"] is None:
                raise ValueError("Missing required information: 'database_info'")
            db_connector_id = datasource["database_info"]["database_connector_id"]
            _, parsed_credentials = get_database_details(db_connector_id)
        except KeyError as e:
            raise ValueError(f"Missing required information: {e}")

        # Validate connection details using Pydantic
        try:
            connection_details = ClickHouseConnectionDetails(**parsed_credentials)
        except Exception as e:
            raise ValueError(
                f"Invalid ClickHouse connection details: {e}. "
                f"Expected: name (non-empty string), host (valid hostname/IP), "
                f"port (1-65535), user_name (starts with letter/underscore), "
                f"password (non-empty string)"
            )

        try:
            client = clickhouse_connect.get_client(
                host=connection_details.host,
                port=connection_details.port,
                user=connection_details.user_name,
                password=connection_details.password,
                secure=True,
            )
            result = client.query(query)
            if result.result_set:
                columns = result.column_names
                df = pl.DataFrame(result.result_set, schema=columns, orient="row")
            else:
                df = pl.DataFrame()

            return df, tuple(zip(result.column_names, result.column_types))

        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error querying Clickhouse: {e}")
            )

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
            connection_details = ClickHouseConnectionDetails(**parsed_credentials)
        except Exception as e:
            raise ValueError(
                f"Invalid ClickHouse connection details: {e}. "
                f"Expected: name (non-empty string), host (valid hostname/IP), "
                f"port (1-65535), user_name (starts with letter/underscore), "
                f"password (non-empty string)"
            )

        try:
            qualified_table_name = (
                f"{save_config['schema_name']}.{save_config['table_name']}"
            )
            client = clickhouse_connect.get_client(
                host=connection_details.host,
                port=connection_details.port,
                user=connection_details.user_name,
                password=connection_details.password,
                secure=True,
            )

            result = self._write_df_to_clickhouse(
                df,
                client,
                qualified_table_name,
                save_config["table_is_new"],
                save_config["write_mode"],
            )

            if result["error"]:
                raise RuntimeError(
                    sanitize_sensitive_data(
                        f"Error writing to Clickhouse: {result['error']}"
                    )
                )
            output = QueryResult(
                data=result["result"],
                metadata=QueryResultMetadata(
                    limit=50,
                    offset=0,
                    total_rows=result["rows_written"],
                    cache_info=CacheInfo(id="", status=CacheStatus.MISS),
                ),
            )
            payload = LivedocsResult(output)
            return payload
        except Exception as e:
            raise RuntimeError(sanitize_sensitive_data(f"DBSave Error: {e}"))

    def teardown(self) -> None:
        pass

    def _write_df_to_clickhouse(
        self,
        df: pl.DataFrame,
        client: Any,
        table_name: str,
        create_table: bool = False,
        write_mode: Literal["append", "overwrite"] = "append",
    ) -> dict[str, Any]:
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
            db_table = table_name.split(".")
            database, table = (
                (db_table[0], db_table[1]) if len(db_table) == 2 else (None, table_name)
            )

            # Check if table exists and get schema
            try:
                schema = client.query(f"DESCRIBE TABLE {table}")
                ch_schema = {
                    col[0]: {"type": col[1], "nullable": "Nullable" in col[1]}
                    for col in schema.result_set
                }
            except Exception as e:
                if not create_table:
                    raise ValueError(
                        f"Table {table_name} does not exist and create_table is False"
                    )

                # Create new table
                if database:
                    client.query(f"USE {database}")

                # Create schema from DataFrame
                columns = [
                    f"{col_name} {self._map_polars_to_clickhouse_type(dtype)}"
                    for col_name, dtype in zip(df.columns, df.dtypes)
                ]

                # Create table with MergeTree engine
                order_by_columns = ", ".join(df.columns)
                create_table_sql = f"""CREATE TABLE {table} (
                    {", ".join(columns)}
                ) ENGINE = MergeTree()
                ORDER BY ({order_by_columns})"""
                client.query(create_table_sql)

                # Get the schema of the newly created table
                schema = client.query(f"DESCRIBE TABLE {table}")
                ch_schema = {
                    col[0]: {"type": col[1], "nullable": "Nullable" in col[1]}
                    for col in schema.result_set
                }

            # Validate schema and data
            missing_required = [
                col
                for col, info in ch_schema.items()
                if not info["nullable"] and col not in df.columns
            ]
            if missing_required:
                raise ValueError(
                    f"DataFrame is missing required (non-nullable) columns from the table: {missing_required}"
                )

            matching_columns = [col for col in df.columns if col in ch_schema]
            if not matching_columns or len(df) == 0:
                output["rows_written"] = 0
                return output

            # Prepare expressions for schema alignment
            expressions = []
            for col_name, info in ch_schema.items():
                ch_type: str = str(info["type"])
                nullable: bool = bool(info["nullable"])
                target_type = self._map_clickhouse_to_polars_type(ch_type)

                if col_name in df.columns:
                    expr = pl.col(col_name)
                    if "DateTime" in ch_type:
                        expr = expr.cast(pl.Datetime).dt.replace_time_zone("UTC")
                    elif "Date" in ch_type:
                        expr = expr.cast(pl.Date).dt.replace_time_zone("UTC")
                    else:
                        expr = expr.cast(target_type)

                    if not nullable:
                        default_val = self._get_default_value(ch_type)
                        expr = expr.fill_null(default_val)

                    expressions.append(expr.alias(col_name))
                else:
                    if nullable:
                        expressions.append(
                            pl.lit(None).cast(target_type).alias(col_name)
                        )
                    else:
                        default_val = self._get_default_value(ch_type)
                        expressions.append(
                            pl.lit(default_val).cast(target_type).alias(col_name)
                        )

            # Align DataFrame schema and convert to pandas
            aligned_df = df.select(expressions)
            pd_df = aligned_df.to_pandas()

            # Handle overwrite mode
            if write_mode == "overwrite":
                client.query(f"TRUNCATE TABLE {table}")

            # Insert data with deduplication token
            dedup_token = str(int(datetime.now(timezone.utc).timestamp() * 1000))
            client.insert(
                table, pd_df, settings={"insert_deduplication_token": dedup_token}
            )

            # Prepare output
            try:
                result_df = aligned_df.head(19).select(
                    [pl.col(col).cast(pl.Utf8) for col in aligned_df.columns]
                )
                types_row = {
                    col_name: info["type"] for col_name, info in ch_schema.items()
                }
                types_df = pl.DataFrame([types_row])
                output["result"] = pl.concat([types_df, result_df])
                output["rows_written"] = len(df)
                return output

            except Exception as e:
                output["error"] = {
                    "message": f"Data written successfully but error in preparing output: {str(e)}",
                    "stacktrace": traceback.format_exc(),
                }
                output["rows_written"] = len(df)
                return output

        except Exception as e:
            output["error"] = {"message": str(e), "stacktrace": traceback.format_exc()}
            return output

    def _map_polars_to_clickhouse_type(self, pol_type: str | pl.DataType) -> str:
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

    def _map_clickhouse_to_polars_type(self, ch_type: str) -> type[pl.DataType]:
        """Convert ClickHouse type string to Polars dtype"""
        type_mapping: dict[str, type[pl.DataType]] = {
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

    def _get_default_value(self, ch_type: str) -> Any:
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

    def process_schema(self, schema: tuple[tuple[str, Any], ...]) -> dict[str, str]:
        """
        Processes Clickhouse schema and returns a mapping of column names
        to their Livedocs types (NUMBER, DATE, STRING).

        :param schema: Tuple of (column_name, column_type) pairs from Clickhouse
        :return: Dictionary {column_name: livedocs_type}
        """
        # Get schema information
        processed_schema: dict[str, str] = {}

        for col_name, col_type in schema:
            type_name = type(col_type).__name__

            # Handle numeric types
            if any(
                t in type_name
                for t in [
                    "Int",
                    "UInt",
                    "Float",
                    "Decimal",
                    "FixedString",  # Basic numeric types
                    "Int8",
                    "Int16",
                    "Int32",
                    "Int64",
                    "Int128",
                    "Int256",  # Signed integers
                    "UInt8",
                    "UInt16",
                    "UInt32",
                    "UInt64",
                    "UInt128",
                    "UInt256",  # Unsigned integers
                    "Float32",
                    "Float64",  # Floating point
                    "Decimal32",
                    "Decimal64",
                    "Decimal128",
                    "Decimal256",  # Decimal types
                    "Money",
                    "Money64",  # Money types
                ]
            ):
                col_type = "NUMBER"

            # Handle datetime types
            elif any(
                t in type_name
                for t in [
                    "DateTime",
                    "DateTime32",
                    "DateTime64",  # DateTime types
                    "Date",
                    "Date32",  # Date types
                    "Time",
                    "Time32",
                    "Time64",  # Time types
                    "Timestamp",
                    "Timestamp32",
                    "Timestamp64",  # Timestamp types
                ]
            ):
                col_type = "DATE"

            # Handle string types
            elif any(
                t in type_name
                for t in [
                    "String",
                    "FixedString",  # String types
                    "Enum",
                    "Enum8",
                    "Enum16",  # Enum types
                    "UUID",
                    "IPv4",
                    "IPv6",  # Special string types
                    "LowCardinality",  # Low cardinality types
                    "Nullable",  # Nullable types
                ]
            ):
                col_type = "STRING"

            # Handle boolean types
            elif "Bool" in type_name:
                col_type = (
                    "NUMBER"  # Map boolean to NUMBER as it's typically used for 0/1
                )

            # Handle array types
            elif "Array" in type_name:
                col_type = "STRING"  # Map arrays to STRING as they'll be serialized

            # Handle map types
            elif "Map" in type_name:
                col_type = "STRING"  # Map types to STRING as they'll be serialized

            # Handle tuple types
            elif "Tuple" in type_name:
                col_type = "STRING"  # Map tuples to STRING as they'll be serialized

            # Handle nested types
            elif "Nested" in type_name:
                col_type = (
                    "STRING"  # Map nested types to STRING as they'll be serialized
                )

            # Default to STRING for any other types
            else:
                col_type = "STRING"

            processed_schema[col_name] = col_type

        return processed_schema

    def _get_livedocs_type(self, clickhouse_type: str) -> str:
        """
        Map ClickHouse type to LivedocsStandardType.
        Returns: "NUMBER", "DATE", "BOOLEAN", or "STRING"
        """
        numeric_types = [
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "UInt128",
            "UInt256",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Int128",
            "Int256",
            "Float32",
            "Float64",
            "Decimal",
            "Decimal32",
            "Decimal64",
            "Decimal128",
            "Decimal256",
        ]

        date_types = [
            "Date",
            "Date32",
            "DateTime",
            "DateTime32",
            "DateTime64",
            "Time",
            "Time32",
            "Time64",
        ]

        boolean_types = ["Bool", "Boolean"]

        # Remove Nullable() and LowCardinality() wrappers for base type detection
        base_type = clickhouse_type.replace("Nullable(", "").replace(")", "")
        base_type = base_type.replace("LowCardinality(", "").replace(")", "")

        if base_type in numeric_types:
            return "NUMBER"
        elif any(dt in base_type for dt in date_types):
            return "DATE"
        elif base_type in boolean_types:
            return "BOOLEAN"
        else:
            return "STRING"

    def get_schema(
        self, connector_id: str, connection_details: dict[str, Any]
    ) -> list[SchemaNode]:
        """
        Fetch schema information from ClickHouse database and return as list of schema nodes.

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
            validated_connection_details = ClickHouseConnectionDetails(
                **connection_details
            )
        except Exception as e:
            raise ValueError(
                f"Invalid ClickHouse connection details: {e}. "
                f"Expected: name (non-empty string), host (valid hostname/IP), "
                f"port (1-65535), user_name (starts with letter/underscore), "
                f"password (non-empty string)"
            )

        # Get database server name (host)
        db_server_name = validated_connection_details.host

        # Create database node (level 0)
        db_server_node_id = uuid.uuid4()
        db_server_path = db_server_name
        nodes.append(
            SchemaNode(
                id=db_server_node_id,
                connector_id=UUID(connector_id),
                parent_id=None,
                path=db_server_path,
                type=SchemaNodeType.DATABASE,
                name=db_server_name,
                data_type=None,
                livedocs_type=None,
                description=None,
                level=0,
                metadata={"database_type": "clickhouse"},
                created_at=now,
                updated_at=now,
            )
        )

        # Build connection
        client = None
        try:
            client = clickhouse_connect.get_client(
                host=validated_connection_details.host,
                port=validated_connection_details.port,
                user=validated_connection_details.user_name,
                password=validated_connection_details.password,
                secure=True,
            )

            # Query schema details
            schema_details_query = """
                SELECT
                    c.database AS schema_name,
                    sdb.comment AS schema_comment,
                    c.table AS table_name,
                    t.engine AS table_engine,
                    t.comment AS table_comment,
                    c.name AS column_name,
                    c.type AS column_data_type,
                    c.comment AS column_comment,
                    c.position AS column_position
                FROM system.columns AS c
                JOIN system.tables AS t ON c.database = t.database AND c.table = t.name
                LEFT JOIN system.databases AS sdb ON c.database = sdb.name
                WHERE c.database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
                  AND t.database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
                ORDER BY c.database, c.table, c.position
            """

            result = client.query(schema_details_query)
            result_rows = result.result_set

            # NOTE: We assume the databases are schemas in ClickHouse
            schema_node_ids: dict[str, UUID] = {}  # "schemaName" -> nodeId
            table_node_ids: dict[str, UUID] = {}  # "schemaName.tableName" -> nodeId

            for row in result_rows:
                schema_name = row[0]
                schema_comment = row[1]
                table_name = row[2]
                table_engine = row[3]
                table_comment = row[4]
                column_name = row[5]
                column_data_type = row[6]
                column_comment = row[7]

                # Create or get schema node (level 1)
                schema_node_id = schema_node_ids.get(schema_name)
                schema_path = f"{db_server_path}/{schema_name}"
                if not schema_node_id:
                    schema_node_id = uuid.uuid4()
                    nodes.append(
                        SchemaNode(
                            id=schema_node_id,
                            connector_id=UUID(connector_id),
                            parent_id=db_server_node_id,
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
                is_view = table_engine in ("View", "MaterializedView", "LiveView")
                node_type = SchemaNodeType.VIEW if is_view else SchemaNodeType.TABLE

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
                                "engine": table_engine,
                                "database_type": "clickhouse",
                                "schema_name": schema_name,
                                "database_name": db_server_name,
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
                        data_type=column_data_type if column_data_type else None,
                        livedocs_type=(
                            self._get_livedocs_type(column_data_type)
                            if column_data_type
                            else None
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
                sanitize_sensitive_data(f"Error fetching ClickHouse schema: {e}")
            )
        finally:
            if client is not None:
                client.close()

        return nodes
