from __future__ import annotations

import re
import traceback
import uuid
from collections import OrderedDict
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlparse
from uuid import UUID

import polars as pl
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
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


class PostgresConnectionUrl(BaseModel):
    """
    Pydantic model for PostgreSQL connection using connection URL.
    Matches the Zod schema validation from the client.

    Expected structure:
    {
        name: string,                    // Required, non-empty string
        connect_using: "url",            // Literal "url"
        connection_url: string,          // Required, valid PostgreSQL URL
        host?: string,                   // Optional
        port?: number,                   // Optional
        database?: string,                // Optional
        user_name?: string,              // Optional
        password?: string,               // Optional
    }
    """

    name: str = Field(
        ...,
        min_length=1,
        description="Connector name (required, non-empty string)",
    )
    connect_using: Literal["url"] = Field(..., description="Connection method: 'url'")
    connection_url: str = Field(
        ...,
        min_length=1,
        description="PostgreSQL connection URL (required, must start with postgres:// or postgresql://)",
    )
    host: str | None = Field(None, description="Host (optional)")
    port: int | None = Field(None, description="Port (optional)")
    database: str | None = Field(None, description="Database (optional)")
    user_name: str | None = Field(None, description="Username (optional)")
    password: str | None = Field(None, description="Password (optional)")

    @field_validator("connection_url")
    @classmethod
    def validate_connection_url(cls, v: str) -> str:
        """
        Validate connection URL: must be a valid PostgreSQL URL.
        Matches Zod validation from the client.
        """
        if not v:
            raise ValueError("Connection URL is required")
        try:
            parsed = urlparse(v)
            if parsed.scheme not in ("postgres", "postgresql"):
                raise ValueError(
                    "Connection URL must be a valid PostgreSQL URL "
                    "(postgres:// or postgresql://)"
                )
        except Exception:
            raise ValueError(
                "Connection URL must be a valid PostgreSQL URL "
                "(postgres:// or postgresql://)"
            )
        return v


class PostgresConnectionCredentials(BaseModel):
    """
    Pydantic model for PostgreSQL connection using individual credentials.
    Matches the Zod schema validation from the client.

    Expected structure:
    {
        name: string,                    // Required, non-empty string
        connect_using: "credentials",    // Literal "credentials"
        host: string,                    // Required, valid hostname or IP address
        port: number,                    // Required, integer between 1 and 65535
        database: string,                // Required, starts with letter/underscore
        user_name: string,               // Required, starts with letter/underscore
        password: string,                // Required, non-empty string
        connection_url?: string,         // Optional
    }
    """

    name: str = Field(
        ...,
        min_length=1,
        description="Connector name (required, non-empty string)",
    )
    connect_using: Literal["credentials"] = Field(
        ..., description="Connection method: 'credentials'"
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
    database: str = Field(
        ...,
        min_length=1,
        description="Database name (required, starts with letter/underscore, alphanumeric + underscores)",
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
    connection_url: str | None = Field(None, description="Connection URL (optional)")

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
        Note: Range validation (1-65535) is handled by Field constraints above.
        """
        if v < 1 or v > 65535:
            raise ValueError("Port must be between 1 and 65535")
        return v

    @field_validator("database")
    @classmethod
    def validate_database(cls, v: str) -> str:
        """
        Validate database name: must start with letter or underscore,
        and contain only letters, numbers, and underscores.
        Matches Zod validation from the client.
        """
        if not v:
            raise ValueError("Database name is required")
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError(
                "Database name must start with a letter or underscore and contain only letters, numbers, and underscores"
            )
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


# Discriminated union type for PostgreSQL connection details
PostgresConnectionDetails = Union[PostgresConnectionUrl, PostgresConnectionCredentials]


class PostgresDatasourceConnector(BaseDatasourceConnector):
    """
    Datasource connector for PostgreSQL.
    """

    def read(
        self,
        query: str,
        datasource: ElementDataSource,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
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
                f"Invalid PostgreSQL connection details: {e}. "
                f"Expected: connect_using ('url' or 'credentials'), "
                f"name (non-empty string), and either connection_url (for 'url') "
                f"or host, port, database, user_name, password (for 'credentials')"
            )

        try:
            connection_string = self._build_connection_string(connection_details)
        except ValueError as e:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error building PostgreSQL connection: {e}")
            )

        schema_rows: list[dict[str, str]] = []

        try:
            with psycopg.connect(connection_string) as conn:
                with conn.cursor(row_factory=dict_row) as cursor:
                    _ = cursor.execute(query)  # type: ignore[arg-type]
                    rows = cursor.fetchall()
                    description = cursor.description or ()
                    column_names = [desc.name for desc in description]

                schema_rows = self._schema_from_description(conn, description)
        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error executing PostgreSQL query: {e}")
            )

        schema_df = (
            pl.DataFrame(schema_rows)
            if schema_rows
            else pl.DataFrame({"column_name": [], "data_type": []})
        )

        if rows:
            result_df = pl.DataFrame(rows)
            if column_names:
                result_df = result_df.select(column_names)
        elif column_names:
            result_df = pl.DataFrame({column: [] for column in column_names})
        else:
            result_df = pl.DataFrame()

        return result_df, schema_df

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
                f"Invalid PostgreSQL connection details: {e}. "
                f"Expected: connect_using ('url' or 'credentials'), "
                f"name (non-empty string), and either connection_url (for 'url') "
                f"or host, port, database, user_name, password (for 'credentials')"
            )

        try:
            connection_string = self._build_connection_string(connection_details)
        except ValueError as e:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error building PostgreSQL connection: {e}")
            )

        try:
            qualified_table_name = (
                f"{save_config['schema_name']}.{save_config['table_name']}"
            )
            result = self._write_df_to_postgres(
                df,
                connection_string,
                qualified_table_name,
                save_config["table_is_new"],
                save_config["write_mode"],
            )

            if result["error"]:
                raise RuntimeError(
                    sanitize_sensitive_data(
                        f"Error writing to PostgreSQL: {result['error']}"
                    )
                )
            output = QueryResult(
                data=result["result"],
                metadata=QueryResultMetadata(  # type: ignore[typeddict-item]
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

    def teardown(self) -> None:
        pass

    def _validate_connection_details(
        self, parsed_credentials: dict[str, Any]
    ) -> PostgresConnectionUrl | PostgresConnectionCredentials:
        """
        Validate connection details using Pydantic models.
        Returns the validated connection details model.
        """
        connect_using = parsed_credentials.get("connect_using")

        if connect_using == "url":
            return PostgresConnectionUrl(**parsed_credentials)
        elif connect_using == "credentials":
            return PostgresConnectionCredentials(**parsed_credentials)
        else:
            raise ValueError(
                f"Invalid connect_using value: {connect_using}. "
                f"Must be either 'url' or 'credentials'"
            )

    def _build_connection_string(
        self,
        connection_details: PostgresConnectionUrl | PostgresConnectionCredentials,
    ) -> str:
        """
        Build PostgreSQL connection string from validated Pydantic model.
        Handles both URL-based and credentials-based connections.
        """
        if isinstance(connection_details, PostgresConnectionUrl):
            # Use connection URL directly
            return connection_details.connection_url
        else:
            # Build connection URL from individual credentials
            return self._create_postgres_connection_url(connection_details)

    def _create_postgres_connection_url(
        self, details: PostgresConnectionCredentials
    ) -> str:
        """
        Create PostgreSQL connection URL from credentials.
        All fields are required in PostgresConnectionCredentials, so they're never None.
        """
        user = details.user_name
        password = details.password
        host = details.host
        port = details.port
        database = details.database

        # All fields are required in PostgresConnectionCredentials
        if password:
            return f"postgresql://{user}:{password}@{host}:{port}/{database}"
        else:
            return f"postgresql://{user}@{host}:{port}/{database}"

    def _schema_from_description(
        self, conn: psycopg.Connection, description: Sequence[Any]
    ) -> list[dict[str, Any]]:
        if not description:
            return []

        type_oids = [
            type_code
            for type_code in {getattr(desc, "type_code", None) for desc in description}
            if type_code is not None
        ]

        type_map: dict[Any, dict[str, Any]] = {}

        if type_oids:
            with conn.cursor(row_factory=dict_row) as meta_cursor:
                meta_cursor.execute(
                    """
                    SELECT
                        t.oid,
                        pg_catalog.format_type(t.oid, NULL) AS data_type,
                        t.typcategory,
                        t.typname,
                        n.nspname AS type_schema
                    FROM pg_catalog.pg_type t
                    JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
                    WHERE t.oid = ANY(%s)
                    """,
                    (type_oids,),
                )

                for row in meta_cursor.fetchall() or []:
                    type_map[row["oid"]] = row

        schema_rows: list[dict[str, Any]] = []
        for desc in description:
            type_info = type_map.get(getattr(desc, "type_code", None), {})
            data_type = type_info.get("data_type", "TEXT")

            schema_rows.append(
                {
                    "column_name": getattr(desc, "name", ""),
                    "data_type": data_type,
                }
            )

        return schema_rows

    def _write_df_to_postgres(
        self,
        df: pl.DataFrame,
        connection_string: str,
        table_name: str,
        create_table: bool = False,
        write_mode: Literal["append", "overwrite"] = "append",
    ) -> dict[str, Any]:
        """
        Aligns a Polars DataFrame to match a Postgres table schema and writes the DataFrame to the table.

        Args:
            df: Polars DataFrame to align
            connection_string: psycopg connection string
            table_name: Fully qualified table name (database optional)
            create_table: If True, create the table before writing
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

            schema_name, table_identifier = self._parse_table_identifier(table_name)
            aligned_df = df.clone()
            table_schema: list[dict[str, Any]] = []
            rows_to_insert: list[tuple[Any, ...]] = []

            with psycopg.connect(connection_string) as conn:
                with conn.cursor(row_factory=dict_row) as cursor:
                    if create_table:
                        self._ensure_schema_exists(cursor, schema_name)
                        if not self._table_exists(
                            cursor, schema_name, table_identifier
                        ):
                            self._create_table_from_df(
                                cursor, schema_name, table_identifier, df
                            )

                    table_schema = self._fetch_postgres_schema(
                        cursor, schema_name, table_identifier
                    )

                    if not table_schema:
                        raise ValueError(
                            f"Table {schema_name}.{table_identifier} does not exist and create_table is False"
                        )

                    self._validate_required_columns(df, table_schema)

                    aligned_df = self._align_dataframe_to_schema(df, table_schema)

                    if write_mode == "overwrite":
                        self._truncate_table(cursor, schema_name, table_identifier)

                    rows_to_insert = self._prepare_rows_for_insert(
                        aligned_df, table_schema
                    )

                    if rows_to_insert:
                        insert_query = self._build_insert_query(
                            schema_name, table_identifier, table_schema
                        )
                        cursor.executemany(insert_query, rows_to_insert)

            output["result"], output["rows_written"] = self._prepare_result_payload(
                aligned_df, table_schema, len(rows_to_insert)
            )

            return output

        except Exception as e:
            output["error"] = {"message": str(e), "stacktrace": traceback.format_exc()}
            return output

    def _parse_table_identifier(self, table_name: str) -> tuple[str, str]:
        parts = [part.strip() for part in table_name.split(".") if part.strip()]
        if not parts:
            raise ValueError("table_name cannot be empty")

        if len(parts) == 1:
            return "public", self._strip_identifier(parts[0])

        schema_name = self._strip_identifier(parts[-2])
        table_identifier = self._strip_identifier(parts[-1])

        if not schema_name:
            schema_name = "public"

        return schema_name, table_identifier

    def _strip_identifier(self, identifier: str) -> str:
        identifier = identifier.strip()
        if identifier.startswith('"') and identifier.endswith('"'):
            return identifier[1:-1]
        return identifier

    def _ensure_schema_exists(self, cursor: Any, schema_name: str) -> None:
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.schemata
            WHERE schema_name = %s
            """,
            (schema_name,),
        )
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(schema_name)
                )
            )

    def _table_exists(
        self, cursor: Any, schema_name: str, table_identifier: str
    ) -> bool:
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            ) AS exists
            """,
            (schema_name, table_identifier),
        )
        result = cursor.fetchone() or {}
        return bool(result.get("exists"))

    def _create_table_from_df(
        self, cursor: Any, schema_name: str, table_identifier: str, df: pl.DataFrame
    ) -> None:
        if not df.columns:
            raise ValueError("Cannot create a table without columns")

        columns_sql = []
        for name, dtype in df.schema.items():
            postgres_type = self._polars_to_postgres_type(dtype)
            columns_sql.append(
                sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(postgres_type))
            )

        create_stmt = sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_identifier),
            sql.SQL(", ").join(columns_sql),
        )

        cursor.execute(create_stmt)

    def _polars_to_postgres_type(self, dtype: pl.DataType) -> str:
        type_str = str(dtype)

        if type_str.startswith("Decimal"):
            inside = type_str[type_str.find("(") + 1 : type_str.rfind(")")]
            precision = None
            scale = None
            for part in inside.replace(" ", "").split(","):
                if part.startswith("precision="):
                    precision = part.split("=", 1)[1]
                elif part.startswith("scale="):
                    scale = part.split("=", 1)[1]
                elif not precision:
                    precision = part
                elif not scale:
                    scale = part
            if precision and scale:
                return f"DECIMAL({precision},{scale})"
            return "NUMERIC"

        lower = type_str.lower()

        if lower in {"int8", "uint8", "int16", "uint16"}:
            return "SMALLINT"
        if lower in {"int32", "uint32"}:
            return "INTEGER"
        if lower in {"int64", "uint64"}:
            return "BIGINT"
        if lower == "float32":
            return "REAL"
        if lower == "float64":
            return "DOUBLE PRECISION"
        if lower == "boolean":
            return "BOOLEAN"
        if lower.startswith("datetime"):
            return "TIMESTAMP WITH TIME ZONE"
        if lower == "date":
            return "DATE"
        if lower == "time":
            return "TIME"
        if lower == "binary":
            return "BYTEA"
        if (
            lower.startswith("list")
            or lower.startswith("struct")
            or lower.startswith("object")
        ):
            return "JSONB"

        return "TEXT"

    def _fetch_postgres_schema(
        self, cursor: Any, schema_name: str, table_identifier: str
    ) -> list[dict[str, Any]]:
        schema_query = """
            SELECT
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                NOT a.attnotnull AS is_nullable,
                t.typcategory,
                t.typname,
                tn.nspname AS type_schema
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
            JOIN pg_catalog.pg_namespace tn ON t.typnamespace = tn.oid
            WHERE c.relname = %s
              AND n.nspname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum;
        """

        cursor.execute(schema_query, (table_identifier, schema_name))
        rows = cursor.fetchall() or []

        schema: list[dict[str, Any]] = []
        for row in rows:
            enum_values = None
            if row.get("typcategory") == "E":
                enum_values = self._fetch_enum_values(
                    cursor, row.get("typname"), row.get("type_schema")
                )

            schema.append(
                {
                    "column_name": row.get("column_name"),
                    "data_type": row.get("data_type"),
                    "nullable": bool(row.get("is_nullable")),
                    "enum_values": enum_values,
                }
            )

        return schema

    def _fetch_enum_values(
        self, cursor: Any, enum_type: str, enum_schema: str | None
    ) -> list[str]:
        if not enum_type:
            return []

        cursor.execute(
            """
            SELECT enumlabel
            FROM pg_catalog.pg_enum e
            JOIN pg_catalog.pg_type t ON e.enumtypid = t.oid
            JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
            WHERE t.typname = %s AND n.nspname = COALESCE(%s, n.nspname)
            ORDER BY e.enumsortorder
            """,
            (enum_type, enum_schema),
        )

        return [row["enumlabel"] for row in cursor.fetchall() or []]

    def _validate_required_columns(
        self, df: pl.DataFrame, schema: list[dict[str, Any]]
    ) -> None:
        missing_required = [
            column["column_name"]
            for column in schema
            if not column["nullable"] and column["column_name"] not in df.columns
        ]

        if missing_required:
            raise ValueError(f"Missing required columns: {missing_required}")

    def _postgres_to_polars_type(
        self,
        type_name: str,
    ) -> (
        type[pl.Int64]
        | type[pl.Float64]
        | type[pl.Boolean]
        | type[pl.Datetime]
        | type[pl.Date]
        | type[pl.Time]
        | type[pl.Utf8]
        | type[pl.Binary]
    ):
        type_upper = type_name.upper()

        if any(keyword in type_upper for keyword in ["BIGINT", "INT", "SMALLINT"]):
            return pl.Int64
        if any(
            keyword in type_upper
            for keyword in ["DOUBLE", "NUMERIC", "DECIMAL", "REAL", "FLOAT"]
        ):
            return pl.Float64
        if "BOOL" in type_upper:
            return pl.Boolean
        if "TIMESTAMP" in type_upper:
            return pl.Datetime
        if "DATE" in type_upper:
            return pl.Date
        if "TIME" in type_upper:
            return pl.Time
        if "UUID" in type_upper:
            return pl.Utf8
        if "BYTEA" in type_upper or "BLOB" in type_upper:
            return pl.Binary

        return pl.Utf8

    def _align_dataframe_to_schema(
        self, df: pl.DataFrame, schema: Iterable[dict[str, Any]]
    ) -> pl.DataFrame:
        if df.is_empty():
            base = {column["column_name"]: [] for column in schema}
            return pl.DataFrame(base)

        expressions = []
        for column in schema:
            column_name = column["column_name"]
            data_type = column["data_type"]
            nullable = column["nullable"]
            enum_values = column.get("enum_values") or []
            target_type = self._postgres_to_polars_type(data_type)

            if column_name in df.columns:
                expr = pl.col(column_name)

                if enum_values:
                    if nullable:
                        expr = expr.cast(pl.Utf8, strict=False).map_elements(
                            lambda value: value if value in enum_values else None
                        )
                    else:
                        fallback = enum_values[0]
                        expr = expr.cast(pl.Utf8, strict=False).map_elements(
                            lambda value, fallback=fallback: value
                            if value in enum_values
                            else fallback
                        )
                elif "UUID" in data_type.upper():
                    expr = expr.cast(pl.Utf8, strict=False)
                elif "TIMESTAMP" in data_type.upper():
                    expr = expr.cast(pl.Datetime, strict=False).dt.cast_time_unit("us")
                elif (
                    "DATE" in data_type.upper() and "TIMESTAMP" not in data_type.upper()
                ):
                    expr = expr.cast(pl.Date, strict=False)
                elif (
                    "TIME" in data_type.upper() and "TIMESTAMP" not in data_type.upper()
                ):
                    expr = expr.cast(pl.Time, strict=False)
                else:
                    expr = expr.cast(target_type, strict=False)

                if not nullable:
                    default_value = self._get_default_value_for_postgres_type(
                        data_type, enum_values
                    )
                    expr = expr.fill_null(default_value)

                expressions.append(expr.alias(column_name))
            else:
                if nullable:
                    expressions.append(
                        pl.lit(None).cast(target_type).alias(column_name)
                    )
                else:
                    default_value = self._get_default_value_for_postgres_type(
                        data_type, enum_values
                    )
                    expressions.append(
                        pl.lit(default_value).cast(target_type).alias(column_name)
                    )

        return df.select(expressions)

    def _get_default_value_for_postgres_type(
        self, type_name: str, enum_values: Iterable[str] | None
    ) -> Any:
        if enum_values:
            return next(iter(enum_values), None)

        type_upper = type_name.upper()

        if "INT" in type_upper:
            return 0
        if any(
            keyword in type_upper
            for keyword in ["DOUBLE", "NUMERIC", "DECIMAL", "REAL", "FLOAT"]
        ):
            return 0.0
        if "BOOL" in type_upper:
            return False
        if "TIMESTAMP" in type_upper:
            return datetime.now(timezone.utc)
        if "DATE" in type_upper and "TIMESTAMP" not in type_upper:
            return datetime.now(timezone.utc).date()
        if "TIME" in type_upper:
            return datetime.now(timezone.utc).time()
        if "UUID" in type_upper:
            return str(uuid.uuid4())
        if "BYTEA" in type_upper or "BLOB" in type_upper:
            return b""

        return ""

    def _truncate_table(
        self, cursor: Any, schema_name: str, table_identifier: str
    ) -> None:
        cursor.execute(
            sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(schema_name), sql.Identifier(table_identifier)
            )
        )

    def _prepare_rows_for_insert(
        self, aligned_df: pl.DataFrame, schema: Iterable[dict[str, Any]]
    ) -> list[tuple[Any, ...]]:
        if aligned_df.is_empty():
            return []

        columns = [column["column_name"] for column in schema]
        rows: list[tuple[Any, ...]] = []

        for row in aligned_df.iter_rows(named=True):
            rows.append(tuple(row[column] for column in columns))

        return rows

    def _build_insert_query(
        self, schema_name: str, table_identifier: str, schema: Iterable[dict[str, Any]]
    ) -> Any:
        columns_sql = [sql.Identifier(column["column_name"]) for column in schema]
        placeholders = [sql.Placeholder() for _ in schema]

        return sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_identifier),
            sql.SQL(", ").join(columns_sql),
            sql.SQL(", ").join(placeholders),
        )

    def _prepare_result_payload(
        self,
        aligned_df: pl.DataFrame,
        schema: Iterable[dict[str, Any]],
        rows_written: int,
    ) -> tuple[pl.DataFrame, int]:
        ordered_schema = OrderedDict(
            (column["column_name"], column["data_type"]) for column in schema
        )

        types_df = pl.DataFrame([ordered_schema])

        preview_df = self._cast_dataframe_for_preview(aligned_df, ordered_schema.keys())

        result_df = types_df.vstack(preview_df)

        return result_df, rows_written

    def _cast_dataframe_for_preview(
        self, df: pl.DataFrame, column_order: Iterable[str]
    ) -> pl.DataFrame:
        if df.is_empty():
            return pl.DataFrame({column: [] for column in column_order})

        return df.head(19).select(
            [
                pl.col(column).cast(pl.Utf8, strict=False).alias(column)
                for column in column_order
            ]
        )

    def process_schema(self, schema_data: pl.DataFrame) -> dict[str, str]:
        """
        Process postgres schema DataFrame and return a mapping of column names to Livedocs types.
        This is a public method that can be used by external code.
        """
        if schema_data.is_empty():
            return {}

        type_column = None
        for candidate in ("data_type", "column_type"):
            if candidate in schema_data.columns:
                type_column = candidate
                break

        if type_column is None:
            raise ValueError(
                "Schema DataFrame must contain a 'data_type' or 'column_type' column"
            )

        processed_schema: dict[str, str] = {}
        for row in schema_data.iter_rows(named=True):
            processed_schema[row["column_name"]] = self._map_postgres_type(
                row[type_column]
            )

        return processed_schema

    def _map_postgres_type(self, column_type: str) -> str:
        """
        Map common Postgres data types to the categories expected by Livedocs.
        """
        if not column_type:
            return "STRING"

        normalized = column_type.upper()

        if any(
            keyword in normalized
            for keyword in ["INT", "DECIMAL", "NUMERIC", "DOUBLE", "REAL", "FLOAT"]
        ):
            return "NUMBER"

        if any(keyword in normalized for keyword in ["DATE", "TIME", "TIMESTAMP"]):
            return "DATE"

        return "STRING"

    def _get_livedocs_type(self, pg_type: str) -> str:
        """
        Map PostgreSQL type to LivedocsStandardType.
        Returns: "NUMBER", "DATE", "BOOLEAN", or "STRING"
        """
        type_lower = pg_type.lower()

        numeric_types = [
            "smallint",
            "integer",
            "bigint",
            "decimal",
            "numeric",
            "real",
            "double precision",
            "serial",
            "bigserial",
            "int",
            "int2",
            "int4",
            "int8",
            "float4",
            "float8",
        ]

        date_types = [
            "date",
            "time",
            "timetz",
            "timestamp",
            "timestamptz",
            "interval",
        ]

        boolean_types = ["boolean", "bool"]

        if any(nt in type_lower for nt in numeric_types):
            return "NUMBER"
        elif any(dt in type_lower for dt in date_types):
            return "DATE"
        elif any(bt in type_lower for bt in boolean_types):
            return "BOOLEAN"
        else:
            return "STRING"

    def get_schema(
        self, connector_id: str, connection_details: dict[str, Any]
    ) -> list[SchemaNode]:
        """
        Fetch schema information from PostgreSQL database and return as list of schema nodes.

        Args:
            connector_id: The connector ID to use for schema nodes
            connection_details: Dictionary containing connection details (host, port, database, etc.)

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
                f"Invalid PostgreSQL connection details: {e}. "
                f"Expected: connect_using ('url' or 'credentials'), "
                f"name (non-empty string), and either connection_url (for 'url') "
                f"or host, port, database, user_name, password (for 'credentials')"
            )

        # Get database name
        db_name: str | None = None
        if isinstance(validated_connection_details, PostgresConnectionUrl):
            connection_url = validated_connection_details.connection_url
            parsed_url = urlparse(connection_url)
            db_name = parsed_url.path.lstrip("/")
            if not db_name:
                raise ValueError("Invalid connection URL: database is required")
        else:
            db_name = validated_connection_details.database
            if not db_name:
                raise ValueError(
                    "Database name is not available in connection parameters, cannot build schema."
                )

        # Build connection string
        connection_string = self._build_connection_string(validated_connection_details)

        # Create database node (level 0)
        db_node_id = uuid.uuid4()
        db_path = db_name
        nodes.append(
            SchemaNode(
                id=db_node_id,
                connector_id=UUID(connector_id),
                parent_id=None,
                path=db_path,
                type=SchemaNodeType.DATABASE,
                name=db_name,
                data_type=None,
                livedocs_type=None,
                description=None,
                level=0,
                metadata={"database_type": "postgres"},
                created_at=now,
                updated_at=now,
            )
        )

        # Query schema details
        schema_details_query = """
            SELECT 
                c.table_schema,
                c.table_name,
                c.column_name,
                c.udt_name AS data_type,
                t.table_type,
                col_description(
                    (quote_ident(c.table_schema)||'.'||quote_ident(c.table_name))::regclass::oid, 
                    c.ordinal_position
                ) AS column_description,
                obj_description(
                    (quote_ident(t.table_schema)||'.'||quote_ident(c.table_name))::regclass::oid, 
                    'pg_class'
                ) AS table_description
            FROM 
                information_schema.columns c
            JOIN 
                information_schema.tables t
                ON c.table_schema = t.table_schema
                AND c.table_name = t.table_name
            WHERE 
                c.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast') 
                AND t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY 
                c.table_schema, c.table_name, c.ordinal_position;
        """

        schema_node_ids: dict[str, UUID] = {}  # "schemaName" -> nodeId
        table_node_ids: dict[str, UUID] = {}  # "schemaName.tableName" -> nodeId

        try:
            with psycopg.connect(connection_string) as conn:
                with conn.cursor(row_factory=dict_row) as cursor:
                    cursor.execute(schema_details_query)
                    rows = cursor.fetchall()

                    for row in rows:
                        table_schema = row.get("table_schema")
                        table_name = row.get("table_name")
                        column_name = row.get("column_name")
                        data_type = row.get("data_type")
                        table_type = row.get("table_type")
                        column_description = row.get("column_description")
                        table_description = row.get("table_description")

                        # Create or get schema node (level 1)
                        schema_node_id = schema_node_ids.get(str(table_schema))
                        schema_path = f"{db_path}/{table_schema}"
                        if not schema_node_id:
                            schema_node_id = uuid.uuid4()
                            nodes.append(
                                SchemaNode(
                                    id=schema_node_id,
                                    connector_id=UUID(connector_id),
                                    parent_id=db_node_id,
                                    path=schema_path,
                                    type=SchemaNodeType.SCHEMA,
                                    name=str(table_schema),
                                    data_type=None,
                                    livedocs_type=None,
                                    description=None,
                                    level=1,
                                    metadata={},
                                    created_at=now,
                                    updated_at=now,
                                )
                            )
                            schema_node_ids[str(table_schema)] = schema_node_id

                        # Create or get table/view node (level 2)
                        table_key = f"{table_schema}.{table_name}"
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
                                    name=str(table_name),
                                    data_type=None,
                                    livedocs_type=None,
                                    description=table_description
                                    if table_description
                                    else None,
                                    level=2,
                                    metadata={
                                        "database_type": "postgres",
                                        "schema_name": str(table_schema),
                                        "database_name": db_name,
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
                                name=str(column_name),
                                data_type=str(data_type) if data_type else None,
                                livedocs_type=(
                                    self._get_livedocs_type(str(data_type))
                                    if data_type
                                    else None
                                ),
                                description=(
                                    str(column_description)
                                    if column_description
                                    else None
                                ),
                                level=3,
                                metadata={},
                                created_at=now,
                                updated_at=now,
                            )
                        )

        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error fetching PostgreSQL schema: {e}")
            )

        return nodes
