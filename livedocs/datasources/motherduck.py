from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import quote_plus
from uuid import UUID

import duckdb
import polars as pl
from pydantic import BaseModel, Field, field_validator

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


class MotherduckConnectionDetails(BaseModel):
    """
    Pydantic model for Motherduck connection details.
    Matches the Zod schema validation from the client.

    Expected structure (from form submission):
    {
        name: string,   // Required, non-empty string
        token: string,  // Required, non-empty string (MotherDuck token)
    }

    Note: Additional fields like 'database' and 'default_schema' may be present
    but are not validated as they're not part of the form payload.
    """

    name: str = Field(
        ...,
        min_length=1,
        description="Connector name (required, non-empty string)",
    )
    token: str = Field(
        ...,
        min_length=1,
        description="MotherDuck token (required, non-empty string)",
    )

    class Config:
        extra = "allow"  # Allow additional fields like 'database' and 'default_schema'

    @field_validator("token")
    @classmethod
    def validate_token(cls, v: str) -> str:
        """
        Validate token: non-empty string.
        Matches Zod validation from the client.
        """
        if not v:
            raise ValueError("Token is required")
        return v


class MotherduckDatasourceConnector(BaseDatasourceConnector):
    """
    Datasource connector for Motherduck.
    """

    _INPUT_VIEW_NAME: str = "_livedocs_motherduck_input"

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
            connection_details = MotherduckConnectionDetails(**parsed_credentials)
        except Exception as e:
            raise ValueError(
                f"Invalid Motherduck connection details: {e}. "
                f"Expected: name (non-empty string), token (non-empty string)"
            )

        details = self._extract_details(connection_details.model_dump())

        conn = None
        try:
            conn = self._motherduck_connection(details)
            relation = conn.sql(query)
            result_df = relation.pl()
            columns = relation.columns or []
            dtypes = [str(dtype) for dtype in relation.dtypes or []]
        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error querying Motherduck: {e}")
            )
        finally:
            if conn is not None:
                conn.close()

        schema_rows = [
            {"column_name": column, "data_type": dtype}
            for column, dtype in zip(columns, dtypes)
        ]
        schema_df = (
            pl.DataFrame(schema_rows)
            if schema_rows
            else pl.DataFrame({"column_name": [], "data_type": []})
        )

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
            connection_details = MotherduckConnectionDetails(**parsed_credentials)
        except Exception as e:
            raise ValueError(
                f"Invalid Motherduck connection details: {e}. "
                f"Expected: name (non-empty string), token (non-empty string)"
            )

        details = self._extract_details(connection_details.model_dump())

        write_mode = save_config["write_mode"]
        if write_mode not in {"append", "overwrite"}:
            raise ValueError('write_mode must be either "append" or "overwrite"')

        schema_name = save_config["schema_name"] or details.get("default_schema") or ""
        table_name = save_config["table_name"]
        qualified_table = self._format_table_name(schema_name, table_name)
        has_rows = df.height > 0

        conn = None
        try:
            conn = self._motherduck_connection(details)
            if schema_name:
                _ = conn.execute(
                    f"CREATE SCHEMA IF NOT EXISTS {self._quote_identifier(schema_name)}"
                )

            try:
                _ = conn.register(self._INPUT_VIEW_NAME, df.to_pandas())
            except Exception as e:
                raise RuntimeError(
                    sanitize_sensitive_data(
                        f"Failed to stage DataFrame for Motherduck write: {e}"
                    )
                )

            try:
                table_exists = self._table_exists(conn, schema_name, table_name)

                if write_mode == "overwrite":
                    _ = conn.execute(
                        f"CREATE OR REPLACE TABLE {qualified_table} AS SELECT * FROM {self._INPUT_VIEW_NAME}"
                    )
                else:
                    if not table_exists and not save_config["table_is_new"]:
                        raise ValueError(
                            f"Table {qualified_table} does not exist and table_is_new is False"
                        )

                    if not table_exists:
                        _ = conn.execute(
                            f"CREATE TABLE {qualified_table} AS SELECT * FROM {self._INPUT_VIEW_NAME} LIMIT 0"
                        )

                    if has_rows:
                        _ = conn.execute(
                            f"INSERT INTO {qualified_table} SELECT * FROM {self._INPUT_VIEW_NAME}"
                        )
                result_df = self._prepare_write_result(conn, qualified_table)
            finally:
                try:
                    _ = conn.unregister(self._INPUT_VIEW_NAME)
                except Exception:
                    pass
        except Exception as e:
            raise RuntimeError(sanitize_sensitive_data(f"DBSave Error: {e}"))
        finally:
            if conn is not None:
                conn.close()

        output = QueryResult(
            data=result_df,
            metadata=QueryResultMetadata(
                limit=50,
                offset=0,
                total_rows=df.height,
                cache_info=CacheInfo(id="", status=CacheStatus.MISS),
            ),
        )
        return LivedocsResult(output)

    def teardown(self) -> None:
        pass

    def process_schema(self, schema_data: pl.DataFrame) -> dict[str, str]:
        """
        Process Motherduck schema DataFrame and return a mapping of column names to Livedocs types.
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
            processed_schema[row["column_name"]] = self._map_motherduck_type(
                row[type_column]
            )

        return processed_schema

    def _map_motherduck_type(self, column_type: str) -> str:
        """
        Map common Motherduck/DuckDB data types to the categories expected by Livedocs.
        """
        if not column_type:
            return "STRING"

        normalized = column_type.upper()

        if any(
            keyword in normalized
            for keyword in [
                "INT",
                "DECIMAL",
                "NUMERIC",
                "DOUBLE",
                "REAL",
                "FLOAT",
                "BIGINT",
                "SMALLINT",
                "TINYINT",
            ]
        ):
            return "NUMBER"

        if any(keyword in normalized for keyword in ["DATE", "TIME", "TIMESTAMP"]):
            return "DATE"

        return "STRING"

    def _extract_details(self, connection_details: dict[str, Any]) -> dict[str, Any]:
        database = connection_details.get("database")
        token = connection_details.get("token")

        if not token:
            raise ValueError("Missing Motherduck connection detail: 'token'")

        return {
            "database": database,
            "token": token,
            "default_schema": connection_details.get("default_schema"),
        }

    def _build_connection_uri(self, details: dict[str, Any]) -> str:
        database = details.get("database") or ""
        token = quote_plus(details["token"])
        base = f"md:{database}" if database else "md:"
        separator = "&" if "?" in base else "?"
        return f"{base}{separator}motherduck_token={token}"

    def _motherduck_connection(
        self, details: dict[str, Any]
    ) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(self._build_connection_uri(details))
        try:
            conn.execute("LOAD motherduck")
        except Exception:
            pass
        return conn

    def _quote_identifier(self, identifier: str) -> str:
        return f'"{identifier.replace('"', '""')}"'

    def _format_table_name(self, schema_name: str, table_name: str) -> str:
        if schema_name:
            return f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"
        return self._quote_identifier(table_name)

    def _table_exists(
        self, conn: duckdb.DuckDBPyConnection, schema_name: str, table_name: str
    ) -> bool:
        schema = schema_name or "main"
        result = conn.execute(
            """
            SELECT COUNT(*) > 0
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name = ?
            """,
            [schema, table_name],
        ).fetchone()
        return bool(result[0]) if result else False

    def _prepare_write_result(
        self, conn: duckdb.DuckDBPyConnection, qualified_table: str
    ) -> pl.DataFrame:
        schema_relation = conn.sql(f"SELECT * FROM {qualified_table} LIMIT 0")
        columns = schema_relation.columns or []
        dtypes = [str(dtype) for dtype in schema_relation.dtypes or []]

        preview_relation = conn.sql(f"SELECT * FROM {qualified_table} LIMIT 20")
        preview_df = preview_relation.pl()

        if not columns:
            return preview_df

        types_row = {column: dtype for column, dtype in zip(columns, dtypes)}
        types_schema = {column: pl.Utf8 for column in columns}
        types_df = pl.DataFrame([types_row], schema=types_schema)

        if preview_df.is_empty():
            return types_df

        return pl.concat([types_df, preview_df.select(columns)], how="vertical_relaxed")

    def _get_livedocs_type(self, duckdb_type: str) -> str:
        """
        Map DuckDB/Motherduck type to LivedocsStandardType.
        Returns: "NUMBER", "DATE", "BOOLEAN", or "STRING"
        """
        type_lower = (duckdb_type or "").lower()

        numeric_types = [
            "int",
            "integer",
            "bigint",
            "smallint",
            "tinyint",
            "hugeint",
            "decimal",
            "numeric",
            "double",
            "real",
            "float",
            "float4",
            "float8",
        ]

        date_types = ["date", "time", "timestamp", "timestamptz", "interval"]

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
        Fetch schema information from Motherduck database and return as list of schema nodes.

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
            validated_connection_details = MotherduckConnectionDetails(
                **connection_details
            )
        except Exception as e:
            raise ValueError(
                f"Invalid Motherduck connection details: {e}. "
                f"Expected: name (non-empty string), token (non-empty string)"
            )

        details = self._extract_details(validated_connection_details.model_dump())

        conn = None
        try:
            conn = self._motherduck_connection(details)

            # Motherduck system views to exclude (metadata views, not user data)
            MOTHERDUCK_SYSTEM_VIEWS = (
                "database_snapshots",
                "databases",
                "query_history",
                "recent_queries",
                "owned_shares",
                "shared_with_me",
                "storage_info",
                "storage_info_history",
            )

            # Get all databases in the Motherduck account
            all_dbs_raw = conn.execute("SHOW DATABASES").fetchall()
            all_databases = [
                db[0] for db in all_dbs_raw if db[0] != "md_information_schema"
            ]

            # Process each database
            for database_name in all_databases:
                # Switch to this database
                conn.execute(f"USE {database_name}")

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
                        metadata={"database_type": "motherduck"},
                        created_at=now,
                        updated_at=now,
                    )
                )

                # Query schema details for this database
                schema_details_query = f"""
                    SELECT 
                        c.table_schema,
                        c.table_name,
                        c.column_name,
                        c.data_type,
                        t.table_type,
                        c.ordinal_position
                    FROM 
                        information_schema.columns c
                    JOIN 
                        information_schema.tables t
                        ON c.table_schema = t.table_schema
                        AND c.table_name = t.table_name
                        AND c.table_catalog = t.table_catalog
                    WHERE 
                        c.table_catalog = '{database_name}'
                        AND t.table_catalog = '{database_name}'
                        AND c.table_schema NOT IN ('information_schema', 'pg_catalog')
                        AND t.table_schema NOT IN ('information_schema', 'pg_catalog')
                        AND t.table_name NOT IN {MOTHERDUCK_SYSTEM_VIEWS}
                    ORDER BY 
                        c.table_schema, c.table_name, c.ordinal_position;
                """

                result = conn.execute(schema_details_query).fetchall()

                # Track schema and table nodes for this database
                schema_node_ids: dict[str, UUID] = {}  # "schemaName" -> nodeId
                table_node_ids: dict[str, UUID] = {}  # "schemaName.tableName" -> nodeId

                for row in result:
                    table_schema = row[0]
                    table_name = row[1]
                    column_name = row[2]
                    data_type = row[3]
                    table_type = row[4]

                    # Create or get schema node (level 1)
                    schema_node_id = schema_node_ids.get(table_schema)
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
                                name=table_schema,
                                data_type=None,
                                livedocs_type=None,
                                description=None,
                                level=1,
                                metadata={},
                                created_at=now,
                                updated_at=now,
                            )
                        )
                        schema_node_ids[table_schema] = schema_node_id

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
                                name=table_name,
                                data_type=None,
                                livedocs_type=None,
                                description=None,
                                level=2,
                                metadata={
                                    "database_type": "motherduck",
                                    "schema_name": table_schema,
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
                                self._get_livedocs_type(data_type)
                                if data_type
                                else None
                            ),
                            description=None,
                            level=3,
                            metadata={},
                            created_at=now,
                            updated_at=now,
                        )
                    )

        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error fetching Motherduck schema: {e}")
            )
        finally:
            if conn is not None:
                conn.close()

        return nodes
