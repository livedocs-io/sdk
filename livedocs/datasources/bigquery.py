from __future__ import annotations

import json
import re
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import UUID

import polars as pl
from google.cloud import bigquery
from google.oauth2 import service_account
from pydantic import BaseModel, Field, field_validator
from typing_extensions import Literal

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


class BigQueryServiceAccountKey(BaseModel):
    """
    Pydantic model for BigQuery service account key JSON structure.

    Required fields:
    - type: string
    - project_id: string
    - private_key_id: string
    - private_key: string
    - client_email: string
    - client_id: string

    Additional fields (auth_uri, token_uri, etc.) are allowed but not validated.
    """

    type: str = Field(
        ..., description="Service account type, typically 'service_account'"
    )
    project_id: str = Field(..., description="GCP project ID")
    private_key_id: str = Field(..., description="Private key ID")
    private_key: str = Field(..., description="Private key in PEM format")
    client_email: str = Field(..., description="Service account email")
    client_id: str = Field(..., description="Client ID")

    class Config:
        extra = "allow"  # Allow additional fields like auth_uri, token_uri, etc.


class BigQueryConnectionDetails(BaseModel):
    """
    Pydantic model for BigQuery connection details.
    Matches the Zod schema validation from the client.

    Expected structure (from form submission):
    {
        name: string,                    // Required, min length 1
        project_id: string,              // Required, 6-30 chars, must start/end with alphanumeric
        service_account_key: string,      // Required, valid JSON string containing service account key
    }

    The service_account_key JSON string must contain:
    - type: string
    - project_id: string
    - private_key_id: string
    - private_key: string
    - client_email: string
    - client_id: string
    - Additional fields (auth_uri, token_uri, etc.) are allowed but optional.
    """

    name: str = Field(
        ...,
        min_length=1,
        description="Connector name (required, non-empty string)",
    )
    project_id: str = Field(
        ...,
        min_length=6,
        max_length=30,
        description="GCP project ID (6-30 chars, must start and end with alphanumeric)",
    )
    service_account_key: str = Field(
        ..., description="Service account key as JSON string"
    )

    @field_validator("project_id")
    @classmethod
    def validate_project_id(cls, v: str) -> str:
        """
        Validate project_id format: must start and end with lowercase letter or number,
        and can only contain lowercase letters, numbers, and hyphens in the middle.
        Matches Zod validation: /^[a-z0-9][a-z0-9\-]*[a-z0-9]$/
        Note: Length validation (6-30 chars) is handled by Field constraints above.
        """
        if not v:
            raise ValueError("Project ID is required")
        if len(v) < 6:
            raise ValueError("Project ID must be at least 6 characters")
        if len(v) > 30:
            raise ValueError("Project ID must be at most 30 characters")
        if not re.match(r"^[a-z0-9][a-z0-9-]*[a-z0-9]$", v):
            raise ValueError(
                "Project ID must start and end with a lowercase letter or number, "
                "and can only contain lowercase letters, numbers, and hyphens"
            )
        return v

    @field_validator("service_account_key")
    @classmethod
    def validate_service_account_key_json(cls, v: str) -> str:
        """
        Validate that service_account_key is valid JSON with required fields.
        Matches Zod validation from the client.
        """
        if not v or len(v.strip()) == 0:
            raise ValueError("Service account key is required")

        try:
            parsed = json.loads(v)
        except json.JSONDecodeError:
            raise ValueError(
                "Service account key must be valid JSON with required fields "
                "(type, project_id, private_key_id, private_key, client_email, client_id)"
            )

        # Validate the parsed JSON matches the service account key structure
        # This matches the Zod refine validation
        if not isinstance(parsed, dict):
            raise ValueError(
                "Service account key must be valid JSON with required fields "
                "(type, project_id, private_key_id, private_key, client_email, client_id)"
            )

        # Check all required fields are present and are strings (matching Zod validation)
        required_fields = [
            "type",
            "project_id",
            "private_key_id",
            "private_key",
            "client_email",
            "client_id",
        ]
        for field_name in required_fields:
            if field_name not in parsed:
                raise ValueError(
                    "Service account key must be valid JSON with required fields "
                    "(type, project_id, private_key_id, private_key, client_email, client_id)"
                )
            if not isinstance(parsed[field_name], str):
                raise ValueError(
                    "Service account key must be valid JSON with required fields "
                    "(type, project_id, private_key_id, private_key, client_email, client_id)"
                )

        # Additional validation using Pydantic model for type safety
        try:
            BigQueryServiceAccountKey(**parsed)
        except Exception:
            # If Pydantic validation fails, we've already checked the basic structure above
            # so this is just for additional type safety
            pass

        return v

    def get_parsed_service_account_key(self) -> BigQueryServiceAccountKey:
        """Parse and return the service account key as a validated model"""
        parsed = json.loads(self.service_account_key)
        return BigQueryServiceAccountKey(**parsed)


class BigQueryDatasourceConnector(BaseDatasourceConnector):
    """
    Datasource connector for BigQuery.
    """

    def read(
        self,
        query: str,
        datasource: ElementDataSource,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
    ) -> tuple[pl.DataFrame, object]:
        try:
            if datasource["database_info"] is None:
                raise ValueError("Missing required information: 'database_info'")
            db_connector_id = datasource["database_info"]["database_connector_id"]
            _, outer_parsed = get_database_details(db_connector_id)
        except KeyError as e:
            raise ValueError(f"Missing required information: {e}")

        # Validate connection details using Pydantic
        try:
            connection_details = BigQueryConnectionDetails(**outer_parsed)
        except Exception as e:
            raise ValueError(
                f"Invalid BigQuery connection details: {e}. "
                f"Expected: project_id (6-30 chars, lowercase alphanumeric with hyphens), "
                f"service_account_key (valid JSON string with type, project_id, private_key_id, "
                f"private_key, client_email, client_id)"
            )

        service_account_key = connection_details.get_parsed_service_account_key()

        try:
            credentials = service_account.Credentials.from_service_account_info(
                service_account_key.model_dump()
            )
            client = bigquery.Client(
                credentials=credentials, project=connection_details.project_id
            )

            query_job = client.query(query)
            schema = query_job.result().schema
            df_pointer = query_job.to_dataframe(create_bqstorage_client=True)
            df_polars = pl.from_pandas(df_pointer)
        except Exception as e:
            raise RuntimeError(sanitize_sensitive_data(f"Error querying BigQuery: {e}"))

        return df_polars, schema

    def write(
        self,
        df: pl.DataFrame,
        save_config: DBSaveConfig,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
    ) -> LivedocsResult:
        try:
            db_connector_id = save_config["database_id"]
            _, outer_parsed = get_database_details(db_connector_id)
        except KeyError as e:
            raise ValueError(f"Missing required information: {e}")

        # Validate connection details using Pydantic
        try:
            connection_details = BigQueryConnectionDetails(**outer_parsed)
        except Exception as e:
            raise ValueError(
                f"Invalid BigQuery connection details: {e}. "
                f"Expected: project_id (6-30 chars, lowercase alphanumeric with hyphens), "
                f"service_account_key (valid JSON string with type, project_id, private_key_id, "
                f"private_key, client_email, client_id)"
            )

        service_account_key = connection_details.get_parsed_service_account_key()

        try:
            qualified_table_name = f"{connection_details.project_id}.{save_config['schema_name']}.{save_config['table_name']}"

            credentials = service_account.Credentials.from_service_account_info(
                service_account_key.model_dump()
            )
            client = bigquery.Client(
                credentials=credentials, project=connection_details.project_id
            )

            result = self._write_df_to_bigquery(
                df,
                client,
                qualified_table_name,
                save_config["table_is_new"],
                save_config["write_mode"],
            )

            if result["error"]:
                raise RuntimeError(
                    sanitize_sensitive_data(
                        f"Error writing to BigQuery: {result['error']}"
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

    def _write_df_to_bigquery(
        self,
        df: pl.DataFrame,
        client: bigquery.Client,
        table_name: str,
        create_table: bool = False,
        write_mode: Literal["append", "overwrite"] = "append",
    ) -> dict[str, Any]:
        """
        Write a Polars DataFrame to a BigQuery table with schema alignment.

        Args:
            df: Polars DataFrame to write
            client: BigQuery client
            table_name: Fully-qualified table name (project.dataset.table)
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

            table = None
            try:
                table = client.get_table(table_name)
            except Exception as e:
                if create_table:
                    # Create schema from DataFrame
                    schema = []
                    for col_name, dtype in zip(df.columns, df.dtypes):
                        bq_type = self._map_polars_to_bigquery_type(dtype)
                        schema.append(bigquery.SchemaField(col_name, bq_type))

                    # Create the table
                    table = bigquery.Table(table_name, schema=schema)
                    table = client.create_table(table, exists_ok=True)
                else:
                    raise ValueError(
                        f"Table {table_name} does not exist and create_table=False"
                    )

            # Get table schema
            bq_schema = {
                field.name: {"type": field.field_type, "mode": field.mode}
                for field in table.schema
            }

            # Check for missing required columns
            missing_required = [
                col
                for col, info in bq_schema.items()
                if info["mode"] == "REQUIRED" and col not in df.columns
            ]

            if missing_required:
                raise ValueError(f"Missing required columns: {missing_required}")

            # Prepare expressions for schema alignment
            expressions = []
            for col_name, info in bq_schema.items():
                bq_type = info["type"]
                nullable = info["mode"] != "REQUIRED"
                target_type = self._map_bigquery_to_polars_type(bq_type)

                if col_name in df.columns:
                    expr = pl.col(col_name)

                    if bq_type == "TIMESTAMP":
                        expr = expr.cast(pl.Datetime).dt.cast_time_unit("us")
                    else:
                        expr = expr.cast(target_type)

                    # Handle null values for non-nullable columns
                    if not nullable:
                        default_val = self._get_default_value(bq_type)
                        expr = expr.fill_null(default_val)

                    expressions.append(expr.alias(col_name))
                else:
                    # Add missing columns with default values
                    if nullable:
                        expressions.append(
                            pl.lit(None).cast(target_type).alias(col_name)
                        )
                    else:
                        default_val = self._get_default_value(bq_type)
                        expressions.append(
                            pl.lit(default_val).cast(target_type).alias(col_name)
                        )

            # Align DataFrame schema
            aligned_df = df.select(expressions)

            # Convert to pandas for BigQuery upload
            pd_df = aligned_df.to_pandas()

            # Configure job
            job_config = bigquery.LoadJobConfig()

            if write_mode == "overwrite":
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            else:
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

            # Load data
            job = client.load_table_from_dataframe(
                pd_df, table_name, job_config=job_config
            )

            # Wait for job to complete
            job.result()

            # Prepare output
            try:
                # Convert aligned DataFrame to strings for consistent output
                result_df = aligned_df.head(19).select(
                    [pl.col(col).cast(pl.Utf8) for col in aligned_df.columns]
                )

                # Create types row
                types_row = {
                    col_name: info["type"] for col_name, info in bq_schema.items()
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

    def _map_polars_to_bigquery_type(self, pol_type: str | pl.DataType) -> str:
        """Convert Polars dtype string to BigQuery type string"""
        type_str = str(pol_type).lower()

        if "datetime" in type_str:
            return "TIMESTAMP"
        elif "date" in type_str:
            return "DATE"
        elif "int8" in type_str or "int16" in type_str:
            return "INT64"
        elif "int32" in type_str or "int64" in type_str:
            return "INT64"
        elif "float32" in type_str or "float64" in type_str:
            return "FLOAT64"
        elif "bool" in type_str:
            return "BOOL"
        elif "utf8" in type_str or "string" in type_str:
            return "STRING"
        else:
            return "STRING"

    def _map_bigquery_to_polars_type(self, bq_type: str) -> type[pl.DataType]:
        """Convert BigQuery type string to Polars dtype"""
        type_mapping: dict[str, type[pl.DataType]] = {
            "INT64": pl.Int64,
            "INTEGER": pl.Int64,
            "FLOAT64": pl.Float64,
            "FLOAT": pl.Float64,
            "NUMERIC": pl.Float64,
            "BOOL": pl.Boolean,
            "BOOLEAN": pl.Boolean,
            "STRING": pl.Utf8,
            "DATE": pl.Date,
            "DATETIME": pl.Datetime,
            "TIMESTAMP": pl.Datetime,
            "TIME": pl.Time,
        }

        return type_mapping.get(bq_type.upper(), pl.Utf8)

    def _get_default_value(self, bq_type: str) -> Any:
        """Get default value for a BigQuery type"""
        if bq_type.upper() in ("INT64", "INTEGER"):
            return 0
        elif bq_type.upper() in ("FLOAT64", "FLOAT", "NUMERIC"):
            return 0.0
        elif bq_type.upper() in ("BOOL", "BOOLEAN"):
            return False
        elif bq_type.upper() in ("TIMESTAMP", "DATETIME"):
            return datetime.now(timezone.utc)
        elif bq_type.upper() == "DATE":
            return datetime.now(timezone.utc).date()
        else:
            return ""

    def process_schema(self, schema: Any) -> dict[str, str]:
        """
        Processes BigQuery schema and returns a mapping of column names
        to their Livedocs types (NUMBER, DATE, STRING).

        :param schema: List of SchemaField objects from BigQuery
        :return: Dictionary {column_name: livedocs_type}
        """
        processed_schema: dict[str, str] = {}

        for field in schema:
            processed_schema[field.name] = self._map_bigquery_type(field.field_type)

        return processed_schema

    def _map_bigquery_type(self, bigquery_type: str) -> str:
        """
        Maps BigQuery types to Livedocs types: NUMBER, DATE, STRING.
        """
        bigquery_type = bigquery_type.upper()

        # Mapping to NUMBER
        if bigquery_type in (
            "INT64",
            "INT",
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "BYTEINT",
            "NUMERIC",
            "DECIMAL",
            "BIGNUMERIC",
            "BIGDECIMAL",
            "FLOAT64",
            "NUMERIC DECIMAL",
            "BIGNUMERIC DECIMAL",
        ):
            return "NUMBER"

        # Mapping to DATE
        elif bigquery_type in ("DATE", "DATETIME", "TIME", "TIMESTAMP"):
            return "DATE"

        # Mapping to STRING (default case)
        else:
            return "STRING"

    def _get_livedocs_type(self, bq_type: str) -> str:
        """
        Map BigQuery type to LivedocsStandardType.
        Returns: "NUMBER", "DATE", "BOOLEAN", or "STRING"
        """
        type_upper = bq_type.upper()

        numeric_types = [
            "INT64",
            "INT",
            "INTEGER",
            "SMALLINT",
            "TINYINT",
            "BYTEINT",
            "NUMERIC",
            "DECIMAL",
            "BIGNUMERIC",
            "BIGDECIMAL",
            "FLOAT64",
            "FLOAT",
            "DOUBLE",
        ]

        date_types = ["DATE", "DATETIME", "TIME", "TIMESTAMP"]

        boolean_type = "BOOL"

        if type_upper in numeric_types:
            return "NUMBER"
        elif type_upper in date_types:
            return "DATE"
        elif type_upper == boolean_type:
            return "BOOLEAN"
        else:
            return "STRING"

    def get_schema(
        self, connector_id: str, connection_details: dict[str, Any]
    ) -> list[SchemaNode]:
        """
        Fetch schema information from BigQuery and return as list of schema nodes.

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
            validated_connection_details = BigQueryConnectionDetails(
                **connection_details
            )
        except Exception as e:
            raise ValueError(
                f"Invalid BigQuery connection details: {e}. "
                f"Expected: project_id (6-30 chars, lowercase alphanumeric with hyphens), "
                f"service_account_key (valid JSON string with type, project_id, private_key_id, "
                f"private_key, client_email, client_id)"
            )

        project_id = validated_connection_details.project_id

        # Create project node (level 0) - BigQuery uses projects as the top level
        project_node_id = uuid.uuid4()
        project_path = project_id
        nodes.append(
            SchemaNode(
                id=project_node_id,
                connector_id=UUID(connector_id),
                parent_id=None,
                path=project_path,
                type=SchemaNodeType.DATABASE,
                name=project_id,
                data_type=None,
                livedocs_type=None,
                description=f"BigQuery Project: {project_id}",
                level=0,
                metadata={"database_type": "bigquery"},
                created_at=now,
                updated_at=now,
            )
        )

        # Initialize BigQuery client
        service_account_key = (
            validated_connection_details.get_parsed_service_account_key()
        )
        credentials = service_account.Credentials.from_service_account_info(
            service_account_key.model_dump()
        )
        client = bigquery.Client(credentials=credentials, project=project_id)

        try:
            # Get all datasets
            datasets_query = f"SELECT schema_name, location FROM `{project_id}.INFORMATION_SCHEMA.SCHEMATA`;"
            datasets_job = client.query(datasets_query)
            bq_dataset_rows = datasets_job.result()

            if not bq_dataset_rows:
                return nodes

            # Create dataset nodes and map dataset names to their node IDs
            dataset_node_info_map: dict[str, dict[str, Any]] = {}

            for ds_row in bq_dataset_rows:
                schema_name = ds_row.schema_name
                location = ds_row.location

                # Get dataset metadata
                description = f"Dataset: {schema_name}"
                try:
                    dataset = client.dataset(schema_name)
                    metadata = dataset.get_metadata()
                    description = (
                        metadata.description or metadata.friendly_name or description
                    )
                except Exception:
                    pass  # Use default description if metadata fetch fails

                dataset_node_id = uuid.uuid4()
                dataset_path = f"{project_path}/{schema_name}"
                nodes.append(
                    SchemaNode(
                        id=dataset_node_id,
                        connector_id=UUID(connector_id),
                        parent_id=project_node_id,
                        path=dataset_path,
                        type=SchemaNodeType.SCHEMA,
                        name=schema_name,
                        data_type=None,
                        livedocs_type=None,
                        description=description,
                        level=1,
                        metadata={"location": location},
                        created_at=now,
                        updated_at=now,
                    )
                )
                dataset_node_info_map[schema_name] = {
                    "id": dataset_node_id,
                    "path": dataset_path,
                    "location": location,
                }

            # Group datasets by region for regional INFORMATION_SCHEMA calls
            unique_regions = list(
                set(
                    ds_row.location
                    for ds_row in client.query(
                        f"SELECT DISTINCT location FROM `{project_id}.INFORMATION_SCHEMA.SCHEMATA`;"
                    ).result()
                )
            )

            all_table_rows: list[dict[str, Any]] = []
            all_column_rows: list[dict[str, Any]] = []

            for region in unique_regions:
                # Get datasets in this region
                datasets_in_region_query = f"""
                    SELECT schema_name
                    FROM `{project_id}.INFORMATION_SCHEMA.SCHEMATA`
                    WHERE location = '{region}';
                """
                datasets_in_region = [
                    row.schema_name
                    for row in client.query(datasets_in_region_query).result()
                ]

                if not datasets_in_region:
                    continue

                dataset_filter = ", ".join(f"'{ds}'" for ds in datasets_in_region)

                # Fetch TABLES for the region
                tables_query = f"""
                    SELECT table_catalog, table_schema, table_name, table_type
                    FROM `region-{region}.INFORMATION_SCHEMA.TABLES`
                    WHERE table_schema IN ({dataset_filter});
                """
                regional_table_rows = client.query(tables_query).result()
                for row in regional_table_rows:
                    all_table_rows.append(
                        {
                            "table_catalog": row.table_catalog,
                            "table_schema": row.table_schema,
                            "table_name": row.table_name,
                            "table_type": row.table_type,
                        }
                    )

                # Fetch COLUMNS for the region
                columns_query = f"""
                    SELECT table_catalog, table_schema, table_name, column_name, ordinal_position, data_type
                    FROM `region-{region}.INFORMATION_SCHEMA.COLUMNS`
                    WHERE table_schema IN ({dataset_filter})
                    ORDER BY table_schema, table_name, ordinal_position;
                """
                regional_column_rows = client.query(columns_query).result()
                for row in regional_column_rows:
                    all_column_rows.append(
                        {
                            "table_catalog": row.table_catalog,
                            "table_schema": row.table_schema,
                            "table_name": row.table_name,
                            "column_name": row.column_name,
                            "ordinal_position": row.ordinal_position,
                            "data_type": row.data_type,
                        }
                    )

            # Process all fetched table and column rows
            table_node_data_map: dict[str, dict[str, Any]] = {}

            # Create Table/View Nodes (Level 2)
            for table_row in all_table_rows:
                dataset_info = dataset_node_info_map.get(table_row["table_schema"])
                if not dataset_info:
                    continue

                table_node_id = uuid.uuid4()
                table_path = f"{dataset_info['path']}/{table_row['table_name']}"
                node_type = (
                    SchemaNodeType.VIEW
                    if table_row["table_type"] in ("VIEW", "MATERIALIZED VIEW")
                    else SchemaNodeType.TABLE
                )

                nodes.append(
                    SchemaNode(
                        id=table_node_id,
                        connector_id=UUID(connector_id),
                        parent_id=dataset_info["id"],
                        path=table_path,
                        type=node_type,
                        name=table_row["table_name"],
                        data_type=None,
                        livedocs_type=None,
                        description=f"{node_type.value}: {table_row['table_name']}",
                        level=2,
                        metadata={
                            "table_type": table_row["table_type"],
                            "location": dataset_info["location"],
                            "database_type": "bigquery",
                            "schema_name": table_row["table_schema"],
                            "database_name": project_id,
                        },
                        created_at=now,
                        updated_at=now,
                    )
                )
                table_node_data_map[
                    f"{table_row['table_schema']}.{table_row['table_name']}"
                ] = {
                    "id": table_node_id,
                    "path": table_path,
                    "parentId": dataset_info["id"],
                }

            # Create Column Nodes (Level 3)
            for col_row in all_column_rows:
                table_key = f"{col_row['table_schema']}.{col_row['table_name']}"
                table_node_info = table_node_data_map.get(table_key)

                if not table_node_info:
                    continue

                nodes.append(
                    SchemaNode(
                        id=uuid.uuid4(),
                        connector_id=UUID(connector_id),
                        parent_id=table_node_info["id"],
                        path=f"{table_node_info['path']}/{col_row['column_name']}",
                        type=SchemaNodeType.COLUMN,
                        name=col_row["column_name"],
                        data_type=col_row["data_type"],
                        livedocs_type=self._get_livedocs_type(col_row["data_type"]),
                        description=None,
                        level=3,
                        metadata={},
                        created_at=now,
                        updated_at=now,
                    )
                )

        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(f"Error fetching BigQuery schema: {e}")
            )

        return nodes
