from __future__ import annotations

import json
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import UUID

import polars as pl
from google.cloud import bigquery
from google.oauth2 import service_account
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

        service_account_parsed = self._parse_service_account(outer_parsed)

        try:
            credentials = service_account.Credentials.from_service_account_info(
                service_account_parsed
            )
            client = bigquery.Client(
                credentials=credentials, project=outer_parsed["project_id"]
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

        service_account_parsed = self._parse_service_account(outer_parsed)

        try:
            qualified_table_name = f"{outer_parsed['project_id']}.{save_config['schema_name']}.{save_config['table_name']}"

            credentials = service_account.Credentials.from_service_account_info(
                service_account_parsed
            )
            client = bigquery.Client(
                credentials=credentials, project=outer_parsed["project_id"]
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

    def _parse_service_account(self, outer_parsed: dict[str, Any]) -> dict[str, Any]:
        try:
            return json.loads(outer_parsed["service_account_key"])
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing connection details: {e}")

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

        project_id = connection_details.get("project_id")
        if not project_id:
            raise ValueError("project_id is required in connection details")

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
                metadata={},
                created_at=now,
                updated_at=now,
            )
        )

        # Initialize BigQuery client
        service_account_parsed = self._parse_service_account(connection_details)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_parsed
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
                        metadata.description
                        or metadata.friendly_name
                        or description
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
