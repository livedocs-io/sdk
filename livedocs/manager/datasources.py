from __future__ import annotations

from typing import Any, Callable

import polars as pl

from livedocs.datasources.base import BaseDatasourceConnector
from livedocs.datasources.bigquery import BigQueryDatasourceConnector
from livedocs.datasources.clickhouse import ClickHouseDatasourceConnector
from livedocs.datasources.dataframe import DataframeDatasourceConnector
from livedocs.datasources.databricks import DatabricksDatasourceConnector
from livedocs.datasources.file import FileDatasourceConnector
from livedocs.datasources.googledrive import GoogleDriveDatasourceConnector
from livedocs.datasources.motherduck import MotherduckDatasourceConnector
from livedocs.datasources.postgres import PostgresDatasourceConnector
from livedocs.datasources.s3 import S3DatasourceConnector
from livedocs.datasources.snowflake import SnowflakeDatasourceConnector
from livedocs.types import (
    CacheInfo,
    CacheStatus,
    DBSaveConfig,
    DatabaseType,
    ElementDataSource,
    ElementDatasourceType,
    FileConnectorType,
    LivedocsResult,
)
from livedocs.utils.common import get_run_context
from livedocs.utils.lib.cache import QueryCache


class DatasourceManager:
    """
    Manager class that handles datasource operations by routing to the appropriate
    connector based on datasource type. This centralizes datasource selection logic
    and reduces code duplication.
    """

    # Mapping from DatabaseType to connector classes (for database datasources)
    _DATABASE_CONNECTOR_MAP: dict[DatabaseType, type[BaseDatasourceConnector]] = {
        DatabaseType.Postgres: PostgresDatasourceConnector,
        DatabaseType.Motherduck: MotherduckDatasourceConnector,
        DatabaseType.Bigquery: BigQueryDatasourceConnector,
        DatabaseType.Snowflake: SnowflakeDatasourceConnector,
        DatabaseType.Clickhouse: ClickHouseDatasourceConnector,
        DatabaseType.Databricks: DatabricksDatasourceConnector,
    }

    @classmethod
    def _get_connector(cls, datasource: ElementDataSource) -> BaseDatasourceConnector:
        """
        Get an instance of the appropriate connector for the given datasource.

        Args:
            datasource: The datasource configuration

        Returns:
            An instance of the appropriate connector class

        Raises:
            ValueError: If the datasource type is not supported
        """
        source_type = ElementDatasourceType(datasource["source_type"])

        # Handle database datasources
        if source_type in (
            ElementDatasourceType.database,
            ElementDatasourceType.database_table,
        ):
            database_info = datasource.get("database_info")
            if database_info is None:
                raise ValueError("Missing required information: 'database_info'")
            database_type = DatabaseType(database_info["database_type"])
            connector_class = cls._DATABASE_CONNECTOR_MAP.get(database_type)
            if connector_class is None:
                raise ValueError(f"Unsupported database type: {database_type}")
            return connector_class()

        # Handle file datasources
        if source_type == ElementDatasourceType.file:
            file_info = datasource.get("file_info")
            if file_info is None:
                raise ValueError("Missing required information: 'file_info'")
            connector_info = file_info.get("connector_info")
            if connector_info is None:
                return FileDatasourceConnector()
            connector_type = connector_info["connector_type"]
            if connector_type == FileConnectorType.s3bucket:
                return S3DatasourceConnector()
            if connector_type == FileConnectorType.googledrive:
                return GoogleDriveDatasourceConnector()
            # Fallback to default file connector for other connector types
            return FileDatasourceConnector()

        # Handle dataframe datasources
        if source_type == ElementDatasourceType.dataframe:
            return DataframeDatasourceConnector()

        # Unsupported datasource type
        raise ValueError(f"Unsupported datasource type: {source_type}")

    @classmethod
    def read(
        cls,
        query: str,
        datasource: ElementDataSource,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
        schema: bool = True,
        use_cache: bool = True,
        query_cache: QueryCache | None = None,
        **kwargs: Any,
    ) -> tuple[pl.DataFrame, dict[str, str] | pl.DataFrame | object, CacheInfo]:
        """
        Execute a query against the datasource and optionally return schema information with caching.

        Args:
            query: SQL query string to execute
            datasource: Datasource configuration
            get_database_details: Callable to retrieve database credentials (required for database datasources)
            schema: If True, return processed schema as dict. If False, return raw schema.
                   Defaults to True.
            use_cache: If True, use query cache if available. Defaults to True.
            query_cache: Optional QueryCache instance for caching results. If None and use_cache=True,
                        caching will be skipped.
            **kwargs: Additional arguments for specific datasource types:
                - duckdb_conn: DuckDB connection (required for file/dataframe datasources)
                - download_file: Function to download file by file_id (required for file datasources)
                - dataframe: DataFrame to register for dataframe datasources (optional)

        Returns:
            Tuple of (DataFrame, schema_dict_or_raw, CacheInfo):
            - DataFrame with query results
            - Schema (processed dict if schema=True, raw format if schema=False)
            - CacheInfo with cache status and ID
        """
        # Initialize cache info
        cache_info = CacheInfo(
            id="",
            status=CacheStatus.MISS,
        )

        # Handle caching if enabled
        if use_cache and query_cache is not None:
            cache_info["id"] = query_cache.generate_cache_id(query, datasource)
            cache_result = query_cache.get(query, datasource)
            if cache_result is not None and not cache_result[0].is_empty():
                cache_info["status"] = CacheStatus.HIT
                # Return cached result with cache info
                return (*cache_result, cache_info)

        # Prepare kwargs for datasource-specific dependencies
        source_type = ElementDatasourceType(datasource["source_type"])

        # Handle dataframe registration if needed
        if source_type == ElementDatasourceType.dataframe:
            dataframe = kwargs.get("dataframe")
            duckdb_conn = kwargs.get("duckdb_conn")
            if dataframe is not None and duckdb_conn is not None:
                duckdb_conn.register(datasource["dataframe_info"]["df_name"], dataframe)

        # If file_path is already provided (file was pre-downloaded), use FileDatasourceConnector
        # directly instead of routing to S3/GDrive connectors that would try to download again
        if kwargs.get("file_path") is not None and source_type == ElementDatasourceType.file:
            connector = FileDatasourceConnector()
        else:
            connector = cls._get_connector(datasource)

        # Execute the query with appropriate parameters
        if source_type in (
            ElementDatasourceType.database,
            ElementDatasourceType.database_table,
        ):
            # Database datasources use get_database_details
            result_df, raw_schema = connector.read(
                query, datasource, get_database_details
            )

            # Process schema if requested
            if schema:
                database_type = DatabaseType(
                    datasource["database_info"]["database_type"]
                )
                processed_schema = cls._process_schema(
                    database_type, connector, raw_schema
                )
                result = (result_df, processed_schema)
            else:
                result = (result_df, raw_schema)
        else:
            # File and dataframe datasources use kwargs (duckdb_conn, etc.)
            result_df, schema_dict = connector.read(
                query, datasource, get_database_details, **kwargs
            )
            # File and dataframe connectors already return processed schema as dict
            result = (result_df, schema_dict)

        # Cache the result if caching is enabled (cache only df and schema, not cache_info)
        if use_cache and query_cache is not None:
            # Cache expects (df, schema) tuple, not (df, schema, cache_info)
            query_cache.set(query, datasource, (result[0], result[1]))

        return (*result, cache_info)

    @classmethod
    def _process_schema(
        cls,
        database_type: DatabaseType,
        connector: BaseDatasourceConnector,
        raw_schema: pl.DataFrame | object,
    ) -> dict[str, str]:
        """
        Process raw schema data into a standardized dict format.

        Args:
            database_type: The type of database (used for validation)
            connector: The connector instance (used to call schema processing methods)
            raw_schema: Raw schema data (format depends on datasource type)

        Returns:
            Dictionary mapping column names to Livedocs types (NUMBER, DATE, STRING)
        """
        # Postgres and Motherduck require DataFrame schema format
        if database_type in (DatabaseType.Postgres, DatabaseType.Motherduck):
            if not isinstance(raw_schema, pl.DataFrame):
                raise ValueError(
                    f"Expected DataFrame for {database_type} schema, got {type(raw_schema)}"
                )

        # All connectors now implement process_schema() method
        return connector.process_schema(raw_schema)

    @classmethod
    def write(
        cls,
        dataframe: pl.DataFrame,
        save_config: DBSaveConfig,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
    ) -> LivedocsResult | None:
        """
        Write a DataFrame to the datasource, checking run_settings first.

        Args:
            dataframe: DataFrame to write
            save_config: Configuration for saving to database
            get_database_details: Callable to retrieve database credentials

        Returns:
            LivedocsResult if write was executed, None if skipped due to run_settings

        Raises:
            ValueError: If database type is not supported
        """
        database_type = DatabaseType(save_config["database_type"])

        # Check run_settings
        current_run_context = get_run_context()
        if current_run_context not in save_config["run_settings"]:
            return None

        # Get connector and execute write
        # Create a minimal datasource for connector lookup
        temp_datasource: ElementDataSource = {
            "source_type": ElementDatasourceType.database,
            "database_info": {
                "database_connector_id": save_config["database_id"],
                "database_name": save_config["database_name"],
                "database_type": database_type,
            },
            "database_table_info": None,
            "dataframe_info": None,
            "file_info": None,
        }
        connector = cls._get_connector(temp_datasource)
        result = connector.write(dataframe, save_config, get_database_details)
        return result
