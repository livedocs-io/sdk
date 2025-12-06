import base64
import gzip
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, cast
from uuid import UUID, uuid5

import polars as pl
import requests
import sentry_sdk
from jinja2 import Template

from livedocs.datasources.googledrive import GoogleDriveDatasourceConnector
from livedocs.datasources.s3 import S3DatasourceConnector
from livedocs.manager.credentials import CredentialStore
from livedocs.manager.datasources import DatasourceManager
from livedocs.manager.duckdb import DuckDBSingleton
from livedocs.types import (
    CacheInfo,
    CacheStatus,
    ChartResult,
    DatabaseConnection,
    DatabaseType,
    DBSaveConfig,
    ElementDataSource,
    ElementDatasourceType,
    FileConnectorType,
    FileNode,
    FileNodeType,
    GCSBucketType,
    GoogleDriveConnectorInfo,
    JsonDisplay,
    LivedocsResult,
    MountHealth,
    MountHealthStatus,
    MsgPackDisplay,
    QueryResult,
    QueryResultMetadata,
    SchemaNodeType,
    Spec,
    VegaSpec,
    WorkspaceSecret,
)
from livedocs.utils.cells.chart_helpers import (
    _LIVEDOCS_PROTECTED_VARS,
    apply_chart_filters,
)
from livedocs.utils.cells.single_value_helpers import process_single_value
from livedocs.utils.cells.table_helpers import apply_table_operations
from livedocs.utils.common import (
    _download_file,
    _setup_dirs,
    debug,
    get_query_for_datasource,
    serializer,
)
from livedocs.utils.lib.cache import QueryCache
from livedocs.utils.lib.internals import (
    livedocs_internal_fetch_file_manifest,
    livedocs_internal_instrument,
    livedocs_internal_list_files,
    livedocs_internal_persist_built_in_vars,
    livedocs_internal_setup_sentry,
)
from livedocs.utils.lib.vega import create_vega_spec


@dataclass(frozen=True)
class LivedocsConfig:
    """Runtime configuration hooks that make Livedocs more testable and tunable."""

    credential_store_factory: Callable[[str, str], CredentialStore] = CredentialStore
    query_cache_factory: Callable[[str, str], QueryCache] = QueryCache
    template_cache_size: int = 256


class Livedocs:
    """
    Main class for the Livedocs library. Handles initialization, querying, and data processing.
    This is initialized in the prelude cell of the notebook like this:

    livedocs = Livedocs()
    livedocs.initialize(report_id, session_token)
    """

    def __init__(self, config: LivedocsConfig | None = None):
        """
        Creates the Livedocs instance, setting up necessary components and configurations.
        """
        livedocs_internal_setup_sentry()
        _setup_dirs()

        self._config: LivedocsConfig = config or LivedocsConfig()

        files_path = os.getenv("LIVEDOCS_FILES_PATH")
        self._duckdb: DuckDBSingleton = DuckDBSingleton(
            file_search_path=[files_path] if files_path is not None else []
        )
        self._report_id: str | None = None
        self._token: str | None = None
        self._credential_store: CredentialStore | None = None
        self._secrets: dict[str, WorkspaceSecret] = {}
        self._built_in_vars: dict[str, str] = {}

        self._template_factory: Callable[[str], Template] = lru_cache(
            maxsize=self._config.template_cache_size
        )(Template)
        self._query_cache: QueryCache | None = None
        self.is_initialized: bool = False

    def initialize(
        self, report_id: str, token: str, client_id_token: str | None = None
    ) -> None:
        """
        Initializes the Livedocs instance with the given report ID and token.
        Called when the pod is initialized. If an optional client ID token is provided,
        it will be used to initialize the SDK without needing to fetch anything from the backend.

        Args:
            report_id (str): The report ID.
            token (str): The session token.
            client_id_token (str, optional): The client ID token.
        """
        with sentry_sdk.start_transaction(op="task", name="initialize sdk"):
            if not client_id_token:
                sentry_sdk.set_tag("report_id", report_id)
                self._report_id = report_id
                self._token = token
                span = sentry_sdk.start_span(name="fetch credentials")
                self._credential_store = self._config.credential_store_factory(
                    report_id, token
                )
                bundle = self._credential_store.load()
                _ = span.finish()
                self.is_initialized = True

                self._secrets = {
                    key: secret for key, secret in bundle.workspace_secrets.items()
                }
                self._built_in_vars = {**bundle.built_in_vars}
                self._query_cache = self._config.query_cache_factory(report_id, token)
            else:
                self.is_initialized = True

    """
    #########################################################
    # PYTHON CELL HELPER FUNCTIONS
    #########################################################
    """

    @livedocs_internal_instrument
    def set_var(self, key: str, value: str):
        """
        Sets a built-in variable.

        Args:
            key (str): The variable key.
            value (str): The variable value.
        """
        self._built_in_vars[key] = value
        _ = livedocs_internal_persist_built_in_vars(
            self._report_id, self._token, self._built_in_vars
        )

    @livedocs_internal_instrument
    def get_var(self, key: str) -> str | None:
        """
        Gets the value of a built-in variable.

        Args:
            key (str): The variable key.

        Returns:
            str: The variable value.
        """
        if key == "run_context":
            return os.getenv("LIVEDOCS_RUN_CONTEXT")

        return self._built_in_vars.get(key, None)

    @livedocs_internal_instrument
    def unset_var(self, key: str):
        """
        Unsets a built-in variable.

        Args:
            key (str): The variable key.
        """
        if key not in _LIVEDOCS_PROTECTED_VARS:
            _ = self._built_in_vars.pop(key, None)
            _ = livedocs_internal_persist_built_in_vars(
                self._report_id, self._token, self._built_in_vars
            )

    @livedocs_internal_instrument
    def clear_vars(self):
        """
        Clears all built-in variables.
        """
        protected_values = {
            k: v
            for k, v in self._built_in_vars.items()
            if k in _LIVEDOCS_PROTECTED_VARS
        }
        self._built_in_vars = protected_values
        _ = livedocs_internal_persist_built_in_vars(
            self._report_id, self._token, self._built_in_vars
        )

    @livedocs_internal_instrument
    def secrets(self, key: str, default_value: str = "") -> str:
        """
        Access user-defined secrets with default value if not found.

        Args:
            key: The key of the secret to access
            default_value: Value to return if the secret is not found

        Returns:
            The secret value or default value if not found
        """
        if not hasattr(self, "_secrets"):
            return default_value

        if key in self._secrets:
            return self._secrets[key].value.get_secret_value()

        try:
            store = self.helper_get_initialized_credentials()
        except RuntimeError:
            return default_value

        secret_model = store.get_secret(key)
        if secret_model is None:
            secret_model = store.refresh().workspace_secrets.get(key)
        if secret_model:
            self._secrets[key] = secret_model
            return secret_model.value.get_secret_value()

        return default_value

    @livedocs_internal_instrument
    def download_file(
        self,
        file_name: str | None = None,
        file_id: str | None = None,
        force_download: bool = False,
        path: str | None = None,
    ) -> str:
        """
        Downloads a file to a local path based on either its name or ID.

        Parameters:
            file_name (str | None): The name of the file to download. Must be provided exclusively if file_id is not specified.
            file_id (str | None): The unique identifier of the file to download. Must be provided exclusively if file_name is not specified.
            force_download (bool): If True, forces the file to be redownloaded and overwritten if it exists locally.
            path (str | None): The directory path where the file will be stored.
                                Defaults to the value of the environment variable 'LIVEDOCS_FILES_PATH'.

        Returns:
            str: The local file system path where the downloaded file is stored.

        Raises:
            RuntimeError: If the system is not initialized, or if an unexpected error occurs during the manifest retrieval or download process.
            ValueError: If neither or both of 'file_name' and 'file_id' are provided, or if multiple files with the same name are found.
            FileNotFoundError: If the file with the specified 'file_name' or 'file_id' does not exist on the remote server.
        """
        if not self.is_initialized:
            raise RuntimeError("Livedocs is not initialized. Call initialize() first.")

        if path is None:
            path = os.getenv("LIVEDOCS_FILES_PATH")

        if not (file_name or file_id) or (file_name and file_id):
            raise ValueError("Exactly one of file_name or file_id must be provided.")

        if path is None:
            raise ValueError("Please provide a valid path to save the file.")

        os.makedirs(path, exist_ok=True)

        manifest_data = livedocs_internal_fetch_file_manifest(
            report_id=self._report_id,
            token=self._token,
            action="read",
            bucket=GCSBucketType.USER_FILES,
            file_id=file_id,
            file_name=file_name,
        )

        authoritative_file_name = manifest_data.file_name
        local_file_path = os.path.join(path, authoritative_file_name)
        file_exists = os.path.exists(local_file_path)

        if not force_download and file_exists:
            print(
                f"File '{authoritative_file_name}' (ID: {manifest_data.file_id}) already exists locally at '{local_file_path}'. \nUse option force_download=True to overwrite."
            )
            return local_file_path

        if force_download and file_exists:
            print(
                f"File '{authoritative_file_name}' already exists locally at '{local_file_path}'. Overwriting."
            )
            os.remove(local_file_path)

        signed_url = manifest_data.signed_url
        expected_size_bytes = manifest_data.size if manifest_data.size else None

        _download_file(
            signed_url,
            local_file_path,
            file_description=authoritative_file_name,
            expected_size_bytes=expected_size_bytes,
        )

        return local_file_path

    """
    #########################################################
    # PRIVATE HELPER FUNCTIONS
    #########################################################
    """

    @livedocs_internal_instrument
    @sentry_sdk.trace
    def query(
        self,
        query: str,
        str_datasource: str,
        context: dict,
        dataframe=None,
        limit=10,
        offset=0,
        use_cache=True,
        table_metadata=None,
    ) -> tuple[pl.DataFrame, str]:
        """
        Executes a query on a given datasource and returns the result as a Polars DataFrame and JSON string.

        Args:
            query (str): The query string.
            str_datasource (str): The datasource as a JSON string.
            context (dict): The context for Jinja variables.
            dataframe (optional): A DataFrame used if the datasource type is 'dataframe'. Defaults to None.
            limit (int, optional): The number of rows to return. Defaults to 10.
            offset (int, optional): The offset for the rows to return. Defaults to 0.
            use_cache (bool, optional): Indicates whether to use caching. Defaults to True.
            table_metadata (dict, optional): Metadata for table operations. Defaults to None.
        Returns:
            tuple[pl.DataFrame, str]: A tuple containing the resulting DataFrame and JSON string.
        """
        with sentry_sdk.start_transaction(op="task", name="run query"):
            datasource: ElementDataSource = json.loads(str_datasource)
            if not datasource:
                raise ValueError(
                    "No datasource selected. Please choose a datasource from the dropdown before running your query."
                )

            # Plug in the Jinja variables
            final_query = self.helper_render_jinja_template(query, context)

            # Run the actual queries
            query_span = sentry_sdk.start_span(name="run query")
            df: pl.DataFrame = pl.DataFrame()

            # Prepare kwargs for DatasourceManager
            source_type = ElementDatasourceType(datasource["source_type"])
            kwargs: dict[str, Any] = {}
            if source_type == ElementDatasourceType.file:
                kwargs["duckdb_conn"] = self._duckdb.conn
                kwargs["download_file"] = self.download_file
            elif source_type == ElementDatasourceType.dataframe:
                kwargs["duckdb_conn"] = self._duckdb.conn
                kwargs["dataframe"] = dataframe

            df, schema, cache_info = DatasourceManager.read(
                final_query,
                datasource,
                self.helper_get_database_details,
                schema=True,
                use_cache=use_cache,
                query_cache=self._query_cache,
                **kwargs,
            )
            query_span.finish()

            # Apply table operations
            applied_metadata = None
            additional_metadata = {}
            if table_metadata:
                df, additional_metadata = apply_table_operations(df, table_metadata)
                applied_metadata = table_metadata

            # Prepare paginated results
            post_span = sentry_sdk.start_span(name="post-processing")
            df_slice = df.slice(offset, limit)

            # Compress and encode response
            result = QueryResult(
                data=df_slice,
                metadata=QueryResultMetadata(
                    limit=limit,
                    offset=offset,
                    total_rows=len(df),
                    cache_info=cache_info,
                    applied_metadata=applied_metadata,
                    calculation_results=additional_metadata.get("calculation_results"),
                ),
            )
            payload = LivedocsResult(result)
            post_span.finish()

            return (df, payload)

    @livedocs_internal_instrument
    @sentry_sdk.trace
    def save_to_database(self, dataframe: pl.DataFrame, str_save_config: str):
        with sentry_sdk.start_transaction(op="task", name="save to database"):
            save_config: DBSaveConfig = json.loads(str_save_config)
            result = DatasourceManager.write(
                dataframe, save_config, self.helper_get_database_details
            )
            return result

    @livedocs_internal_instrument
    @sentry_sdk.trace
    def process_raw_text(self, str_src: str, context: dict) -> str:
        """
        Processes raw text by plugging in Jinja variables.

        Args:
            str_src (str): The source text as a JSON string.
            context (dict): The context for Jinja variables.

        Returns:
            str: The processed HTML text.
        """
        with sentry_sdk.start_transaction(op="task", name="run text element"):
            src = json.loads(str_src)
            return self.helper_render_jinja_template(src["html"], context)

    @livedocs_internal_instrument
    def enrich_prompt(self, system, user, context: dict):
        enriched_prompt = {
            "system": self.helper_render_jinja_template(system, context),
            "user": self.helper_render_jinja_template(user, context),
        }
        return MsgPackDisplay(enriched_prompt)

    def process_dependencies(
        self, dependencies: str, datasource: dict = None, globals_dict: dict = None
    ) -> dict:
        """
        Process dependencies and serialize DataFrames to dictionaries.

        Args:
            dependencies (dict): Dictionary of dependencies
            datasource (dict, optional): Datasource configuration
            globals_dict (dict, optional): Global variables dictionary

        Returns:
            dict: Context dictionary with processed dependencies
        """
        deps = json.loads(dependencies)
        ctx = {}

        # Use provided globals or fall back to globals()
        global_vars = globals_dict if globals_dict is not None else globals()

        for dep_name, dep_info in deps.items():
            if (
                not datasource
                or ElementDatasourceType(datasource.get("source_type"))
                == ElementDatasourceType.dataframe
                or dep_info.get("field_type") == ""
            ):
                try:
                    # Get the value from globals
                    value = global_vars.get(dep_name)
                    if value is None:
                        raise NameError(f"name '{dep_name}' is not defined")

                    # Handle DataFrame serialization
                    if isinstance(value, pl.DataFrame):
                        df = value.to_dicts()
                        ctx[dep_name] = json.dumps(
                            df, default=serializer, separators=(",", ":")
                        )
                    else:
                        ctx[dep_name] = value

                except Exception as e:
                    raise Exception(
                        f"Unable to find {dep_name}, ensure the element where you declared it "
                        "has been run at least once"
                    ) from e
        return ctx

    @livedocs_internal_instrument
    @sentry_sdk.trace
    def _get_vega_spec(
        self,
        settings_str: str,
        datasource_str: str,
        dataframe=None,
        use_cache=True,
        chart_metadata=None,
    ) -> dict:
        """
        Gets a Vega specification for a given datasource and settings.

        Args:
            settings_str (str): The settings as a JSON string.
            datasource_str (str): The datasource as a JSON string.
            dataframe (optional): A DataFrame used if the datasource type is 'dataframe'. Defaults to None.

        Returns:
            dict: The Vega specification as a base64 encoded string.
        """
        with sentry_sdk.start_transaction(op="task", name="run chart element"):
            settings: Spec = json.loads(settings_str)
            datasource: ElementDataSource = json.loads(datasource_str)

            # Run actual span
            query_span = sentry_sdk.start_span(name="run query")
            query = get_query_for_datasource(datasource, 50000)

            if (
                datasource["source_type"] == "database_table"
                and DatabaseType(datasource["database_info"]["database_type"])
                == DatabaseType.Snowflake
            ):
                try:
                    db_connector_id = datasource["database_info"][
                        "database_connector_id"
                    ]
                    _, parsed_credentials = self.helper_get_database_details(
                        db_connector_id
                    )
                except KeyError as e:
                    raise ValueError(f"Missing required information: {e}")

                query = f'SELECT * FROM "{parsed_credentials["database"]}"."{datasource["database_table_info"]["schema_name"]}"."{datasource["database_table_info"]["table_name"]}" LIMIT 500000;'

            # Prepare kwargs for DatasourceManager
            source_type = ElementDatasourceType(datasource["source_type"])
            kwargs: dict[str, Any] = {}
            if source_type == ElementDatasourceType.file:
                kwargs["duckdb_conn"] = self._duckdb.conn
                kwargs["download_file"] = self.download_file
            elif source_type == ElementDatasourceType.dataframe:
                kwargs["duckdb_conn"] = self._duckdb.conn
                kwargs["dataframe"] = dataframe

            df, schema, cache_info = DatasourceManager.read(
                query,
                datasource,
                self.helper_get_database_details,
                schema=True,
                use_cache=use_cache,
                query_cache=self._query_cache,
                **kwargs,
            )
            query_span.finish()

            filter_span = sentry_sdk.start_span(name="run apply_chart_filters")
            df = apply_chart_filters(df, schema, settings, chart_metadata)
            filter_span.finish()

            # Find how many data points we have
            total_data_points = len(df) * len(df.columns)
            debug(f"Total data points: {total_data_points}")

            vega_span = sentry_sdk.start_span(name="run create_vega_spec (vegafusion)")
            MAX_POINTS = 50000
            if total_data_points > MAX_POINTS:
                # Limit the dataframe so that rendered points stay within the cap
                num_cols = max(1, len(df.columns))
                max_rows = max(1, MAX_POINTS // num_cols)
                try:
                    df_limited = df.slice(0, max_rows)
                    # Build a normal spec from the limited data
                    limited_spec_json = create_vega_spec(df_limited, settings, schema)
                    # Change status to signal warning while still rendering the chart
                    limited_spec = json.loads(limited_spec_json)
                    limited_spec["status"] = "OVERLOADED"
                    validated_spec = VegaSpec(**limited_spec)
                    vega_spec_json_str = validated_spec.model_dump_json()
                except Exception:
                    style_settings = settings.get("styleSettings", {})
                    empty_chart = {
                        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                        "usermeta": {
                            "styleSettings": style_settings,
                            "chartType": "main",
                        },
                    }
                    validated_spec = VegaSpec(
                        **{
                            "spec": json.dumps(empty_chart, separators=(",", ":")),
                            "schema": schema,
                            "status": "OVERLOADED",
                        }
                    )
                    vega_spec_json_str = validated_spec.model_dump_json()
            else:
                vega_spec_json_str = create_vega_spec(df, settings, schema)
            vega_span.finish()

            # Post-process the results
            post_span = sentry_sdk.start_span(name="post-processing")
            compressed = gzip.compress(vega_spec_json_str.encode("utf-8"))
            encoded = base64.b64encode(compressed).decode("ascii")

            result = ChartResult(data=encoded, cache_info=cache_info)
            payload = LivedocsResult(result)
            post_span.finish()

            return payload

    @livedocs_internal_instrument
    @sentry_sdk.trace
    def _get_table_response(
        self,
        str_datasource: ElementDataSource,
        dataframe=None,
        limit=10,
        offset=0,
        use_cache=True,
        table_metadata=None,
    ) -> pl.DataFrame:
        """
        Gets a Polars table for a given datasource.

        Args:
            str_datasource (ElementDataSource): The datasource as a JSON string.
            dataframe (optional): A DataFrame used if the datasource type is 'dataframe'. Defaults to None.
            limit (int, optional): The number of rows to return. Defaults to 10.
            offset (int, optional): The offset for the rows to return. Defaults to 0.
            use_cache (bool, optional): Indicates whether to use caching. Defaults to True.
            table_metadata (dict, optional): Metadata for table operations. Defaults to None.

        Returns:
            pl.DataFrame: The resulting Polars DataFrame.
        """
        with sentry_sdk.start_transaction(op="task", name="run table element"):
            datasource: ElementDataSource = json.loads(str_datasource)

            query = get_query_for_datasource(datasource, None)
            if (
                datasource["source_type"] == "database_table"
                and DatabaseType(datasource["database_info"]["database_type"])
                == DatabaseType.Snowflake
            ):
                try:
                    db_connector_id = datasource["database_info"][
                        "database_connector_id"
                    ]
                    _, parsed_credentials = self.helper_get_database_details(
                        db_connector_id
                    )
                except KeyError as e:
                    raise ValueError(f"Missing required information: {e}")

                query = f'SELECT * FROM "{parsed_credentials["database"]}"."{datasource["database_table_info"]["schema_name"]}"."{datasource["database_table_info"]["table_name"]}"'

            query_span = sentry_sdk.start_span(name="run query")
            # Prepare kwargs for DatasourceManager
            source_type = ElementDatasourceType(datasource["source_type"])
            kwargs: dict[str, Any] = {}
            if source_type == ElementDatasourceType.file:
                kwargs["duckdb_conn"] = self._duckdb.conn
                kwargs["download_file"] = self.download_file
            elif source_type == ElementDatasourceType.dataframe:
                kwargs["duckdb_conn"] = self._duckdb.conn
                kwargs["dataframe"] = dataframe

            df, schema, cache_info = DatasourceManager.read(
                query,
                datasource,
                self.helper_get_database_details,
                schema=True,
                use_cache=use_cache,
                query_cache=self._query_cache,
                **kwargs,
            )
            query_span.finish()

            # Apply table operations
            applied_metadata = None
            additional_metadata = {}
            if table_metadata:
                df, additional_metadata = apply_table_operations(df, table_metadata)
                applied_metadata = table_metadata

            # Prepare paginated results
            post_span = sentry_sdk.start_span(name="post-processing")
            df_slice = df.slice(offset, limit)
            result = QueryResult(
                data=df_slice,
                metadata=QueryResultMetadata(
                    limit=limit,
                    offset=offset,
                    total_rows=len(df),
                    cache_info=cache_info,
                    applied_metadata=applied_metadata,
                    calculation_results=additional_metadata.get("calculation_results"),
                ),
            )
            payload = LivedocsResult(result)
            post_span.finish()
            return payload

    # DOOMED: when web-client hits relay server in standalone mode
    @livedocs_internal_instrument
    @sentry_sdk.trace
    def _get_chart_schema(
        self, datasource_str: str, dataframe: pl.DataFrame = None
    ) -> dict:
        """
        Returns a dictionary with the schema for a given datasource.

        Args:
            datasource_str (str): The datasource as a JSON string.
            dataframe (pl.DataFrame, optional): A DataFrame used if the datasource type is 'dataframe'. Defaults to None.

        Returns:
            dict: The schema as a base64 encoded string.
        """
        with sentry_sdk.start_transaction(op="task", name="get schema for chart"):
            datasource: ElementDataSource = json.loads(datasource_str)

            query_span = sentry_sdk.start_span(name="run query")
            query = get_query_for_datasource(datasource, None)
            if query is None:
                raise ValueError("Query is required")

            # Prepare kwargs for DatasourceManager based on datasource type
            source_type = ElementDatasourceType(datasource["source_type"])
            kwargs: dict[str, Any] = {}

            if source_type == ElementDatasourceType.file:
                kwargs["duckdb_conn"] = self._duckdb.conn
                kwargs["download_file"] = self.download_file
            elif source_type == ElementDatasourceType.dataframe:
                kwargs["duckdb_conn"] = self._duckdb.conn
                if dataframe is not None and datasource is not None:
                    self._duckdb.conn.register(
                        datasource["dataframe_info"]["df_name"], dataframe
                    )

            # Execute query using DatasourceManager (no caching for schema-only queries)
            _, schema, _ = DatasourceManager.read(
                query,
                datasource,
                self.helper_get_database_details,
                schema=True,
                use_cache=False,
                query_cache=None,
                **kwargs,
            )
            query_span.finish()

            post_span = sentry_sdk.start_span(name="post-processing")
            empty_chart = {
                "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
                "usermeta": {
                    "styleSettings": {},
                    "chartType": "main",
                },
            }

            empty_spec_with_schema = json.dumps(
                {
                    "spec": json.dumps(empty_chart, separators=(",", ":")),
                    "schema": schema,
                    "status": "EMPTY",
                },
                separators=(",", ":"),
            )
            compressed = gzip.compress(empty_spec_with_schema.encode("utf-8"))
            encoded = base64.b64encode(compressed).decode("ascii")

            # Create empty cache info for schema requests
            empty_cache_info = CacheInfo(id="", status=CacheStatus.MISS)
            result = ChartResult(data=encoded, cache_info=empty_cache_info)
            payload = LivedocsResult(result)
            _ = post_span.finish()

            return payload

    @livedocs_internal_instrument
    def process_single_value(self, config: str, context: dict = None) -> dict:
        """
        Process a SingleValue element with formatting and comparison calculations

        Args:
            config (str): JSON string containing single value configuration
            context (dict, optional): Context containing variables. Defaults to None.

        Returns:
            JsonDisplay: Formatted result with main value and comparison data
        """
        result = process_single_value(config, context)
        return JsonDisplay(result)

    @livedocs_internal_instrument
    def list_nodes(
        self,
        path: str | None = None,
        connector_type: FileConnectorType | None = None,
        connector_id: str | None = None,
        warehouse_node_id: str | None = None,
        warehouse_node_type: SchemaNodeType | None = None,
        search_string: str | None = None,
        refresh_google_drive_token: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
    ):
        if not self._report_id or not self._token:
            raise ValueError(
                "Livedocs is not initialized with report_id and token. Call initialize() with report_id and token first."
            )

        warehouses_and_files = livedocs_internal_list_files(
            self._report_id,
            self._token,
            warehouse_node_id,
            warehouse_node_type,
            search_string,
        )

        s3_nodes = []
        google_drive_file_nodes = []
        runtime_file_nodes = []

        if not connector_id:
            # return a filenode for each credential of type s3bucket and googledrive
            if self._credential_store:
                # Add S3 connectors to s3_nodes
                for connector_info in self._credential_store.get_all_s3_connectors():
                    now = datetime.now(timezone.utc)
                    s3_nodes.append(
                        FileNode(
                            id=UUID(connector_info["connector_id"]),
                            name=connector_info["name"],
                            type=FileNodeType.directory,
                            mount_type=FileConnectorType.s3bucket,
                            connector_id=UUID(connector_info["connector_id"]),
                            path=connector_info.get("path_prefix", ""),
                            parent_id=None,
                            size=None,
                            mime_type=None,
                            modified_at=None,
                            created_at=None,
                            health=MountHealth(
                                status=MountHealthStatus.connected,
                                last_checked=now,
                                error_message=None,
                            ),
                        )
                    )
                # Add Google Drive connectors to google_drive_file_nodes
                for (
                    connector_info
                ) in self._credential_store.get_all_google_drive_connectors():
                    now = datetime.now(timezone.utc)
                    google_drive_file_nodes.append(
                        FileNode(
                            id=UUID(connector_info["connector_id"]),
                            name=connector_info["name"],
                            type=FileNodeType.directory,
                            mount_type=FileConnectorType.googledrive,
                            connector_id=UUID(connector_info["connector_id"]),
                            path="",  # Google Drive root
                            parent_id=None,
                            size=None,
                            mime_type=None,
                            modified_at=None,
                            created_at=None,
                            health=MountHealth(
                                status=MountHealthStatus.connected,
                                last_checked=now,
                                error_message=None,
                            ),
                        )
                    )
        else:
            # Determine connector type and list files
            if not self._credential_store:
                raise ValueError("Credential store not initialized")

            if connector_type == FileConnectorType.s3bucket:
                # Try S3 first
                connector_info = self._credential_store.get_s3_connector(connector_id)
                if connector_info:
                    s3_connector = S3DatasourceConnector()
                    s3_nodes = s3_connector.list(
                        path=path,
                        connector_id=connector_id,
                        get_connection_details=self.helper_get_s3_connection_details,
                    )
                else:
                    s3_nodes = []
            elif connector_type == FileConnectorType.googledrive:
                # Try Google Drive
                connector_info = self._credential_store.get_google_drive_connector(
                    connector_id
                )
                if not connector_info:
                    raise ValueError(
                        f"Connector '{connector_id}' not found (checked S3 and Google Drive)"
                    )

                google_drive_connector = GoogleDriveDatasourceConnector()
                refresh_callback = (
                    refresh_google_drive_token
                    if refresh_google_drive_token
                    else self.refresh_google_drive_token
                )
                google_drive_file_nodes = google_drive_connector.list(
                    path=path,
                    connector_id=connector_id,
                    get_connection_details=self.helper_get_google_drive_connection_details,
                    refresh_token_callback=refresh_callback,
                )
            elif connector_type == FileConnectorType.runtime:
                # Runtime connector - list files/folders from local filesystem
                # Use provided path or root (current directory)
                list_path = path if path else "."
                list_path_obj = Path(list_path)

                # Check if path exists
                if not list_path_obj.exists():
                    raise ValueError(f"Path not found: {list_path}")

                # Ensure it's a directory
                if not list_path_obj.is_dir():
                    raise ValueError(f"Path is not a directory: {list_path}")

                now = datetime.now(timezone.utc)
                nodes: list[FileNode] = []

                # Helper function to generate deterministic UUID
                def _generate_file_id(connector_id: str, file_path: str) -> UUID:
                    namespace = UUID(connector_id)
                    return uuid5(namespace, file_path)

                # Helper function to get parent path
                def _get_parent_path(file_path: str) -> str | None:
                    """Extract parent directory path."""
                    file_path = file_path.rstrip("/")
                    if not file_path or file_path == "/":
                        return None
                    parent = "/".join(file_path.split("/")[:-1])
                    return parent if parent else None

                # List all items in the directory
                try:
                    items = list_path_obj.iterdir()
                    for item in items:
                        # Determine if it's a directory or file
                        is_directory = item.is_dir()
                        name = item.name

                        # Calculate relative path: if path was provided, prepend it to the item name
                        if path:
                            # Normalize the base path
                            base_path = path.strip("/")
                            relative_path = f"{base_path}/{name}" if base_path else name
                        else:
                            # If no path provided, use just the name (root level)
                            relative_path = name

                        # Normalize path separators
                        relative_path = relative_path.replace("\\", "/")

                        # Generate IDs
                        file_id = _generate_file_id(connector_id, relative_path)
                        parent_path = _get_parent_path(relative_path)
                        parent_id = (
                            _generate_file_id(connector_id, parent_path)
                            if parent_path
                            else None
                        )

                        # Get file metadata
                        size = None
                        modified_at = None
                        created_at = None

                        if item.is_file():
                            try:
                                stat = item.stat()
                                size = stat.st_size
                                modified_at = datetime.fromtimestamp(
                                    stat.st_mtime, tz=timezone.utc
                                )
                                created_at = datetime.fromtimestamp(
                                    stat.st_ctime, tz=timezone.utc
                                )
                            except (OSError, ValueError):
                                pass
                        elif item.is_dir():
                            try:
                                stat = item.stat()
                                modified_at = datetime.fromtimestamp(
                                    stat.st_mtime, tz=timezone.utc
                                )
                                created_at = datetime.fromtimestamp(
                                    stat.st_ctime, tz=timezone.utc
                                )
                            except (OSError, ValueError):
                                pass

                        nodes.append(
                            FileNode(
                                id=file_id,
                                name=name,
                                type=FileNodeType.directory
                                if is_directory
                                else FileNodeType.file,
                                mount_type=FileConnectorType.runtime,
                                connector_id=UUID(connector_id),
                                path=relative_path,
                                parent_id=parent_id,
                                size=size,
                                mime_type=None,
                                modified_at=modified_at,
                                created_at=created_at,
                                health=MountHealth(
                                    status=MountHealthStatus.connected,
                                    last_checked=now,
                                    error_message=None,
                                ),
                            )
                        )
                except (OSError, PermissionError) as e:
                    raise ValueError(f"Error listing directory '{list_path}': {str(e)}")

                runtime_file_nodes = nodes

        return {
            "s3buckets": s3_nodes,
            "googledrive": google_drive_file_nodes,
            "runtime": runtime_file_nodes,
            "databases": warehouses_and_files.schema_nodes,
            "workspace_files": warehouses_and_files.files,
        }

    @livedocs_internal_instrument
    def get_file(
        self,
        connector_type: FileConnectorType,
        file_id: str | None = None,
        path: str | None = None,
        connector_id: str | None = None,
        refresh_google_drive_token: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
    ):
        """
        Get a file from the specified connector.

        If file_id is provided, downloads the file using download_file() and returns the local path.
        Otherwise, returns a download URL (signed URL for S3, download link for Google Drive).

        Args:
            connector_type: Type of file connector (runtime, s3bucket, or googledrive)
            file_id: File ID (if provided, file will be downloaded and path returned, ignoring other params)
            path: Path to the file (required when file_id is not provided)
            connector_id: Connector ID (required for s3bucket and googledrive when getting URLs)

        Returns:
            Local file path (if file_id provided) or download URL (if path provided)
        """
        # If file_id is provided, use download_file and return the path (ignore everything else)
        if file_id is not None:
            return self.download_file(file_id=file_id)

        # If no file_id, return download URL/link based on connector_type
        if connector_type == FileConnectorType.runtime:
            # For runtime, file is already local - just return the path
            if not path:
                raise ValueError("path is required for runtime connector type")

            if not os.path.exists(path):
                raise ValueError(f"File not found: {path}")
            return path

        # For S3 and Google Drive, we need connector_id and path
        if not connector_id:
            raise ValueError(
                "connector_id is required for S3 and Google Drive operations"
            )
        if not path:
            raise ValueError("path is required when file_id is not provided")
        if not self._credential_store:
            raise ValueError("Credential store not initialized")

        if connector_type == FileConnectorType.s3bucket:
            s3_connector = S3DatasourceConnector()
            signed_url = s3_connector.get_signed_url(
                file_path=path,
                connector_type=connector_type,
                connector_id=connector_id,
                get_connection_details=self.helper_get_s3_connection_details,
            )
            if signed_url is None:
                raise ValueError(f"Failed to generate signed URL for path: {path}")
            return signed_url

        elif connector_type == FileConnectorType.googledrive:
            google_drive_connector = GoogleDriveDatasourceConnector()
            refresh_callback = (
                refresh_google_drive_token
                if refresh_google_drive_token
                else self.refresh_google_drive_token
            )
            download_url = google_drive_connector.get_signed_url(
                file_path=path,
                connector_type=connector_type,
                connector_id=connector_id,
                get_connection_details=self.helper_get_google_drive_connection_details,
                refresh_token_callback=refresh_callback,
            )
            if download_url is None:
                raise ValueError(f"Failed to get download URL for path: {path}")
            return download_url

        else:
            raise ValueError(f"Unsupported connector type: {connector_type}")

    @livedocs_internal_instrument
    def save_file(
        self,
        file_path: str,
        connector_type: FileConnectorType,
        connector_id: str | None = None,
        destination_path: str | None = None,
        refresh_google_drive_token: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
    ):
        """
        Upload a local file to the specified connector (S3 or Google Drive).

        Args:
            file_path: Local file path to upload (must exist on filesystem)
            connector_type: Type of file connector (s3bucket or googledrive)
            connector_id: Connector ID (required)
            destination_path: Destination path in the connector. For S3, relative to path_prefix.
                              For Google Drive, folder path. If None, uses filename and uploads to root.

        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(file_path):
            raise ValueError(f"Local file not found: {file_path}")

        if connector_type == FileConnectorType.s3bucket:
            if not connector_id:
                raise ValueError("connector_id is required for S3 operations")
            if not self._credential_store:
                raise ValueError("Credential store not initialized")

            s3_connector = S3DatasourceConnector()
            return s3_connector.save_file(
                file_path=file_path,
                connector_type=connector_type,
                connector_id=connector_id,
                s3_path=destination_path,
                get_connection_details=self.helper_get_s3_connection_details,
            )

        elif connector_type == FileConnectorType.googledrive:
            if not connector_id:
                raise ValueError("connector_id is required for Google Drive operations")
            if not self._credential_store:
                raise ValueError("Credential store not initialized")

            google_drive_connector = GoogleDriveDatasourceConnector()
            refresh_callback = (
                refresh_google_drive_token
                if refresh_google_drive_token
                else self.refresh_google_drive_token
            )
            return google_drive_connector.save_file(
                file_path=file_path,
                connector_type=connector_type,
                connector_id=connector_id,
                drive_path=destination_path,
                get_connection_details=self.helper_get_google_drive_connection_details,
                refresh_token_callback=refresh_callback,
            )

        else:
            raise ValueError(
                f"Unsupported connector type: {connector_type}. Only s3bucket and googledrive are supported for save_file."
            )

    @livedocs_internal_instrument
    def delete_file(
        self,
        file_path: str,
        connector_type: FileConnectorType,
        connector_id: str | None = None,
        refresh_google_drive_token: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
    ):
        """
        Delete a file from the specified connector.

        Args:
            file_path: Path to the file to delete
            connector_type: Type of file connector (s3bucket, googledrive, or runtime)
            connector_id: Connector ID (required for s3bucket and googledrive)

        Returns:
            True if successful, False otherwise
        """
        if connector_type == FileConnectorType.runtime:
            # Local file system operation
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    return True
                return False
            except Exception:
                return False

        elif connector_type == FileConnectorType.s3bucket:
            if not connector_id:
                raise ValueError("connector_id is required for S3 operations")
            if not self._credential_store:
                raise ValueError("Credential store not initialized")

            s3_connector = S3DatasourceConnector()
            return s3_connector.delete_file(
                file_path=file_path,
                connector_type=connector_type,
                connector_id=connector_id,
                get_connection_details=self.helper_get_s3_connection_details,
            )

        elif connector_type == FileConnectorType.googledrive:
            if not connector_id:
                raise ValueError("connector_id is required for Google Drive operations")
            if not self._credential_store:
                raise ValueError("Credential store not initialized")

            google_drive_connector = GoogleDriveDatasourceConnector()
            refresh_callback = (
                refresh_google_drive_token
                if refresh_google_drive_token
                else self.refresh_google_drive_token
            )
            return google_drive_connector.delete_file(
                file_path=file_path,
                connector_type=connector_type,
                connector_id=connector_id,
                get_connection_details=self.helper_get_google_drive_connection_details,
                refresh_token_callback=refresh_callback,
            )

        else:
            raise ValueError(f"Unsupported connector type: {connector_type}")

    @livedocs_internal_instrument
    def rename_file(
        self,
        file_path: str,
        new_name: str,
        connector_type: FileConnectorType,
        connector_id: str | None = None,
        refresh_google_drive_token: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
    ):
        """
        Rename a file in the specified connector.

        Args:
            file_path: Current path to the file
            new_name: New name for the file (just the filename, not full path)
            connector_type: Type of file connector (s3bucket, googledrive, or runtime)
            connector_id: Connector ID (required for s3bucket and googledrive)

        Returns:
            True if successful, False otherwise
        """
        if connector_type == FileConnectorType.runtime:
            # Local file system operation
            try:
                # Construct new path by replacing the filename
                parent_path = os.path.dirname(file_path)
                if parent_path:
                    new_path = os.path.join(parent_path, new_name)
                else:
                    new_path = new_name

                if os.path.exists(file_path):
                    os.rename(file_path, new_path)
                    return True
                return False
            except Exception:
                return False

        elif connector_type == FileConnectorType.s3bucket:
            if not connector_id:
                raise ValueError("connector_id is required for S3 operations")
            if not self._credential_store:
                raise ValueError("Credential store not initialized")

            s3_connector = S3DatasourceConnector()
            return s3_connector.rename_file(
                file_path=file_path,
                new_name=new_name,
                connector_type=connector_type,
                connector_id=connector_id,
                get_connection_details=self.helper_get_s3_connection_details,
            )

        elif connector_type == FileConnectorType.googledrive:
            if not connector_id:
                raise ValueError("connector_id is required for Google Drive operations")
            if not self._credential_store:
                raise ValueError("Credential store not initialized")

            google_drive_connector = GoogleDriveDatasourceConnector()
            refresh_callback = (
                refresh_google_drive_token
                if refresh_google_drive_token
                else self.refresh_google_drive_token
            )
            return google_drive_connector.rename_file(
                file_path=file_path,
                new_name=new_name,
                connector_type=connector_type,
                connector_id=connector_id,
                get_connection_details=self.helper_get_google_drive_connection_details,
                refresh_token_callback=refresh_callback,
            )

        else:
            raise ValueError(f"Unsupported connector type: {connector_type}")

    """
    #########################################################
    # DOOMED TOP LEVEL FUNCTIONS
    #########################################################
    """

    def helper_get_initialized_credentials(self) -> CredentialStore:
        if not self._credential_store:
            raise RuntimeError(
                "Livedocs is not initialized with report_id and token. Call initialize() with report_id and token first."
            )
        return self._credential_store

    def helper_get_database_connection(self, connector_id: str) -> DatabaseConnection:
        store = self.helper_get_initialized_credentials()
        db = store.get_database(connector_id)
        if db is None:
            db = store.refresh().databases.get(connector_id)
        if db is None:
            raise ValueError(f"Database connector '{connector_id}' not found")
        return db

    def helper_get_database_details(
        self, connector_id: str
    ) -> tuple[DatabaseConnection, dict[str, str]]:
        model = self.helper_get_database_connection(connector_id)
        try:
            parsed = cast(
                dict[str, str], json.loads(model.connection_details.get_secret_value())
            )
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing connection details: {e}")
        return model, parsed

    def helper_get_s3_connection_details(
        self, connector_id: str
    ) -> tuple[object, dict[str, Any]]:
        """Get S3 connector details for use with S3DatasourceConnector."""
        store = self.helper_get_initialized_credentials()
        connector_info = store.get_s3_connector(connector_id)
        if connector_info is None:
            connector_info = store.refresh().s3_connectors.get(connector_id)
        if connector_info is None:
            raise ValueError(f"S3 connector '{connector_id}' not found")
        # Return tuple matching the expected format: (object, dict)
        # Convert TypedDict to regular dict for the second element
        return connector_info, dict(connector_info)

    def refresh_google_drive_token(
        self, connector_info: GoogleDriveConnectorInfo
    ) -> GoogleDriveConnectorInfo:
        """
        Refresh Google Drive token callback.

        Calls the backend API to refresh the Google Drive token and updates
        the credential store with the new credentials.

        Args:
            connector_info: The Google Drive connector info dictionary

        Returns:
            The updated connector_info dictionary with refreshed tokens
        """
        if not self._report_id or not self._token:
            raise RuntimeError(
                "Livedocs is not initialized with report_id and token. Call initialize() with report_id and token first."
            )

        if not self._credential_store:
            raise RuntimeError("Credential store not initialized")

        CORE_URL = os.getenv("LIVEDOCS_CORE_BASE_URL")
        if not CORE_URL:
            raise ValueError("LIVEDOCS_CORE_BASE_URL environment variable not set")

        connector_id = connector_info["connector_id"]

        # Call the refresh endpoint
        response = requests.post(
            f"{CORE_URL}/v1/drive-connectors/refresh/{self._report_id}",
            json={"connector_id": connector_id},
            headers={"authorization": self._token, "Content-Type": "application/json"},
        )

        if response.status_code != 200:
            error_text = response.text
            raise Exception(
                f"Failed to refresh Google Drive token. Status code: {response.status_code}, Error: {error_text}"
            )

        # Parse the response
        response_data = response.json()

        # Parse token_expiry from ISO string to datetime
        token_expiry_str = response_data.get("token_expiry")
        if isinstance(token_expiry_str, str):
            # Parse ISO format datetime string (handles both 'Z' and timezone offsets)
            try:
                # Replace 'Z' with '+00:00' for compatibility with older Python versions
                token_expiry = datetime.fromisoformat(
                    token_expiry_str.replace("Z", "+00:00")
                )
            except (ValueError, AttributeError) as e:
                raise ValueError(
                    f"Invalid token_expiry format: {token_expiry_str}"
                ) from e
        elif isinstance(token_expiry_str, datetime):
            token_expiry = token_expiry_str
        else:
            raise ValueError(f"Invalid token_expiry format: {token_expiry_str}")

        # Create updated connector info
        updated_connector_info: GoogleDriveConnectorInfo = {
            "connector_id": response_data["connector_id"],
            "name": response_data["name"],
            "provider": response_data["provider"],
            "email": response_data["email"],
            "access_token": response_data["access_token"],
            "refresh_token": response_data["refresh_token"],
            "token_expiry": token_expiry,
            "scopes": response_data["scopes"],
        }

        # Update the credential store's bundle
        with self._credential_store._lock:
            if self._credential_store._bundle:
                self._credential_store._bundle.google_drive_connectors[connector_id] = (
                    updated_connector_info
                )

        return updated_connector_info

    def helper_get_google_drive_connection_details(
        self, connector_id: str
    ) -> tuple[object, dict[str, Any]]:
        """Get Google Drive connector details for use with GoogleDriveDatasourceConnector."""
        store = self.helper_get_initialized_credentials()
        connector_info = store.get_google_drive_connector(connector_id)
        if connector_info is None:
            connector_info = store.refresh().google_drive_connectors.get(connector_id)
        if connector_info is None:
            raise ValueError(f"Google Drive connector '{connector_id}' not found")
        # Return tuple matching the expected format: (object, dict)
        # Convert TypedDict to regular dict for the second element
        return connector_info, dict(connector_info)

    def helper_render_jinja_template(self, text: str, context: dict[str, str]) -> str:
        """
        Adds Jinja variables to the given text.

        Args:
            text (str): The text to process.
            context (dict[str, str]): The context for Jinja variables.

        Returns:
            str: The processed text with Jinja variables.
        """
        template = self._template_factory(text)
        return template.render(context)
