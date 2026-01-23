import base64
import gzip
import json
import os
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Callable, Literal, cast

import duckdb
import polars as pl
import requests
from jinja2 import Template

from livedocs.datasources.googledrive import (
    GoogleDriveDatasourceConnector,
)
from livedocs.datasources.s3 import S3DatasourceConnector
from livedocs.manager.credentials import CredentialStore
from livedocs.manager.datasources import DatasourceManager
from livedocs.manager.duckdb import get_duckdb_connection
from livedocs.types import (
    CacheInfo,
    CacheStatus,
    ChartResult,
    DatabaseConnection,
    DBSaveConfig,
    ElementDataSource,
    ElementDatasourceType,
    FileAction,
    FileConnectorType,
    FileNode,
    GCSBucketType,
    GoogleDriveConnectorInfo,
    JsonDisplay,
    ListPathResponse,
    LivedocsResult,
    MsgPackDisplay,
    QueryResult,
    QueryResultMetadata,
    S3ConnectorInfo,
    SDKContext,
    SchemaNode,
    SourceType,
    Spec,
    TableMetadata,
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
    get_query_for_datasource,
    serializer,
)
from livedocs.utils.lib.cache import QueryCache
from livedocs.utils.logging import configure_logging
from livedocs.utils.lib.internals import (
    livedocs_internal_fetch_file_manifest,
    livedocs_internal_file_operation,
    livedocs_internal_list_files,
    livedocs_internal_persist_built_in_vars,
)
from livedocs.utils.lib.vega import create_vega_spec
from livedocs.utils.cells.canvas_helpers import serialize_canvas_data
from livedocs.utils.runtime_fs import (
    list_runtime_files_in_path,
    list_runtime_files_top_level,
)


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
        _setup_dirs()

        self._config: LivedocsConfig = config or LivedocsConfig()

        files_path = os.getenv("LIVEDOCS_FILES_PATH", None)
        if files_path is None:
            raise ValueError("LIVEDOCS_FILES_PATH environment variable is not set.")

        self._duckdb_conn: duckdb.DuckDBPyConnection = get_duckdb_connection(
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
        self.sdk_context: SDKContext = SDKContext.IPYTHON

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
        if not client_id_token:
            self._report_id = report_id
            self._token = token
            self.sdk_context = SDKContext.IPYTHON
            self._credential_store = self._config.credential_store_factory(
                report_id, token
            )
            bundle = self._credential_store.load()
            self.is_initialized = True

            self._secrets = {
                key: secret for key, secret in bundle.workspace_secrets.items()
            }
            self._built_in_vars = {
                key: str(value)
                for key, value in bundle.built_in_vars.items()
                if value is not None
            }
            self._query_cache = self._config.query_cache_factory(report_id, token)
        else:
            self.is_initialized = True
            self.sdk_context = SDKContext.RELAY

        # Configure logging based on SDK context
        configure_logging(self.sdk_context)

    """
    #########################################################
    # PYTHON CELL HELPER FUNCTIONS
    #########################################################
    """

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

    def query(
        self,
        query: str,
        str_datasource: str,
        context: dict[str, str],
        dataframe: pl.DataFrame | None = None,
        limit: int = 10,
        offset: int = 0,
        use_cache: bool = True,
        table_metadata: TableMetadata | None = None,
    ) -> tuple[pl.DataFrame, LivedocsResult]:
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
        parsed = json.loads(str_datasource)

        if not isinstance(parsed, dict) or not parsed:
            raise ValueError(
                "No datasource selected. Please choose a datasource from the dropdown before running your query."
            )
        if "source_type" not in parsed:
            raise ValueError(
                "Invalid datasource: missing 'source_type'. Please choose a datasource from the dropdown before running your query."
            )
        datasource = cast(ElementDataSource, cast(object, parsed))

        # Plug in the Jinja variables
        final_query = self.helper_render_jinja_template(query, context)

        # For file datasources, materialize the file locally (same pathing as preview)
        file_path: str | None = None
        source_type = ElementDatasourceType(datasource["source_type"])
        if source_type == ElementDatasourceType.file:
            file_path = self._prepare_file_for_query(datasource)

        # Run the actual queries
        df: pl.DataFrame = pl.DataFrame()

        # Prepare kwargs for DatasourceManager
        kwargs: dict[str, Any] = {}
        if source_type == ElementDatasourceType.file:
            kwargs["duckdb_conn"] = self._duckdb_conn
            kwargs["download_file"] = self.download_file
            if file_path is not None:
                kwargs["file_path"] = file_path
            kwargs["get_s3_connection_details"] = self.helper_get_s3_connection_details
            kwargs["get_google_drive_connection_details"] = (
                self.helper_get_google_drive_connection_details
            )
        elif source_type == ElementDatasourceType.dataframe:
            kwargs["duckdb_conn"] = self._duckdb_conn
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

        # Apply table operations
        applied_metadata = None
        additional_metadata = {}
        if table_metadata:
            df, additional_metadata = apply_table_operations(df, table_metadata)
            applied_metadata = table_metadata

        # Prepare paginated results
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

        return (df, payload)

    def save_to_database(self, dataframe: pl.DataFrame, str_save_config: str):
        save_config: DBSaveConfig = json.loads(str_save_config)
        result = DatasourceManager.write(
            dataframe, save_config, self.helper_get_database_details
        )
        return result

    def process_raw_text(self, str_src: str, context: dict) -> str:
        """
        Processes raw text by plugging in Jinja variables.

        Args:
            str_src (str): The source text as a JSON string.
            context (dict): The context for Jinja variables.

        Returns:
            str: The processed HTML text.
        """
        src = json.loads(str_src)
        return self.helper_render_jinja_template(src["html"], context)

    def enrich_prompt(self, system, user, context: dict):
        enriched_prompt = {
            "system": self.helper_render_jinja_template(system, context),
            "user": self.helper_render_jinja_template(user, context),
        }
        return MsgPackDisplay(enriched_prompt)

    def process_dependencies(
        self,
        dependencies: str,
        datasource: dict | None = None,
        globals_dict: dict | None = None,
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

    def _prepare_file_for_query(
        self,
        datasource: ElementDataSource,
        download_file: Callable[[str | None, str | None, bool, str | None], str]
        | None = None,
        get_s3_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
        get_google_drive_connection_details: Callable[
            [str], tuple[object, dict[str, Any]]
        ]
        | None = None,
    ) -> str | None:
        """
        Prepare file for query generation by downloading it if needed.
        This is used for preview scenarios where we need the file path
        before generating the query.

        Args:
            datasource: The datasource configuration

        Returns:
            Local file path if file was downloaded, None otherwise
        """
        source_type = ElementDatasourceType(datasource["source_type"])

        if source_type != ElementDatasourceType.file:
            return None

        file_info = datasource.get("file_info")
        if file_info is None:
            return None

        connector_info = file_info.get("connector_info")

        if self.sdk_context == SDKContext.IPYTHON:
            _download_file = self.download_file
            _get_s3_connection_details = self.helper_get_s3_connection_details
            _get_google_drive_connection_details = (
                self.helper_get_google_drive_connection_details
            )
        else:
            if download_file is None:
                raise ValueError("download_file is required")
            if get_s3_connection_details is None:
                raise ValueError("get_s3_connection_details is required")
            if get_google_drive_connection_details is None:
                raise ValueError("get_google_drive_connection_details is required")
            _download_file = download_file
            _get_s3_connection_details = get_s3_connection_details
            _get_google_drive_connection_details = get_google_drive_connection_details

        files_path_env = os.environ.get("LIVEDOCS_FILES_PATH", "")

        file_name = file_info.get("file_name")
        file_id = file_info.get("file_id")

        def resolve_runtime_or_workspace(
            prefer_runtime_path: bool,
        ) -> str | None:
            """
            Handle runtime/workspace files:
            - Try local path under files_path_env (when set).
            - For runtime, return constructed path even if missing (DuckDB will error later).
            - For workspace, try cache then download via _download_file.
            """
            if prefer_runtime_path and file_id:
                if files_path_env:
                    if file_id.startswith("/"):
                        return os.path.join(files_path_env, file_id.lstrip("/"))
                    return os.path.join(files_path_env, file_id)
                return file_id

            # workspace-style: check cache
            if files_path_env and file_name:
                local_path = os.path.join(files_path_env, file_name)
                if os.path.exists(local_path):
                    return local_path

            # Try download by name then id
            if file_name:
                try:
                    return _download_file(file_name, None, False, None)
                except Exception:
                    pass
            if file_id:
                try:
                    return _download_file(None, file_id, False, None)
                except Exception:
                    return None
            return None

        # Handle files without connector_info (runtime or workspace path/download)
        if connector_info is None:
            if file_id is None:
                return None
            return resolve_runtime_or_workspace(prefer_runtime_path=True)

        connector_type = connector_info.get("connector_type")
        connector_id = connector_info.get("connector_id")
        connector_name = connector_info.get("connector_name")

        # Runtime files (with explicit connector_info)
        if connector_type == FileConnectorType.runtime.value:
            return resolve_runtime_or_workspace(prefer_runtime_path=True)

        # Workspace files
        if connector_type == FileConnectorType.workspace.value:
            return resolve_runtime_or_workspace(prefer_runtime_path=False)

        if connector_id is None or file_name is None:
            return None

        # Handle S3 files
        if connector_type == FileConnectorType.s3bucket.value:
            s3_connector = S3DatasourceConnector()
            # Use file_id for the full path within the bucket
            s3_path = file_id if file_id else file_name
            local_path = s3_connector.download_file(
                path=s3_path,
                connector_id=connector_id,
                get_connection_details=_get_s3_connection_details,
                preview=False,  # Download full file for query generation
                connector_name=connector_name,
            )
            return local_path

        # Handle Google Drive files
        if connector_type == FileConnectorType.googledrive.value:
            gdrive_connector = GoogleDriveDatasourceConnector()
            # Use file_id for the full path
            gdrive_path = file_id if file_id else file_name
            local_path = gdrive_connector.download_file(
                file_path=gdrive_path,
                connector_id=connector_id,
                get_connection_details=_get_google_drive_connection_details,
                preview=False,  # Download full file for query generation
                connector_name=connector_name,
            )
            return local_path

        return None

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
        settings: Spec = json.loads(settings_str)
        datasource: ElementDataSource = json.loads(datasource_str)

        # Download file if needed for preview
        file_path = self._prepare_file_for_query(datasource)

        query = get_query_for_datasource(
            datasource,
            50000,
            file_path=file_path,
            get_database_details=self.helper_get_database_details,
        )

        # Prepare kwargs for DatasourceManager
        source_type = ElementDatasourceType(datasource["source_type"])
        kwargs: dict[str, Any] = {}
        if source_type == ElementDatasourceType.file:
            kwargs["duckdb_conn"] = self._duckdb_conn
            kwargs["download_file"] = self.download_file
            # Pass file_path if it was already downloaded (Case 2: preview scenario)
            if file_path is not None:
                kwargs["file_path"] = file_path
        elif source_type == ElementDatasourceType.dataframe:
            kwargs["duckdb_conn"] = self._duckdb_conn
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

        df = apply_chart_filters(df, schema, settings, chart_metadata)

        # Find how many data points we have
        total_data_points = len(df) * len(df.columns)
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
                vega_spec_json_str = validated_spec.model_dump_json(by_alias=True)
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
                vega_spec_json_str = validated_spec.model_dump_json(by_alias=True)
        else:
            vega_spec_json_str = create_vega_spec(df, settings, schema)

        # Post-process the results
        compressed = gzip.compress(vega_spec_json_str.encode("utf-8"))
        encoded = base64.b64encode(compressed).decode("ascii")

        result = ChartResult(data=encoded, cache_info=cache_info)
        payload = LivedocsResult(result)

        return payload

    def _get_table_response(
        self,
        str_datasource: str,
        dataframe=None,
        limit=10,
        offset=0,
        use_cache=True,
        table_metadata=None,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
        download_file: Callable[[str | None, str | None, bool | None, str | None], str]
        | None = None,
        get_s3_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
        get_google_drive_connection_details: Callable[
            [str], tuple[object, dict[str, Any]]
        ]
        | None = None,
    ) -> LivedocsResult:
        """
        Gets a Polars table for a given datasource.

        Args:
            str_datasource (str): The ElementDataSource struct as a JSON string.
            dataframe (optional): A DataFrame used if the datasource type is 'dataframe'. Defaults to None.
            limit (int, optional): The number of rows to return. Defaults to 10.
            offset (int, optional): The offset for the rows to return. Defaults to 0.
            use_cache (bool, optional): Indicates whether to use caching. Defaults to True.
            table_metadata (dict, optional): Metadata for table operations. Defaults to None.

        Returns:
            pl.DataFrame: The resulting Polars DataFrame.
        """
        datasource: ElementDataSource = json.loads(str_datasource)

        if self.sdk_context == SDKContext.IPYTHON:
            _get_database_details = self.helper_get_database_details
            _get_s3_connection_details = self.helper_get_s3_connection_details
            _get_google_drive_connection_details = (
                self.helper_get_google_drive_connection_details
            )
        else:
            if get_database_details is None:
                raise ValueError("get_database_details is required")
            _get_database_details = get_database_details
            if get_s3_connection_details is None:
                raise ValueError("get_s3_connection_details is required")
            _get_s3_connection_details = get_s3_connection_details
            if get_google_drive_connection_details is None:
                raise ValueError("get_google_drive_connection_details is required")
            _get_google_drive_connection_details = get_google_drive_connection_details

        # Download file if needed for preview
        file_path = self._prepare_file_for_query(
            datasource,
            download_file,
            _get_s3_connection_details,
            _get_google_drive_connection_details,
        )

        # Generate query with file_path and get_database_details for Snowflake
        query = get_query_for_datasource(
            datasource,
            None,
            file_path=file_path,
            get_database_details=_get_database_details,
        )

        # Prepare kwargs for DatasourceManager
        source_type = ElementDatasourceType(datasource["source_type"])
        kwargs: dict[str, Any] = {}
        if source_type == ElementDatasourceType.file:
            kwargs["duckdb_conn"] = self._duckdb_conn
            kwargs["download_file"] = self.download_file
            # Pass file_path if it was already downloaded
            if file_path is not None:
                kwargs["file_path"] = file_path
        elif source_type == ElementDatasourceType.dataframe:
            kwargs["duckdb_conn"] = self._duckdb_conn
            kwargs["dataframe"] = dataframe

        df, schema, cache_info = DatasourceManager.read(
            query,
            datasource,
            _get_database_details,
            schema=True,
            use_cache=use_cache,
            query_cache=self._query_cache,
            **kwargs,
        )
        # Apply table operations
        applied_metadata = None
        additional_metadata = {}
        if table_metadata:
            df, additional_metadata = apply_table_operations(df, table_metadata)
            applied_metadata = table_metadata

        # Prepare paginated results
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
        return payload

    def _get_chart_schema(
        self, datasource_str: str, dataframe: pl.DataFrame | None = None
    ) -> dict:
        """
        Returns a dictionary with the schema for a given datasource.

        Args:
            datasource_str (str): The datasource as a JSON string.
            dataframe (pl.DataFrame, optional): A DataFrame used if the datasource type is 'dataframe'. Defaults to None.

        Returns:
            dict: The schema as a base64 encoded string.
        """
        datasource: ElementDataSource = json.loads(datasource_str)

        # Download file if needed for preview
        file_path = self._prepare_file_for_query(datasource)

        # Generate query with file_path and get_database_details for Snowflake
        # For schema extraction, we only need a small sample (1 row is enough to get all column types)
        query = get_query_for_datasource(
            datasource,
            1,
            file_path=file_path,
            get_database_details=self.helper_get_database_details,
        )
        if query is None:
            raise ValueError("Query is required")

        # Prepare kwargs for DatasourceManager based on datasource type
        source_type = ElementDatasourceType(datasource["source_type"])
        kwargs: dict[str, Any] = {}

        if source_type == ElementDatasourceType.file:
            kwargs["duckdb_conn"] = self._duckdb_conn
            kwargs["download_file"] = self.download_file
            # Pass file_path if it was already downloaded (Case 2: preview scenario)
            if file_path is not None:
                kwargs["file_path"] = file_path
        elif source_type == ElementDatasourceType.dataframe:
            kwargs["duckdb_conn"] = self._duckdb_conn
            if dataframe is not None and datasource is not None:
                self._duckdb_conn.register(
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

        return payload

    def process_single_value(self, config: str, context: dict | None = None):
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

    def render_canvas(
        self,
        html: str,
        css: str,
        js: str,
        context: dict | None = None,
    ):
        """
        Render a Canvas element with data passed via livedocs.data object.

        Args:
            html (str): HTML content for the canvas
            css (str): CSS styles for the canvas
            js (str): JavaScript code - access data via livedocs.data.variableName
            context (dict, optional): Context containing variables to pass to JS.
                DataFrames are converted to list of dicts, etc.
                Example: livedocs.data.dataframe_1 becomes [{"col": "val"}, ...]

        Returns:
            JsonDisplay: Canvas render payload with HTML/CSS/JS and serialized data
        """
        ctx = context or {}

        # Serialize context values to JSON-compatible format for the frontend
        # DataFrames → list of dicts, Series → list, etc.
        serialized_data = serialize_canvas_data(ctx)

        result = {
            "__livedocs_canvas__": True,
            "html": html,
            "css": css,
            "js": js,
            "data": serialized_data,  # Data passed to frontend, accessible via livedocs.data
        }

        return JsonDisplay(result)

    """
    #########################################################
    #              FILE MANAGEMENT FUNCTIONS
    #########################################################
    """

    def list_nodes(
        self,
        path_or_parent_id: str | None = None,
        source_type: SourceType | None = None,
        source_id: str | None = None,
        refresh_google_drive_token: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
        get_all_s3_connectors: Callable[[], list[S3ConnectorInfo]] | None = None,
        get_all_google_drive_connectors: Callable[[], list[GoogleDriveConnectorInfo]]
        | None = None,
        list_files: Callable[
            [str | None, str | None, str | None, str | None], ListPathResponse
        ]
        | None = None,
    ):
        # Initialize result containers
        s3_nodes = []
        google_drive_file_nodes = []
        runtime_file_nodes = []
        database_nodes = []
        workspace_file_nodes = []

        if self.sdk_context == SDKContext.IPYTHON:
            if self._credential_store is None:
                raise ValueError("Credential store not initialized")
            if self._report_id is None:
                raise ValueError("Report ID not initialized")
            if self._token is None:
                raise ValueError("Token not initialized")

            _get_all_s3_connectors = self._credential_store.get_all_s3_connectors
            _get_all_google_drive_connectors = (
                self._credential_store.get_all_google_drive_connectors
            )
            _list_files = livedocs_internal_list_files
            _list_runtime_files_top_level = list_runtime_files_top_level
            _list_runtime_files_in_path = list_runtime_files_in_path
            _get_single_s3_connector = self._credential_store.get_s3_connector
            _get_single_google_drive_connector = (
                self._credential_store.get_google_drive_connector
            )
            _refresh_google_drive_token = self.refresh_google_drive_token
            _get_google_drive_connection_details = (
                self.helper_get_google_drive_connection_details
            )
            _get_s3_connection_details = self.helper_get_s3_connection_details
        else:
            if get_all_s3_connectors is None:
                raise ValueError("get_all_s3_connectors is required")
            if get_all_google_drive_connectors is None:
                raise ValueError("get_all_google_drive_connectors is required")
            if list_files is None:
                raise ValueError("list_files is required")
            _get_all_s3_connectors = get_all_s3_connectors
            _get_all_google_drive_connectors = get_all_google_drive_connectors
            _list_files = list_files
            _refresh_google_drive_token = refresh_google_drive_token

            def _list_runtime_files_top_level() -> list[FileNode]:
                return []

            def _list_runtime_files_in_path(path: str) -> list[FileNode]:
                return []

            def _get_single_s3_connector(connector_id: str) -> S3ConnectorInfo | None:
                all_connectors = _get_all_s3_connectors()
                for connector in all_connectors:
                    if connector["connector_id"] == connector_id:
                        return connector
                return None

            def _get_single_google_drive_connector(
                connector_id: str,
            ) -> GoogleDriveConnectorInfo | None:
                all_connectors = _get_all_google_drive_connectors()
                for connector in all_connectors:
                    if connector["connector_id"] == connector_id:
                        return connector
                return None

            def _get_google_drive_connection_details(
                connector_id: str,
            ) -> tuple[object, dict[str, Any]]:
                all_connectors = _get_all_google_drive_connectors()
                for connector in all_connectors:
                    if connector["connector_id"] == connector_id:
                        return connector, dict(connector)
                return None, {}

            def _get_s3_connection_details(
                connector_id: str,
            ) -> tuple[object, dict[str, Any]]:
                all_connectors = _get_all_s3_connectors()
                for connector in all_connectors:
                    if connector["connector_id"] == connector_id:
                        return connector, dict(connector)
                return None, {}

        # FIRST CASE: No params given (all three are None) -> return roots
        if path_or_parent_id is None and source_type is None and source_id is None:
            if (
                self._credential_store
                and self._report_id
                and self._token
                and self.sdk_context == SDKContext.IPYTHON
            ):
                for connector_info in _get_all_s3_connectors():
                    s3_nodes.append(
                        S3DatasourceConnector.connector_info_to_file_node(
                            connector_info
                        )
                    )

                for connector_info in _get_all_google_drive_connectors():
                    google_drive_file_nodes.append(
                        GoogleDriveDatasourceConnector.connector_info_to_file_node(
                            connector_info
                        )
                    )

                warehouses_and_files = _list_files(
                    self._report_id, self._token, None, None
                )

                workspace_file_nodes = warehouses_and_files.files
                database_nodes = warehouses_and_files.schema_nodes

                runtime_file_nodes = _list_runtime_files_top_level()

                return {
                    "s3buckets": s3_nodes,
                    "googledrive": google_drive_file_nodes,
                    "runtime": runtime_file_nodes,
                    "databases": database_nodes,
                    "workspace_files": workspace_file_nodes,
                }
            else:
                if self.sdk_context == SDKContext.RELAY:
                    raise ValueError(
                        "RelayImplementationError: List nodes is not supported in relay context"
                    )
                raise ValueError("Credential store not initialized")

        # SECOND CASE: path_or_parent_id and source_type provided
        # source_id is optional for workspace/database (built-in sources)
        # source_id is required for s3bucket/googledrive (connector-based sources)
        elif path_or_parent_id is not None and source_type is not None:
            # Normalize path_or_parent_id: "/" means root level (None for database_parent_id)
            effective_parent_id = (
                None if path_or_parent_id == "/" else path_or_parent_id
            )

            # Split into 5 categories based on source_type
            if (
                source_type == SourceType.workspace
                or source_type == SourceType.database
            ):
                warehouses_and_files = _list_files(
                    self._report_id,
                    self._token,
                    effective_parent_id,
                    None,
                )
                workspace_file_nodes = warehouses_and_files.files
                database_nodes = warehouses_and_files.schema_nodes

            elif source_type == SourceType.runtime:
                # path_or_parent_id is the path in this case
                # Runtime doesn't use source_id, path is hashed alone
                runtime_file_nodes = _list_runtime_files_in_path(
                    path=path_or_parent_id or "",
                )
            elif source_type == SourceType.s3bucket:
                if source_id is None:
                    # No source_id: list all S3 connectors as root nodes
                    for connector_info in _get_all_s3_connectors():
                        s3_nodes.append(
                            S3DatasourceConnector.connector_info_to_file_node(
                                connector_info
                            )
                        )
                else:
                    # source_id provided: list files in that connector
                    connector_info = _get_single_s3_connector(source_id)
                    if connector_info:
                        s3_connector = S3DatasourceConnector()
                        s3_nodes = s3_connector.list(
                            path=path_or_parent_id,
                            connector_id=source_id,
                            get_connection_details=_get_s3_connection_details,
                        )
                    else:
                        s3_nodes = []
            elif source_type == SourceType.googledrive:
                if source_id is None:
                    # No source_id: list all Google Drive connectors as root nodes
                    for connector_info in _get_all_google_drive_connectors():
                        google_drive_file_nodes.append(
                            GoogleDriveDatasourceConnector.connector_info_to_file_node(
                                connector_info
                            )
                        )
                else:
                    # source_id provided: list files in that connector
                    connector_info = _get_single_google_drive_connector(source_id)
                    if not connector_info:
                        raise ValueError(f"Connector '{source_id}' not found")

                    google_drive_connector = GoogleDriveDatasourceConnector()
                    google_drive_file_nodes = google_drive_connector.list(
                        path=path_or_parent_id,
                        connector_id=source_id,
                        get_connection_details=_get_google_drive_connection_details,
                        refresh_token_callback=_refresh_google_drive_token,
                    )

            else:
                raise ValueError(f"Unsupported source type: {source_type}")

            return {
                "workspace_files": workspace_file_nodes,
                "databases": database_nodes,
                "runtime": runtime_file_nodes,
                "s3buckets": s3_nodes,
                "googledrive": google_drive_file_nodes,
            }

        else:
            raise ValueError(
                "Invalid parameters: path_or_parent_id and source_type are required."
            )

    def search_nodes(
        self,
        query: str,
        source_type: SourceType | None = None,
        list_files: Callable[
            [str | None, str | None, str | None, str | None], ListPathResponse
        ]
        | None = None,
        get_all_s3_connectors: Callable[[], list[S3ConnectorInfo]] | None = None,
        get_all_google_drive_connectors: Callable[[], list[GoogleDriveConnectorInfo]]
        | None = None,
        refresh_google_drive_token: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
        max_results: int = 50,
    ):
        """
        Search across datasources.

        When source_type is None, searches ALL sources (files, databases, S3, Google Drive).
        When source_type is provided, searches only that specific source type.

        Args:
            query: Search string to match against file/table names (case-insensitive)
            source_type: Optional - filter to specific source type.
                         If None, searches ALL sources.

        Returns:
            Dict with keys: s3buckets, googledrive, runtime, databases, workspace_files
            Each key contains a list of matching nodes (FileNode or SchemaNode).
        """
        if not query or not query.strip():
            raise ValueError("Search query cannot be empty")

        # Initialize result containers
        s3_nodes: list[FileNode] = []
        google_drive_nodes: list[FileNode] = []
        runtime_nodes: list[FileNode] = []
        database_nodes: list[SchemaNode] = []
        workspace_nodes: list[FileNode] = []

        # Determine which sources to search
        search_all = source_type is None

        if self.sdk_context == SDKContext.IPYTHON:
            if not self._credential_store:
                raise ValueError("Credential store not initialized")
            if not self._report_id:
                raise ValueError("Report ID not initialized")
            if not self._token:
                raise ValueError("Token not initialized")
            _get_all_s3_connectors = self._credential_store.get_all_s3_connectors
            _get_all_google_drive_connectors = (
                self._credential_store.get_all_google_drive_connectors
            )
            _list_files = livedocs_internal_list_files
            _list_runtime_files_in_path = list_runtime_files_in_path
            _refresh_google_drive_token = self.refresh_google_drive_token
            _get_google_drive_connection_details = (
                self.helper_get_google_drive_connection_details
            )
            _get_s3_connection_details = self.helper_get_s3_connection_details
        else:
            if list_files is None:
                raise ValueError("list_files is required")
            if get_all_s3_connectors is None:
                raise ValueError("get_all_s3_connectors is required")
            if get_all_google_drive_connectors is None:
                raise ValueError("get_all_google_drive_connectors is required")
            if refresh_google_drive_token is None:
                raise ValueError("refresh_google_drive_token is required")

            _get_all_s3_connectors = get_all_s3_connectors
            _get_all_google_drive_connectors = get_all_google_drive_connectors
            _list_files = list_files
            _refresh_google_drive_token = refresh_google_drive_token

            def _list_runtime_files_in_path(
                path: str, search_string: str | None = None, max_depth: int = 3
            ) -> list[FileNode]:
                return []

            def _get_s3_connection_details(
                connector_id: str,
            ) -> tuple[object, dict[str, Any]]:
                all_connectors = _get_all_s3_connectors()
                for connector in all_connectors:
                    if connector["connector_id"] == connector_id:
                        return connector, dict(connector)
                return None, {}

            def _get_google_drive_connection_details(
                connector_id: str,
            ) -> tuple[object, dict[str, Any]]:
                all_connectors = _get_all_google_drive_connectors()
                for connector in all_connectors:
                    if connector["connector_id"] == connector_id:
                        return connector, dict(connector)
                return None, {}

        # Search S3
        if search_all or source_type == SourceType.s3bucket:
            for connector_info in _get_all_s3_connectors():
                s3_connector = S3DatasourceConnector()
                nodes = s3_connector.search(
                    search_query=query,
                    connector_id=connector_info["connector_id"],
                    get_connection_details=_get_s3_connection_details,
                    max_results=max_results,
                )
                s3_nodes.extend(nodes)

        # Search Google Drive
        if search_all or source_type == SourceType.googledrive:
            for connector_info in _get_all_google_drive_connectors():
                gdrive_connector = GoogleDriveDatasourceConnector()
                nodes = gdrive_connector.search(
                    search_query=query,
                    connector_id=connector_info["connector_id"],
                    get_connection_details=_get_google_drive_connection_details,
                    refresh_token_callback=_refresh_google_drive_token,
                    max_results=max_results,
                )
                google_drive_nodes.extend(nodes)

        # Search workspace files and databases (via Core API)
        if search_all or source_type in (SourceType.workspace, SourceType.database):
            result = _list_files(self._report_id, self._token, None, query)
            if search_all or source_type == SourceType.workspace:
                workspace_nodes = result.files
            if search_all or source_type == SourceType.database:
                database_nodes = result.schema_nodes

        # Search runtime files
        if search_all or source_type == SourceType.runtime:
            runtime_nodes = _list_runtime_files_in_path(
                path="", search_string=query, max_depth=3
            )

        nodes = {
            "s3buckets": s3_nodes,
            "googledrive": google_drive_nodes,
            "runtime": runtime_nodes,
            "databases": database_nodes,
            "workspace_files": workspace_nodes,
        }

        return nodes

    def get_file_url(
        self,
        source_type: SourceType,
        source_id: str | None = None,
        path: str | None = None,
        refresh_google_drive_token: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
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
        if source_type == SourceType.workspace:
            if not source_id:
                raise ValueError("source_id is required for workspace file operations")

            if not self._credential_store or not self._report_id or not self._token:
                raise ValueError("Credential store not initialized")

            if self.sdk_context == SDKContext.IPYTHON:
                manifest_data = livedocs_internal_fetch_file_manifest(
                    report_id=self._report_id,
                    token=self._token,
                    action="read",
                    bucket=GCSBucketType.USER_FILES,
                    file_id=source_id,
                )

                return manifest_data.signed_url
            else:
                raise ValueError(
                    "RelayImplementationError: Get file URL in workspace is not supported in relay context"
                )
        # If no file_id, return download URL/link based on connector_type
        if source_type == SourceType.runtime:
            if self.sdk_context == SDKContext.RELAY:
                raise ValueError(
                    "RelayImplementationError: Get file URL in runtime is not supported in relay context"
                )

            # For runtime, file is already local - just return the path
            if not path:
                raise ValueError("path is required for runtime connector type")

            if not os.path.exists(path):
                raise ValueError(f"File not found: {path}")
            return path

        # For S3 and Google Drive, we need connector_id and path
        if not source_id:
            raise ValueError("connector_id is required for S3 operations")
        if not path:
            raise ValueError("path is required for S3 operations")

        if source_type == SourceType.s3bucket:
            if self.sdk_context == SDKContext.IPYTHON and not self._credential_store:
                raise ValueError("Credential store not initialized")

            s3_connector = S3DatasourceConnector()
            signed_url = s3_connector.get_signed_url(
                file_path=path,
                connector_id=source_id,
                get_connection_details=self.helper_get_s3_connection_details
                if self.sdk_context == SDKContext.IPYTHON
                else get_connection_details,
            )
            if signed_url is None:
                raise ValueError(f"Failed to generate signed URL for path: {path}")
            return signed_url

        elif source_type == SourceType.googledrive:
            if self.sdk_context == SDKContext.IPYTHON and not self._credential_store:
                raise ValueError("Credential store not initialized")

            google_drive_connector = GoogleDriveDatasourceConnector()
            refresh_callback = (
                refresh_google_drive_token
                if refresh_google_drive_token
                else self.refresh_google_drive_token
            )

            download_url = google_drive_connector.get_signed_url(
                file_path=path,
                connector_id=source_id,
                get_connection_details=self.helper_get_google_drive_connection_details
                if self.sdk_context == SDKContext.IPYTHON
                else get_connection_details,
                refresh_token_callback=refresh_callback,
            )

            if download_url is None:
                raise ValueError(f"Failed to get download URL for path: {path}")
            return download_url

        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    def upload_runtime_file(
        self,
        path: str,
        destination_type: Literal[
            SourceType.googledrive, SourceType.s3bucket, SourceType.workspace
        ],
        destination_id: str | None = None,
        destination_path: str | None = None,
        refresh_google_drive_token: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
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
        if not os.path.exists(path):
            raise ValueError(f"Local file not found: {path}")

        match destination_type:
            case SourceType.s3bucket:
                if not destination_id:
                    raise ValueError("destination_id is required for S3 operations")

                if (
                    self.sdk_context == SDKContext.IPYTHON
                    and not self._credential_store
                ):
                    raise ValueError("Credential store not initialized")

                s3_connector = S3DatasourceConnector()
                return s3_connector.upload_file_to_s3(
                    file_path=path,
                    connector_id=destination_id,
                    s3_path=destination_path,
                    get_connection_details=self.helper_get_s3_connection_details
                    if self.sdk_context == SDKContext.IPYTHON
                    else get_connection_details,
                )
            case SourceType.googledrive:
                if not destination_id:
                    raise ValueError(
                        "destination_id is required for Google Drive operations"
                    )
                if (
                    self.sdk_context == SDKContext.IPYTHON
                    and not self._credential_store
                ):
                    raise ValueError("Credential store not initialized")

                google_drive_connector = GoogleDriveDatasourceConnector()
                refresh_callback = (
                    refresh_google_drive_token
                    if refresh_google_drive_token
                    else self.refresh_google_drive_token
                )
                return google_drive_connector.upload_file_to_googledrive(
                    file_path=path,
                    connector_id=destination_id,
                    drive_path=destination_path,
                    get_connection_details=self.helper_get_google_drive_connection_details
                    if self.sdk_context == SDKContext.IPYTHON
                    else get_connection_details,
                    refresh_token_callback=refresh_callback,
                )
            case SourceType.workspace:
                raise ValueError(
                    "RelayImplementationError: Upload runtime file to workspace is not supported in relay context"
                )

    def delete_file(
        self,
        source_type: SourceType,
        path: str | None = None,
        source_id: str | None = None,
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
        if source_type == SourceType.runtime:
            # Local file system operation
            if not path:
                raise ValueError("path is required for runtime connector type")

            try:
                if os.path.exists(path):
                    os.remove(path)
                    return True
                return False
            except Exception:
                return False

        elif source_type == SourceType.s3bucket:
            if not source_id or not path:
                raise ValueError("connector_id and path are required for S3 operations")
            if not self._credential_store:
                raise ValueError("Credential store not initialized")

            s3_connector = S3DatasourceConnector()
            return s3_connector.delete_file(
                file_path=path,
                connector_id=source_id,
                get_connection_details=self.helper_get_s3_connection_details,
            )

        elif source_type == SourceType.googledrive:
            if not source_id or not path:
                raise ValueError(
                    "connector_id and path are required for Google Drive operations"
                )
            if not self._credential_store:
                raise ValueError("Credential store not initialized")

            google_drive_connector = GoogleDriveDatasourceConnector()
            refresh_callback = (
                refresh_google_drive_token
                if refresh_google_drive_token
                else self.refresh_google_drive_token
            )
            return google_drive_connector.delete_file(
                file_path=path,
                connector_id=source_id,
                get_connection_details=self.helper_get_google_drive_connection_details,
                refresh_token_callback=refresh_callback,
            )

        elif source_type == SourceType.workspace:
            if not source_id:
                raise ValueError("source_id is required for workspace file operations")

            if self.sdk_context == SDKContext.IPYTHON:
                if not self._credential_store or not self._report_id or not self._token:
                    raise ValueError("Credential store not initialized")

                return livedocs_internal_file_operation(
                    report_id=self._report_id,
                    token=self._token,
                    file_id=source_id,
                    action=FileAction.DELETE,
                )
            else:
                raise ValueError(
                    "RelayImplementationError: Delete file in workspace is not supported in relay context"
                )

        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    def rename_file(
        self,
        source_type: SourceType,
        new_name: str,
        path: str | None = None,
        source_id: str | None = None,
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
        if source_type == SourceType.runtime:
            # Local file system operation
            if not path:
                raise ValueError("path is required for runtime connector type")

            try:
                # Construct new path by replacing the filename
                parent_path = os.path.dirname(path)
                if parent_path:
                    new_path = os.path.join(parent_path, new_name)
                else:
                    new_path = new_name

                if os.path.exists(path):
                    os.rename(path, new_path)
                    return True
                return False
            except Exception:
                return False

        elif source_type == SourceType.s3bucket:
            if not source_id or not path:
                raise ValueError("connector_id and path are required for S3 operations")
            if not self._credential_store:
                raise ValueError("Credential store not initialized")

            s3_connector = S3DatasourceConnector()
            return s3_connector.rename_file(
                file_path=path,
                new_name=new_name,
                connector_id=source_id,
                get_connection_details=self.helper_get_s3_connection_details,
            )

        elif source_type == SourceType.googledrive:
            if not source_id or not path:
                raise ValueError(
                    "connector_id and path are required for Google Drive operations"
                )
            if not self._credential_store:
                raise ValueError("Credential store not initialized")

            google_drive_connector = GoogleDriveDatasourceConnector()
            refresh_callback = (
                refresh_google_drive_token
                if refresh_google_drive_token
                else self.refresh_google_drive_token
            )
            return google_drive_connector.rename_file(
                file_path=path,
                new_name=new_name,
                connector_id=source_id,
                get_connection_details=self.helper_get_google_drive_connection_details,
                refresh_token_callback=refresh_callback,
            )

        elif source_type == SourceType.workspace:
            if not source_id:
                raise ValueError("source_id is required for workspace file operations")

            if self.sdk_context == SDKContext.IPYTHON:
                if not self._credential_store or not self._report_id or not self._token:
                    raise ValueError("Credential store not initialized")

                return livedocs_internal_file_operation(
                    report_id=self._report_id,
                    token=self._token,
                    file_id=source_id,
                    action=FileAction.RENAME,
                    new_name=new_name,
                )
            else:
                raise ValueError(
                    "RelayImplementationError: Rename file in workspace is not supported in relay context"
                )
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    """
    #########################################################
    # HELPER TOP LEVEL FUNCTIONS
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
