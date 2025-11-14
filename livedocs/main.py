import base64
import gzip
import json
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Callable, Dict, List, Optional

import pandas as pd
import polars as pl
import sentry_sdk
from jinja2 import Template

from livedocs.cache import QueryCache
from livedocs.manager.credentials import CredentialStore
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
    GCSBucketType,
    JsonDisplay,
    LivedocsResult,
    MsgPackDisplay,
    QueryResult,
    QueryResultMetadata,
    Schema,
    Spec,
    VegaSpec,
    WorkspaceSecret,
)
from livedocs.utils.bigquery import process_bigquery_schema
from livedocs.utils.chart_helpers import apply_chart_filters
from livedocs.utils.clickhouse import process_clickhouse_schema
from livedocs.utils.common import (
    _LIVEDOCS_PROTECTED_VARS,
    _capture_exceptions,
    _download_file,
    _fetch_file_manifest,
    _get_dataframe_schema,
    _persist_built_in_vars,
    _setup_dirs,
    _setup_sentry,
    get_run_context,
    sanitize_sensitive_data,
)
from livedocs.utils.debug import debug
from livedocs.utils.databricks import process_databricks_schema
from livedocs.datasources import bigquery as bigquery_datasource
from livedocs.datasources import databricks as databricks_datasource
from livedocs.datasources import clickhouse as clickhouse_datasource
from livedocs.datasources import motherduck as motherduck_datasource
from livedocs.datasources import postgres as postgres_datasource
from livedocs.datasources import snowflake as snowflake_datasource
from livedocs.utils.postgres import _process_postgres_schema
from livedocs.utils.serialize import serializer
from livedocs.utils.single_value_helpers import process_single_value
from livedocs.utils.snowflake import process_snowflake_schema
from livedocs.utils.table_helpers import apply_table_operations
from livedocs.vega import create_vega_spec, get_altair_datasource_query


_process_motherduck_schema = _process_postgres_schema


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

    def __init__(self, config: Optional[LivedocsConfig] = None):
        """
        Creates the Livedocs instance, setting up necessary components and configurations.
        """
        _setup_sentry()
        _setup_dirs()

        self._config = config or LivedocsConfig()

        self._duckdb = DuckDBSingleton(
            file_search_path=[os.getenv("LIVEDOCS_FILES_PATH")]
        )
        self._credential_store: Optional[CredentialStore] = None
        self._secrets: Dict[str, WorkspaceSecret] = {}
        self._built_in_vars: Dict[str, str] = {}
        self._template_factory = lru_cache(maxsize=self._config.template_cache_size)(
            self._compile_template
        )
        self._query_cache: Optional[QueryCache] = None
        self.is_initialized = False

    def initialize(self, report_id: str, token: str) -> tuple[object, dict]:
        """
        Initializes the Livedocs instance with the given report ID and token.
        Called when the pod is initialized.

        Args:
            report_id (str): The report ID.
            token (str): The session token.
        """
        with sentry_sdk.start_transaction(op="task", name="initialize vm-lib"):
            sentry_sdk.set_tag("report_id", report_id)
            self._report_id = report_id
            self._token = token
            span = sentry_sdk.start_span(name="fetch credentials")
            self._credential_store = self._config.credential_store_factory(
                report_id, token
            )
            bundle = self._credential_store.load()
            span.finish()
            self.is_initialized = True

            self._secrets = {
                key: secret for key, secret in bundle.workspace_secrets.items()
            }
            self._built_in_vars = {**bundle.built_in_vars}
            self._query_cache = self._config.query_cache_factory(report_id, token)

    def _require_store(self) -> CredentialStore:
        if not self._credential_store:
            raise RuntimeError("Livedocs is not initialized. Call initialize() first.")
        return self._credential_store

    def _get_database_connection(self, connector_id: str) -> DatabaseConnection:
        store = self._require_store()
        db = store.get_database(connector_id)
        if db is None:
            db = store.refresh().databases.get(connector_id)
        if db is None:
            raise ValueError(f"Database connector '{connector_id}' not found")
        return db

    def _get_database_details(
        self, connector_id: str
    ) -> tuple[DatabaseConnection, dict]:
        model = self._get_database_connection(connector_id)
        try:
            parsed = json.loads(model.connection_details.get_secret_value())
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing connection details: {e}")
        return model, parsed

    @_capture_exceptions
    def set_var(self, key: str, value: str):
        """
        Sets a built-in variable.

        Args:
            key (str): The variable key.
            value (str): The variable value.
        """
        self._built_in_vars[key] = value
        _persist_built_in_vars(self._report_id, self._token, self._built_in_vars)

    @_capture_exceptions
    def get_var(self, key: str) -> str:
        """
        Gets the value of a built-in variable.

        Args:
            key (str): The variable key.

        Returns:
            str: The variable value.
        """
        if key == "run_context":
            return os.getenv("RUN_CONTEXT")

        return self._built_in_vars.get(key, None)

    @_capture_exceptions
    def unset_var(self, key: str):
        """
        Unsets a built-in variable.

        Args:
            key (str): The variable key.
        """
        if key not in _LIVEDOCS_PROTECTED_VARS:
            self._built_in_vars.pop(key, None)
            _persist_built_in_vars(self._report_id, self._token, self._built_in_vars)

    @_capture_exceptions
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
        _persist_built_in_vars(self._report_id, self._token, self._built_in_vars)

    @_capture_exceptions
    def secrets(self, key, default_value="") -> str:
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
            store = self._require_store()
        except RuntimeError:
            return default_value

        secret_model = store.get_secret(key)
        if secret_model is None:
            secret_model = store.refresh().workspace_secrets.get(key)
        if secret_model:
            self._secrets[key] = secret_model
            return secret_model.value.get_secret_value()

        return default_value

    @_capture_exceptions
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
            final_query = self.add_jinja_vars(query, context)

            # Run the actual queries
            query_span = sentry_sdk.start_span(name="run _query_with_schema")
            df: pl.DataFrame = pl.DataFrame()
            df, schema, cache_info = self._query_with_schema(
                final_query, datasource, dataframe, use_cache
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

    @_capture_exceptions
    @sentry_sdk.trace
    def save_to_database(self, dataframe: pl.DataFrame, str_save_config: str):
        with sentry_sdk.start_transaction(op="task", name="save to database"):
            save_config: DBSaveConfig = json.loads(str_save_config)
            if DatabaseType(save_config["database_type"]) == DatabaseType.Postgres:
                current_run_context = get_run_context()
                if current_run_context in save_config["run_settings"]:
                    result = self._write_to_postgres(dataframe, save_config)
                    return result
                else:
                    pass
            elif DatabaseType(save_config["database_type"]) == DatabaseType.Motherduck:
                current_run_context = get_run_context()
                if current_run_context in save_config["run_settings"]:
                    result = self._write_to_motherduck(dataframe, save_config)
                    return result
                else:
                    pass
            elif DatabaseType(save_config["database_type"]) == DatabaseType.Bigquery:
                current_run_context = get_run_context()
                if current_run_context in save_config["run_settings"]:
                    result = self._write_to_bigquery(dataframe, save_config)
                    return result
                else:
                    pass
            elif DatabaseType(save_config["database_type"]) == DatabaseType.Snowflake:
                current_run_context = get_run_context()
                if current_run_context in save_config["run_settings"]:
                    result = self._write_to_snowflake(dataframe, save_config)
                    return result
                else:
                    pass
            elif DatabaseType(save_config["database_type"]) == DatabaseType.Clickhouse:
                current_run_context = get_run_context()
                if current_run_context in save_config["run_settings"]:
                    result = self._write_to_clickhouse(dataframe, save_config)
                    return result
                else:
                    pass
            else:
                raise Exception("Unsupported database type")

    @_capture_exceptions
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
            return self.add_jinja_vars(src["html"], context)

    @_capture_exceptions
    def enrich_prompt(self, system, user, context: dict):
        enriched_prompt = {
            "system": self.add_jinja_vars(system, context),
            "user": self.add_jinja_vars(user, context),
        }
        return MsgPackDisplay(enriched_prompt)

    def _compile_template(self, text: str) -> Template:
        """Compile a Jinja template string. Wrapped so we can memoize easily."""

        return Template(text)

    def add_jinja_vars(self, text: str, context: dict) -> str:
        """
        Adds Jinja variables to the given text.

        Args:
            text (str): The text to process.
            context (dict): The context for Jinja variables.

        Returns:
            str: The processed text with Jinja variables.
        """
        template = self._template_factory(text)
        return template.render(context)

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

    @_capture_exceptions
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
            query_span = sentry_sdk.start_span(name="run _query_with_schema")
            query = get_altair_datasource_query(datasource)

            if (
                datasource["source_type"] == "database_table"
                and DatabaseType(datasource["database_info"]["database_type"])
                == DatabaseType.Snowflake
            ):
                try:
                    db_connector_id = datasource["database_info"][
                        "database_connector_id"
                    ]
                    _, parsed_credentials = self._get_database_details(db_connector_id)
                except KeyError as e:
                    raise ValueError(f"Missing required information: {e}")

                query = f'SELECT * FROM "{parsed_credentials["database"]}"."{datasource["database_table_info"]["schema_name"]}"."{datasource["database_table_info"]["table_name"]}" LIMIT 500000;'

            df, schema, cache_info = self._query_with_schema(
                query,
                datasource,
                dataframe,
                use_cache,
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

    @_capture_exceptions
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

            query = get_altair_datasource_query(datasource)
            if (
                datasource["source_type"] == "database_table"
                and DatabaseType(datasource["database_info"]["database_type"])
                == DatabaseType.Snowflake
            ):
                try:
                    db_connector_id = datasource["database_info"][
                        "database_connector_id"
                    ]
                    _, parsed_credentials = self._get_database_details(db_connector_id)
                except KeyError as e:
                    raise ValueError(f"Missing required information: {e}")

                query = f'SELECT * FROM "{parsed_credentials["database"]}"."{datasource["database_table_info"]["schema_name"]}"."{datasource["database_table_info"]["table_name"]}"'

            query_span = sentry_sdk.start_span(name="run _query_with_schema")
            df, schema, cache_info = self._query_with_schema(
                query,
                datasource,
                dataframe,
                use_cache,
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

    @_capture_exceptions
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

            query_span = sentry_sdk.start_span(name="run _query_with_schema")
            match ElementDatasourceType(datasource["source_type"]):
                case ElementDatasourceType.database_table:
                    if (
                        DatabaseType(datasource["database_info"]["database_type"])
                        == DatabaseType.Bigquery
                    ):
                        query = f"SELECT * FROM {datasource['database_table_info']['schema_name']}.{datasource['database_table_info']['table_name']} LIMIT 10"
                    elif (
                        DatabaseType(datasource["database_info"]["database_type"])
                        == DatabaseType.Clickhouse
                    ):
                        query = f"SELECT * FROM {datasource['database_table_info']['schema_name']}.{datasource['database_table_info']['table_name']} LIMIT 10"
                    elif DatabaseType(datasource["database_info"]["database_type"]) in {
                        DatabaseType.Postgres,
                        DatabaseType.Motherduck,
                    }:
                        query = f'SELECT * FROM "{datasource["database_table_info"]["schema_name"]}"."{datasource["database_table_info"]["table_name"]}" LIMIT 10'
                    elif (
                        DatabaseType(datasource["database_info"]["database_type"])
                        == DatabaseType.Databricks
                    ):
                        query = f'SELECT * FROM {datasource["database_table_info"]["catalog_name"]}.{datasource["database_table_info"]["schema_name"]}.{datasource["database_table_info"]["table_name"]} LIMIT 10'
                    else:
                        query = f"SELECT * FROM {datasource['database_info']['database_name']}.{datasource['database_table_info']['schema_name']}.{datasource['database_table_info']['table_name']} LIMIT 10"
                    _, schema = self._query_database_with_schema(query, datasource)
                    query_span.finish()
                case ElementDatasourceType.file:
                    file_name = datasource["file_info"]["file_name"]
                    if datasource["file_info"]["file_type"] == "csv":
                        query = f"SELECT * FROM read_csv_auto('{file_name}') LIMIT 10"
                    elif datasource["file_info"]["file_type"] == "xlsx":
                        query = f"SELECT * FROM read_xlsx('{file_name}', sheet='{datasource['file_info']['layer_name']}') LIMIT 10"
                    _, schema = self._query_file_with_schema(query, datasource)
                    query_span.finish()
                case ElementDatasourceType.dataframe:
                    if dataframe is not None and datasource is not None:
                        self._duckdb.conn.register(
                            datasource["dataframe_info"]["df_name"], dataframe
                        )
                    query = f"SELECT * FROM {datasource['dataframe_info']['df_name']} LIMIT 10"
                    _, schema = self._query_dataframe_with_schema(query, datasource)
                    query_span.finish()
                case _:
                    query_span.finish()
                    return "Unknown or unsupported datasource type for chart schema"

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
            post_span.finish()

            return payload

    def _query_with_schema(
        self,
        query: str,
        datasource: ElementDataSource,
        dataframe=None,
        use_cache=True,
    ) -> tuple[
        pl.DataFrame,
        dict,
        CacheInfo,
    ]:
        """
        Executes a query on a given datasource with schema handling and optional caching.

        Args:
            query (str): The SQL query string to execute.
            datasource (ElementDataSource): The datasource to execute the query on.
            dataframe (optional): A DataFrame used if the datasource type is 'dataframe'. Defaults to None.
            use_cache (bool): Indicates whether to use caching. Defaults to True.

        Returns:
            tuple[pl.DataFrame, dict, CacheMetadata]: A tuple containing the resulting DataFrame,
            schema as a dict, and info about the cache.
        """

        if self._query_cache is None:
            raise RuntimeError("Livedocs is not initialized. Call initialize() first.")

        cache_info = CacheInfo(
            id=self._query_cache.generate_cache_id(query, datasource),
            status=CacheStatus.MISS,
        )

        # Use cache if enabled and the query is found in the cache
        if use_cache:
            cache_result = self._query_cache.get(query, datasource)
            if cache_result is not None and not cache_result[0].is_empty():
                cache_info["status"] = CacheStatus.HIT
                return (*cache_result, cache_info)

        # Execute query based on datasource type
        match ElementDatasourceType(datasource["source_type"]):
            case ElementDatasourceType.database | ElementDatasourceType.database_table:
                result = self._query_database_with_schema(query, datasource)
            case ElementDatasourceType.file:
                result = self._query_file_with_schema(query, datasource)
            case ElementDatasourceType.dataframe:
                if dataframe is not None:
                    self._duckdb.conn.register(
                        datasource["dataframe_info"]["df_name"], dataframe
                    )
                result = self._query_dataframe_with_schema(query, datasource)
            case _:
                return "Unknown ElementDataSource"

        # We always cache the result, so it's available for querying in public mode
        self._query_cache.set(query, datasource, result)

        return (*result, cache_info)

    def _query_database_with_schema(
        self, query: str, datasource: ElementDataSource
    ) -> tuple[pl.DataFrame, dict]:
        """
        Queries a database and returns the result as a DataFrame with schema.

        Args:
            query (str): The query string.
            datasource (ElementDataSource): The datasource to execute the query on.

        Returns:
            tuple[pl.DataFrame, dict]: A tuple containing the resulting DataFrame and schema as a dict.
        """
        match DatabaseType(datasource["database_info"]["database_type"]):
            case DatabaseType.Postgres:
                result_df, schema_df = self._query_postgres(query, datasource)
                schema = _process_postgres_schema(schema_df)
                return [result_df, schema]
            case DatabaseType.Motherduck:
                result_df, schema_df = self._query_motherduck(query, datasource)
                schema = _process_motherduck_schema(schema_df)
                return [result_df, schema]
            case DatabaseType.Databricks:
                result, raw_schema = self._query_databricks(query, datasource)
                schema = process_databricks_schema(raw_schema)
                return [result, schema]
            case DatabaseType.Bigquery:
                result, raw_schema = self._query_bigquery(query, datasource)
                schema = process_bigquery_schema(raw_schema)
                return [result, schema]
            case DatabaseType.Snowflake:
                result, raw_schema = self._query_snowflake(query, datasource)
                schema = process_snowflake_schema(raw_schema)
                return [result, schema]
            case DatabaseType.Clickhouse:
                result, raw_schema = self._query_clickhouse(query, datasource)
                schema = process_clickhouse_schema(raw_schema)
                return [result, schema]
            case _:
                return "Unknown DatabaseType"

    def _query_snowflake(
        self, query: str, datasource: ElementDataSource
    ) -> tuple[pl.DataFrame, dict]:
        """
        Queries a Snowflake database.
        """
        return snowflake_datasource.query(query, datasource, self._get_database_details)

    def _query_clickhouse(
        self, query: str, datasource: ElementDataSource
    ) -> tuple[pl.DataFrame, tuple]:
        """
        Queries a Clickhouse database.

        Args:
            query (str): The query string.
            datasource (ElementDataSource): The datasource to execute the query on.

        Returns:
            tuple[pl.DataFrame, dict]: A tuple containing the resulting DataFrame and schema.
        """
        return clickhouse_datasource.query(
            query, datasource, self._get_database_details
        )

    def _query_databricks(
        self, query: str, datasource: ElementDataSource
    ) -> tuple[pl.DataFrame, dict]:
        """
        Queries a Databricks database.

        Args:
            query (str): The query string.
            datasource (ElementDataSource): The datasource to execute the query on.

        Returns:
            tuple[pl.DataFrame, dict]: A tuple containing the resulting DataFrame and schema.
        """
        return databricks_datasource.query(
            query, datasource, self._get_database_details
        )

    def _build_postgres_connection_string(self, parsed_credentials: dict) -> str:
        return postgres_datasource.build_connection_string(parsed_credentials)

    def _get_postgres_connection_string(self, datasource: ElementDataSource) -> str:
        return postgres_datasource.get_connection_string(
            datasource, self._get_database_details
        )

    def _query_postgres(
        self, query: str, datasource: ElementDataSource
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Queries a Postgres database using psycopg and returns the result along with a
        schema DataFrame describing the output columns.

        Args:
            query (str): The query string.
            datasource (ElementDataSource): The datasource to execute the query on.

        Returns:
            tuple[pl.DataFrame, pl.DataFrame]: The resulting Polars DataFrame and a
            schema DataFrame with column metadata.
        """
        return postgres_datasource.query(query, datasource, self._get_database_details)

    def _query_motherduck(
        self, query: str, datasource: ElementDataSource
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Queries a Motherduck database using DuckDB and returns the result along with a
        schema DataFrame describing the output columns.

        Args:
            query (str): The query string.
            datasource (ElementDataSource): The datasource to execute the query on.

        Returns:
            tuple[pl.DataFrame, pl.DataFrame]: The resulting Polars DataFrame and a
            schema DataFrame with column metadata.
        """
        return motherduck_datasource.query(
            query, datasource, self._get_database_details
        )

    def _query_bigquery(
        self, query: str, datasource: ElementDataSource
    ) -> pl.DataFrame:
        """
        Queries a Postgres database. Attaches the database to DuckDB and executes the
        query under the alias same as the database name.

        Args:
            query (str): The query string.
            datasource (ElementDataSource): The datasource to execute the query on.

        Returns:
            pl.DataFrame: The resulting DataFrame.
        """
        return bigquery_datasource.query(query, datasource, self._get_database_details)

    def _query_file(self, query: str, datasource: dict) -> pl.DataFrame:
        """
        Queries a file. Currently supports CSV and XLSX files only.

        Args:
            query (str): The query string.
            datasource (dict): The datasource to execute the query on.

        Returns:
            pl.DataFrame: The resulting DataFrame.
        """
        try:
            file_info = datasource["file_info"]
            file_id = file_info["file_id"]

            self.download_file(file_id=file_id)

            result = self._duckdb.conn.sql(query).pl()
            return result

        except KeyError as e:
            raise ValueError(f"Missing required information in datasource: {e}")
        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(
                    f"An error occurred while querying the file: {e}"
                )
            )

    def _query_file_with_schema(
        self, query: str, datasource: dict
    ) -> tuple[pl.DataFrame, dict]:
        """
        Queries a file with the schema included in the response. Currently supports CSV and XLSX files only.

        Args:
            query (str): The query string.
            datasource (dict): The datasource to execute the query on.

        Returns:
            tuple[pl.DataFrame, dict]: A tuple containing the resulting DataFrame and schema as a dict.
        """
        result = self._query_file(query, datasource)
        schema = _get_dataframe_schema(result)
        return [result, schema]

    def _query_dataframe(
        self, query: str, datasource: ElementDataSource
    ) -> pl.DataFrame:
        """
        Queries a DataFrame. Currently only supports Pandas and Polars DataFrames.

        Args:
            query (str): The query string.
            datasource (ElementDataSource): The datasource to execute the query on.

        Returns:
            pl.DataFrame: The resulting DataFrame.
        """
        dataframe_info = datasource.get("dataframe_info")

        if dataframe_info is None:
            raise ValueError("Invalid ElementDataSource")

        try:
            result = self._duckdb.conn.sql(query).pl()
        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(
                    f"An error occurred while querying the DataFrame: {e}"
                )
            )

        return result

    def _query_dataframe_with_schema(
        self, query: str, datasource: ElementDataSource
    ) -> tuple[pl.DataFrame, dict]:
        """
        Queries a DataFrame with the schema included in the response. Currently only supports Pandas and Polars DataFrames.

        Args:
            query (str): The query string.
            datasource (ElementDataSource): The datasource to execute the query on.

        Returns:
            tuple[pl.DataFrame, dict]: A tuple containing the resulting DataFrame and schema as a dict.
        """
        result = self._query_dataframe(query, datasource)
        schema = _get_dataframe_schema(result)
        return [result, schema]

    def _write_to_postgres(self, df: pl.DataFrame, save_config: DBSaveConfig):
        """
        Writes a DataFrame to a Postgres database using psycopg.

        Args:
            df (pl.DataFrame): The DataFrame to write to the database.
            save_config (DBSaveConfig): The save configuration.

        Returns:
            Error, Result and Metrics in a tuple
            Tuple[Result (Dict), Metrics (Dict), Error (str)]
        """
        return postgres_datasource.write_to_postgres(
            df, save_config, self._get_database_details
        )

    def _write_to_motherduck(self, df: pl.DataFrame, save_config: DBSaveConfig):
        """
        Writes a DataFrame to a Motherduck database using DuckDB.

        Args:
            df (pl.DataFrame): The DataFrame to write to the database.
            save_config (DBSaveConfig): The save configuration.

        Returns:
            Error, Result and Metrics in a tuple
            Tuple[Result (Dict), Metrics (Dict), Error (str)]
        """
        return motherduck_datasource.write_to_motherduck(
            df, save_config, self._get_database_details
        )

    def _write_to_bigquery(self, df: pl.DataFrame, save_config: DBSaveConfig):
        """
        Writes a DataFrame to a BigQuery database.

        Args:
            df (pl.DataFrame): The DataFrame to write to the database.
            save_config (DBSaveConfig): The save configuration.

        Returns:
            Error, Result and Metrics in a tuple
            Tuple[Result (Dict), Metrics (Dict), Error (str)]
        """
        return bigquery_datasource.write_to_bigquery(
            df, save_config, self._get_database_details
        )

    def _write_to_snowflake(self, df: pl.DataFrame, save_config: DBSaveConfig):
        """
        Writes a DataFrame to a Snowflake database.

        Args:
            df (pl.DataFrame): The DataFrame to write to the database.
            save_config (DBSaveConfig): The save configuration.

        Returns:
            Error, Result and Metrics in a tuple
            Tuple[Result (Dict), Metrics (Dict), Error (str)]
        """
        return snowflake_datasource.write_to_snowflake(
            df, save_config, self._get_database_details
        )

    def _write_to_clickhouse(self, df: pl.DataFrame, save_config: DBSaveConfig):
        """
        Writes a DataFrame to a Clickhouse database.

        Args:
            df (pl.DataFrame): The DataFrame to write to the database.
            save_config (DBSaveConfig): The save configuration.

        Returns:
            Error, Result and Metrics in a tuple
            Tuple[Result (Dict), Metrics (Dict), Error (str)]
        """
        return clickhouse_datasource.write_to_clickhouse(
            df, save_config, self._get_database_details
        )

    def _get_dataframe_schema(self, df: pl.DataFrame) -> List[Schema]:
        """
        Gets the schema of any given Polars DataFrame.

        Args:
            df (pl.DataFrame): The DataFrame to get the schema from.

        Returns:
            List[Schema]: The schema as a list of Schema objects.
        """
        schema = []

        if isinstance(df, pd.DataFrame):
            for column in df.columns:
                dtype = df[column].dtype
                if pd.api.types.is_numeric_dtype(dtype):
                    col_type = "NUMBER"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    col_type = "DATE"
                else:
                    col_type = "STRING"

                schema.append(
                    {"name": column, "livedocs_type": col_type, "children": []}
                )

        elif isinstance(df, pl.DataFrame):
            for column in df.columns:
                dtype = df[column].dtype
                if isinstance(
                    dtype,
                    (
                        pl.Int8,
                        pl.Int16,
                        pl.Int32,
                        pl.Int64,
                        pl.UInt8,
                        pl.UInt16,
                        pl.UInt32,
                        pl.UInt64,
                        pl.Float32,
                        pl.Float64,
                    ),
                ):
                    col_type = "NUMBER"
                elif isinstance(dtype, (pl.Date, pl.Datetime, pl.Time)):
                    col_type = "DATE"
                else:
                    col_type = "STRING"

                schema.append(
                    {"name": column, "livedocs_type": col_type, "children": []}
                )

        else:
            raise ValueError("Input must be a pandas DataFrame or a polars DataFrame")

        return json.dumps(schema, default=str, separators=(",", ":"))

    @_capture_exceptions
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

    @_capture_exceptions
    def download_file(
        self,
        file_name: Optional[str] = None,
        file_id: Optional[str] = None,
        force_download: bool = False,
        path: Optional[str] = os.getenv("LIVEDOCS_FILES_PATH"),
    ) -> str:
        """
        Downloads a file to a local path based on either its name or ID.

        Parameters:
            file_name (Optional[str]): The name of the file to download. Must be provided exclusively if file_id is not specified.
            file_id (Optional[str]): The unique identifier of the file to download. Must be provided exclusively if file_name is not specified.
            force_download (bool): If True, forces the file to be redownloaded and overwritten if it exists locally.
            path (Optional[str]): The directory path where the file will be stored.
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

        if not (file_name or file_id) or (file_name and file_id):
            raise ValueError("Exactly one of file_name or file_id must be provided.")

        if path is None:
            raise ValueError("Please provide a valid path to save the file.")

        os.makedirs(path, exist_ok=True)

        manifest_data = _fetch_file_manifest(
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
