import base64
import gzip
import json
import os
from typing import List, Optional
import snowflake.connector
import pandas as pd
from livedocs.utils.snowflake import process_snowflake_schema
import polars as pl
import sentry_sdk
from google.cloud import bigquery
from google.oauth2 import service_account
from jinja2 import Template
from livedocs.cache import QueryCache
from livedocs.manager.duckdb import DuckDBSingleton
from livedocs.types import (
    CacheInfo,
    CacheStatus,
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
)
from livedocs.utils.bigquery import process_bigquery_schema, write_df_to_bigquery
from livedocs.utils.common import (
    _LIVEDOCS_PROTECTED_VARS,
    _capture_exceptions,
    _download_file,
    _fetch_credentials,
    _fetch_file_manifest,
    _get_dataframe_schema,
    _persist_built_in_vars,
    _setup_dirs,
    _setup_sentry,
    get_run_context,
)
from livedocs.utils.postgres import (
    _create_postgres_connection_url,
    _process_postgres_schema,
    _write_df_to_postgres,
)
from livedocs.utils.serialize import serializer
from livedocs.utils.single_value_helpers import process_single_value
from livedocs.utils.table_helpers import apply_table_operations
from livedocs.vega import create_vega_spec, get_altair_datasource_query

class Livedocs:
    """
    Main class for the Livedocs library. Handles initialization, querying, and data processing.
    This is initialized in the prelude cell of the notebook like this:

    livedocs = Livedocs()
    livedocs.initialize(report_id, session_token)
    """

    def __init__(self):
        """
        Creates the Livedocs instance, setting up necessary components and configurations.
        """
        _setup_sentry()
        _setup_dirs()

        self._duckdb = DuckDBSingleton(
            file_search_path=[os.getenv("LIVEDOCS_FILES_PATH")]
        )
        self._secrets = {}
        self._built_in_vars = {}
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
            self._credentials = _fetch_credentials(report_id, token)
            span.finish()
            self.is_initialized = True

            secrets = self._credentials.get("workspace_secrets", {})
            secrets_dict = {
                key: secret_info["value"] for key, secret_info in secrets.items()
            }
            self._secrets = secrets_dict
            self._built_in_vars = {**self._credentials.get("built_in_vars", "{}")}
            self._query_cache = QueryCache(report_id=report_id, token=token)

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

        return self._secrets.get(key, default_value)

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
            elif DatabaseType(save_config["database_type"]) == DatabaseType.Bigquery:
                current_run_context = get_run_context()
                if current_run_context in save_config["run_settings"]:
                    result = self._write_to_bigquery(dataframe, save_config)
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

    def add_jinja_vars(self, text: str, context: dict) -> str:
        """
        Adds Jinja variables to the given text.

        Args:
            text (str): The text to process.
            context (dict): The context for Jinja variables.

        Returns:
            str: The processed text with Jinja variables.
        """
        template = Template(text)
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
        self, settings_str: str, datasource_str: str, dataframe=None, use_cache=True
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
            df, schema, cache_info = self._query_with_schema(
                get_altair_datasource_query(datasource),
                datasource,
                dataframe,
                use_cache,
            )
            query_span.finish()

            # Vegafusion
            vega_span = sentry_sdk.start_span(name="run create_vega_spec (vegafusion)")
            vega_spec_json_str = create_vega_spec(df, settings, schema, cache_info)
            vega_span.finish()

            # Post-process the results
            post_span = sentry_sdk.start_span(name="post-processing")
            compressed = gzip.compress(vega_spec_json_str.encode("utf-8"))
            encoded = base64.b64encode(compressed).decode("ascii")
            post_span.finish()

            return encoded

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
            if datasource["source_type"] == "database_table" and DatabaseType(datasource["database_info"]["database_type"]) == DatabaseType.Snowflake:
                try:
                    db_connector_id = datasource["database_info"]["database_connector_id"]
                    credentials = self._credentials.get("databases", {}).get(db_connector_id)

                    if not credentials:
                        self._credentials = _fetch_credentials(self._report_id, self._token)
                        credentials = self._credentials["databases"][db_connector_id]
                except KeyError as e:
                    raise ValueError(f"Missing required information: {e}")

                try:
                    parsed_credentials = json.loads(credentials["connection_details"])
                    query = f'SELECT * FROM "{parsed_credentials["database"]}"."{datasource["database_table_info"]["schema_name"]}"."{datasource["database_table_info"]["table_name"]}"'
                except json.JSONDecodeError as e:
                    raise ValueError(f"Error parsing connection details: {e}")

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
                    if DatabaseType(datasource["database_info"]["database_type"]) == DatabaseType.Bigquery:
                        query = f"SELECT * FROM {datasource['database_table_info']['schema_name']}.{datasource['database_table_info']['table_name']} LIMIT 10"
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
            post_span.finish()

            return encoded

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
                _schema = self._query_postgres(f"DESCRIBE {query}", datasource)
                schema = _process_postgres_schema(_schema)
                result = self._query_postgres(query, datasource)
                return [result, schema]
            case DatabaseType.Bigquery:
                result, raw_schema = self._query_bigquery(query, datasource)
                schema = process_bigquery_schema(raw_schema)
                return [result, schema]
            case DatabaseType.Snowflake:
                result, raw_schema = self._query_snowflake(query, datasource)
                schema = process_snowflake_schema(raw_schema)
                return [result, schema]
            case _:
                return "Unknown DatabaseType"
            
    def _query_snowflake(self, query: str, datasource: ElementDataSource) -> tuple[pl.DataFrame, dict]:
        """
        Queries a Snowflake database.
        """
        try:
            db_connector_id = datasource["database_info"]["database_connector_id"]
            credentials = self._credentials.get("databases", {}).get(db_connector_id)

            if not credentials:
                self._credentials = _fetch_credentials(self._report_id, self._token)
                credentials = self._credentials["databases"][db_connector_id]
        except KeyError as e:
            raise ValueError(f"Missing required information: {e}")

        try:
            parsed_credentials = json.loads(credentials["connection_details"])
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing connection details: {e}")

        try:
            if parsed_credentials.get("auth_type") == "username_password":
                connection = snowflake.connector.connect(
                    user=parsed_credentials["username"],
                    password=parsed_credentials["password"],
                    account=parsed_credentials["host"],
                    database=parsed_credentials["database"],
                    session_parameters={
                        'QUERY_TAG': 'LivedocsQuery',
                    }
                )
            else:
                raise ValueError("Unsupported authentication type")
            
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            
            # Convert results to Polars DataFrame
            if result:
                # Get column names from cursor description
                columns = [desc[0] for desc in cursor.description]
                # Create DataFrame from results and column names
                df = pl.DataFrame(result, schema=columns)
            else:
                # Handle empty result set
                df = pl.DataFrame()
                
            return df, cursor.description
        
        except Exception as e:
            raise RuntimeError(f"Error querying Snowflake: {e}")
        finally:
            if 'connection' in locals():
                connection.close()


    def _query_postgres(
        self, query: str, datasource: ElementDataSource
    ) -> pl.DataFrame:
        """
        Queries a Postgres database. Attaches the database to DuckDB and executes the
        query under the alias same as the database name.

        Args:
            query (str): The query string.
            datasource (ElementDataSource): The datasource to execute the query on.

        Returns:
            pl.DataFrame: The resulting Polars DataFrame.
        """
        try:
            db_connector_id = datasource["database_info"]["database_connector_id"]
            credentials = self._credentials.get("databases", {}).get(db_connector_id)

            if not credentials:
                self._credentials = _fetch_credentials(self._report_id, self._token)
                credentials = self._credentials["databases"][db_connector_id]
        except KeyError as e:
            raise ValueError(f"Missing required information: {e}")

        try:
            parsed_credentials = json.loads(credentials["connection_details"])
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing connection details: {e}")

        try:
            if parsed_credentials.get("connect_using") == "url":
                connection_string = parsed_credentials["connection_url"]
            else:
                connection_string = _create_postgres_connection_url(parsed_credentials)
        except KeyError as e:
            raise ValueError(f"Missing required database connection detail: {e}")

        # This is unique to the workspace, so no chance of conflict
        alias = credentials["db_name"]

        try:
            self._duckdb.attach_postgres(connection_string, alias)
        except Exception as e:
            raise RuntimeError(f"Error attaching PostgreSQL database: {e}")

        try:
            result = self._duckdb.conn.sql(query).pl()
        except Exception as e:
            raise RuntimeError(f"Error executing query: {e}")

        return result

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
        try:
            db_connector_id = datasource["database_info"]["database_connector_id"]
            # This won't throw an error if the credentials are not found
            credentials = self._credentials.get("databases", {}).get(db_connector_id)

            if not credentials:
                self._credentials = _fetch_credentials(self._report_id, self._token)
                # This will throw an error if the credentials are not found
                credentials = self._credentials["databases"][db_connector_id]
        except KeyError as e:
            raise ValueError(f"Missing required information: {e}")

        try:
            # Needs to be parsed twice
            outer_parsed = json.loads(credentials["connection_details"])
            service_account_parsed = json.loads(outer_parsed["service_account_key"])
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing connection details: {e}")

        try:
            # Setup: Create BigQuery client with the given credentials
            credentials = service_account.Credentials.from_service_account_info(
                service_account_parsed
            )
            client = bigquery.Client(
                credentials=credentials, project=outer_parsed["project_id"]
            )

            # Execute query
            query_job = client.query(query)

            # Get schema before fetching results
            schema = query_job.result().schema

            # Convert query results to a DataFrame pointer
            df_pointer = query_job.to_dataframe(create_bqstorage_client=True)
            df_polars = pl.from_pandas(df_pointer)
        except Exception as e:
            raise RuntimeError(f"Error querying BigQuery: {e}")

        return df_polars, schema

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
            raise RuntimeError(f"An error occurred while querying the file: {e}")

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
            raise RuntimeError(f"An error occurred while querying the DataFrame: {e}")

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
        Writes a DataFrame to a Postgres database. Attaches the database to DuckDB and executes the
        write operation under the alias same as the database name.

        Args:
            df (pl.DataFrame): The DataFrame to write to the database.
            save_config (DBSaveConfig): The save configuration.

        Returns:
            Error, Result and Metrics in a tuple
            Tuple[Result (Dict), Metrics (Dict), Error (str)]
        """
        try:
            db_connector_id = save_config["database_id"]
            # This won't throw an error if the credentials are not found
            credentials = self._credentials.get("databases", {}).get(db_connector_id)

            if not credentials:
                self._credentials = _fetch_credentials(self._report_id, self._token)
                # This will throw an error if the credentials are not found
                credentials = self._credentials["databases"][db_connector_id]
        except KeyError as e:
            raise ValueError(f"Missing required information: {e}")

        try:
            parsed_credentials = json.loads(credentials["connection_details"])
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing connection details: {e}")

        try:
            if parsed_credentials.get("connect_using") == "url":
                connection_string = parsed_credentials["connection_url"]
            else:
                connection_string = _create_postgres_connection_url(parsed_credentials)
        except KeyError as e:
            raise ValueError(f"Missing required database connection detail: {e}")

        # This is unique to the workspace, so no chance of conflict
        alias = credentials["db_name"]

        try:
            self._duckdb.attach_postgres(connection_string, alias)
        except Exception as e:
            raise RuntimeError(f"Error attaching PostgreSQL database: {e}")

        try:
            qualified_table_name = f"{save_config['database_name']}.{save_config['schema_name']}.{save_config['table_name']}"
            result = _write_df_to_postgres(
                df,
                self._duckdb.conn,
                qualified_table_name,
                save_config["table_is_new"],
                save_config["write_mode"],
            )

            if result["error"]:
                raise RuntimeError(f"Error writing to PostgreSQL: {result['error']}")
            else:
                # Compress and encode response
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
            raise RuntimeError(f"DBSave Error: {e}")

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
        try:
            db_connector_id = save_config["database_id"]
            # This won't throw an error if the credentials are not found
            credentials = self._credentials.get("databases", {}).get(db_connector_id)

            if not credentials:
                self._credentials = _fetch_credentials(self._report_id, self._token)
                # This will throw an error if the credentials are not found
                credentials = self._credentials["databases"][db_connector_id]
        except KeyError as e:
            raise ValueError(f"Missing required information: {e}")

        try:
            # Needs to be parsed twice
            outer_parsed = json.loads(credentials["connection_details"])
            service_account_parsed = json.loads(outer_parsed["service_account_key"])
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing connection details: {e}")

        try:
            qualified_table_name = f"{outer_parsed['project_id']}.{save_config['schema_name']}.{save_config['table_name']}"

            credentials = service_account.Credentials.from_service_account_info(
                service_account_parsed
            )
            client = bigquery.Client(
                credentials=credentials, project=outer_parsed["project_id"]
            )

            result = write_df_to_bigquery(
                df,
                client,
                qualified_table_name,
                save_config["table_is_new"],
                save_config["write_mode"],
            )

            if result["error"]:
                raise RuntimeError(f"Error writing to BigQuery: {result['error']}")
            else:
                # Compress and encode response
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
            raise RuntimeError(f"DBSave Error: {e}")

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
