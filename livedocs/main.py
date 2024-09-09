import json
import os
import tempfile
from typing import Dict, List

from jinja2 import Template
import pandas as pd

import polars as pl
import gzip
import base64
import requests

from IPython.display import display

from livedocs.manager.duckdb import DuckDBSingleton
from livedocs.types import (
    DatabaseType,
    ElementDataSource,
    ElementDatasourceType,
    LivedocsChartSpec,
    Schema,
)
from duckdb import CatalogException
from livedocs.utils.common import (
    _datetime_json_serializer,
    _fetch_credentials,
    _fetch_file_manifest,
    _get_dataframe_schema,
)
from livedocs.utils.postgres import (
    create_postgres_connection_url,
    process_postgres_schema,
)
from livedocs.vega import _get_altair_datasource_query, create_vega_spec

"""
This is initialized in the prelude cell of the notebook like this:
    
    livedocs = Livedocs()
    livedocs.initialize(report_id, session_token)

"""


class Livedocs:
    """
    On initialization this sets up everything that can be used by the library without
    having a report_id and token.
    """

    def __init__(self):
        self._duckdb = DuckDBSingleton()
        self._file_dir = tempfile.mkdtemp()
        self._file_manifests: Dict[str, str] = {}
        self._secrets = {}
        self.is_initialized = False

    """
    Called when the pod is initialized. Fetches the credentials and sets the 
    is_initialized flag to True. The /v1/credentials endpoint is called to fetch the
    DB connection credentials and secrets for the report.
    """

    def initialize(self, report_id: str, token: str) ->  tuple[object, dict]:
        self._report_id = report_id
        self._token = token
        self._credentials = _fetch_credentials(report_id, token)
        self.is_initialized = True

        secrets = self._credentials.get('workspace_secrets', {})
        secrets_dict = {key: secret_info['value'] for key, secret_info in secrets.items()}
        self._secrets = secrets_dict

    """
    Accessor for user-defined secrets. Use this like:

    livedocs.secrets('CLIENT_ID', 'default_value (optional)')

    """
    def secrets(self, key, default_value = "") -> str:
        if self._secrets.get(key):
            return self._secrets.get(key)
        else:
            result = _fetch_credentials(self._report_id, self._token)
            secrets = result.get('workspace_secrets', {})
            secrets_dict = {key: secret_info['value'] for key, secret_info in secrets.items()}
            self._secrets = secrets_dict
            return self._secrets.get(key, default_value)

    """
    Central query function. Give it a query and a datasource, and it will return 
    a Polars DataFrame. Simple. 
    """

    def query(self, query: str, str_datasource: str, context: dict) -> tuple[pl.DataFrame, str]:
        datasource: ElementDataSource = json.loads(str_datasource)
        final_query = self.add_jinja_vars(query, context)

        df: pl.DataFrame = pl.DataFrame()
        (df, schema) = self._query_with_schema(final_query, datasource)

        json_string = json.dumps(df.to_dicts(), default=_datetime_json_serializer)
        compressed = gzip.compress(json_string.encode('utf-8'))
        encoded = base64.b64encode(compressed).decode('ascii')
        return (df, encoded)
    

    """
    Plugs Jinja variables into a raw HTML string for a text element
    """
    def process_raw_text(self, str_src: str, context: dict) -> str:
        src = json.loads(str_src)
        return self.add_jinja_vars(src["html"], context)

   
    """
    Adds the local variables to the query. 
    """

    def add_jinja_vars(self, text: str, context: dict) -> str:
        template = Template(text)
        return template.render(context)

    """
    Gets a Vega spec for a given datasource and settings. 
    """

    def _get_vega_spec(
        self, settings_str: str, datasource_str: str
    ) -> dict:
        settings: LivedocsChartSpec = json.loads(settings_str)
        datasource: ElementDataSource = json.loads(datasource_str)

        display("_get_vega_spec")

        results: tuple[pl.DataFrame, dict] = self._query_with_schema(
            _get_altair_datasource_query(datasource), datasource
        )

        vega_spec_json_str = create_vega_spec(results[0], settings, results[1])
        compressed = gzip.compress(vega_spec_json_str.encode('utf-8'))
        encoded = base64.b64encode(compressed).decode('ascii')
        
        return encoded


    """
    Gets a polars table for a given datasource. 
    """

    def _get_table_response(self, str_datasource: ElementDataSource) -> pl.DataFrame:
        datasource: ElementDataSource = json.loads(str_datasource)
        results: tuple[pl.DataFrame, dict] = self._query_with_schema(
            _get_altair_datasource_query(datasource), datasource
        )

        (df, schema) = results

        json_string = json.dumps(df.to_dicts(), default=_datetime_json_serializer)
        compressed = gzip.compress(json_string.encode('utf-8'))
        encoded = base64.b64encode(compressed).decode('ascii')
        return encoded

    """
    Query a database and return the result as a DataFrame with schema. Currently only supports Postgres. 
    """

    def _query_with_schema(
        self, query: str, datasource: ElementDataSource
    ) -> tuple[pl.DataFrame, dict]:
        match ElementDatasourceType(datasource["source_type"]):
            case ElementDatasourceType.database | ElementDatasourceType.database_table:
                return self._query_database_with_schema(query, datasource)
            case ElementDatasourceType.file:
                return self._query_file_with_schema(query, datasource)
            case ElementDatasourceType.dataframe:
                return self._query_dataframe_with_schema(query, datasource)
            case _:
                return "Unknown ElementDataSource"

    """
    Query a database and return the result as a DataFrame with schema. Currently only supports Postgres. 
    """

    def _query_database_with_schema(
        self, query: str, datasource: ElementDataSource
    ) -> tuple[pl.DataFrame, dict]:
        match DatabaseType(datasource["database_info"]["database_type"]):
            case DatabaseType.Postgres:
                # Get the schema directly from the query
                schema_query = f"DESCRIBE {query}"
                _schema = self._query_database(schema_query, datasource)
                schema = process_postgres_schema(_schema)
                
                # Execute the original query
                result = self._query_database(query, datasource)
                return [result, schema]
            case _:
                return "Unknown DatabaseType"

    """
    Query a database. Currently only supports Postgres. 
    """

    def _query_database(
        self, query: str, datasource: ElementDataSource
    ) -> pl.DataFrame:
        match DatabaseType(datasource["database_info"]["database_type"]):
            case DatabaseType.Postgres:
                return self._query_postgres(query, datasource)
            case _:
                return "Unknown DatabaseType"

    """
    Query a Postgres database. Attaches the database to DuckDB and executes the 
    query under the alias same as the database name.
    """

    def _query_postgres(
        self, query: str, datasource: ElementDataSource
    ) -> pl.DataFrame:
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
            parsed_credentials = json.loads(credentials["connection_details"])
        except json.JSONDecodeError as e:
            raise ValueError(f"Error parsing connection details: {e}")

        try:
            connection_string = create_postgres_connection_url(parsed_credentials)
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
        except CatalogException as e:
            raise RuntimeError("CatalogError: Tablename should be in format 'DatabaseName.Schema.TableName' (schema is probably 'public')")
        except Exception as e:
            raise RuntimeError(f"Error executing query: {e}")

        return result

    """
    Query a file. Currently supports CSV and XLSX files only. 
    """

    def _query_file(self, query: str, datasource: dict) -> pl.DataFrame:
        try:
            file_info = datasource["file_info"]
            file_id = file_info["file_id"]
            file_type = file_info["file_type"]
            file_name = file_info["file_name"]

            temp_file_path = os.path.join(self._file_dir, f"{file_name}")

            if not os.path.exists(temp_file_path):
                signed_url = self._get_signed_url(file_id)
                self._download_file(signed_url, temp_file_path)
                
            if file_type == "csv":
                query_with_path = query.replace(
                    file_name, f"read_csv_auto('{temp_file_path}')"
                )
            elif file_type in ["xls", "xlsx"]:
                sheet_name = file_info.get("layer_name", "Sheet1")
                query_with_path = query.replace(
                    file_name, f"st_read('{temp_file_path}', layer='{sheet_name}')"
                )
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            result = self._duckdb.conn.sql(query_with_path).pl()
            return result

        except KeyError as e:
            raise ValueError(f"Missing required information in datasource: {e}")
        except Exception as e:
            raise RuntimeError(f"An error occurred while querying the file: {e}")

    """
    Query a file with the schema included in the response. Currently supports CSV and XLSX files only. 
    """

    def _query_file_with_schema(
        self, query: str, datasource: dict
    ) -> tuple[pl.DataFrame, dict]:
        result = self._query_file(query, datasource)
        schema = _get_dataframe_schema(result)
        return [result, schema]

    """
    Query a DataFrame. Currently only supports Pandas and Polars DataFrames. 
    """

    def _query_dataframe(
        self, query: str, datasource: ElementDataSource
    ) -> pl.DataFrame:
        dataframe_info = datasource.get("dataframe_info")
        display("_query_dataframe")

        if dataframe_info is None:
            raise ValueError("Invalid ElementDataSource")

        try:
            result = self._duckdb.conn.sql(query).pl()
        except Exception as e:
            raise RuntimeError(f"An error occurred while querying the DataFrame: {e}")

        return result

    """
    Query a DataFrame with the schema included in the response. Currently only supports Pandas and Polars DataFrames.
    """

    def _query_dataframe_with_schema(
        self, query: str, datasource: ElementDataSource
    ) -> tuple[pl.DataFrame, dict]:
        result = self._query_dataframe(query, datasource)
        schema = _get_dataframe_schema(result)
        return [result, schema]

    """
    Fetches a signed URL from the /v1/manifest endpoint for a file and returns it. 
    It also stores the signed URL in a dictionary for future use. 
    """

    def _get_signed_url(self, file_id: str) -> str:
        if file_id in self._file_manifests:
            return self._file_manifests[file_id]
        else:
            manifest = _fetch_file_manifest(file_id, self._report_id, self._token)
            self._file_manifests[file_id] = manifest["signed_url"]
            return manifest["signed_url"]

    def _download_file(self, signed_url, file_path):
        response = requests.get(signed_url)
        response.raise_for_status()
        with open(file_path, "wb") as f:
            f.write(response.content)

    """
    Get the schema of any given dataframe
    """
    def _get_dataframe_schema(self, df: pl.DataFrame) -> List[Schema]:
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
                
                schema.append({
                    "name": column,
                    "livedocs_type": col_type,
                    "children": []
                })
        
        elif isinstance(df, pl.DataFrame):
            for column in df.columns:
                dtype = df[column].dtype
                if isinstance(dtype, (pl.Int8, pl.Int16, pl.Int32, pl.Int64, 
                      pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, 
                      pl.Float32, pl.Float64)):
                    col_type = "NUMBER"
                elif isinstance(dtype, (pl.Date, pl.Datetime, pl.Time)):
                    col_type = "DATE"
                else:
                    col_type = "STRING"
                
                schema.append({
                    "name": column,
                    "livedocs_type": col_type,
                    "children": []
                })
        
        else:
            raise ValueError("Input must be a pandas DataFrame or a polars DataFrame")
        
        return json.dumps(schema, default=str)
