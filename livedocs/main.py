import os
import json
import requests
import tempfile
from typing import Dict

from livedocs.utils.common import _fetch_credentials, _fetch_file_manifest
import polars as pl

from livedocs.manager.duckdb import DuckDBSingleton
from livedocs.types import (
    DatabaseType,
    ElementDataSource,
    ElementDatasourceType,
)
from livedocs.utils.postgres import create_postgres_connection_url

"""
This is initialized in the prelude cell of the notebook like this:
    
    livedocs = Livedocs(report_id, session_token)

"""


class Livedocs:
    """
    On initialization this calls the /v1/credentials endpoint to fetch the
    DB connection credentials and secrets for the report.
    """

    def __init__(self, report_id: str, token: str):
        self._duckdb = DuckDBSingleton()
        self._report_id = report_id
        self._token = token
        self._file_dir = tempfile.mkdtemp()
        self._credentials = _fetch_credentials(report_id, token)
        self._file_manifests: Dict[str, str] = {}

    """
    Central query function. Give it a query and a datasource, and it will return 
    a Polars DataFrame. Simple. 
    """

    def query(self, query: str, datasource: ElementDataSource) -> pl.DataFrame:
        match datasource["sourceType"]:
            case ElementDatasourceType.database:
                return self._query_database(query, datasource)
            case ElementDatasourceType.file:
                return self._query_file(query, datasource)
            case ElementDatasourceType.dataframe:
                return self._query_dataframe(query, datasource)
            case ElementDatasourceType.database_table:
                return "db table result"
            case _:
                return "unknown result"

    """
    Query a database. Currently only supports Postgres. 
    """

    def _query_database(
        self, query: str, datasource: ElementDataSource
    ) -> pl.DataFrame:
        match datasource["databaseInfo"]["database_type"]:
            case DatabaseType.Postgres:
                return self._query_postgres(query, datasource)
            case _:
                return "unknown result"

    """
    Query a Postgres database. Attaches the database to DuckDB and executes the 
    query under the alias same as the database name.
    """

    def _query_postgres(
        self, query: str, datasource: ElementDataSource
    ) -> pl.DataFrame:
        try:
            db_connector_id = datasource["databaseInfo"]["database_connector_id"]
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

        # This is to prevent a potential conflict with aliases in the database
        alias = parsed_credentials["database"] + "_" + db_connector_id.replace("-", "")

        try:
            self._duckdb.attach_postgres(connection_string, alias)
        except Exception as e:
            raise RuntimeError(f"Error attaching PostgreSQL database: {e}")

        # Replace table names with full table name
        full_table_name = query.replace("FROM ", f"FROM {alias}.").replace(
            "from ", f"from {alias}."
        )

        try:
            result = self._duckdb.conn.sql(
                f"SELECT * FROM ({full_table_name}) AS subquery"
            ).pl()
        except Exception as e:
            raise RuntimeError(f"Error executing query: {e}")

        return result

    """
    Query a file. Currently supports CSV and XLSX files only. 
    """

    def _query_file(self, query: str, datasource: dict) -> pl.DataFrame:
        try:
            file_info = datasource["fileInfo"]
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
    Query a DataFrame. Currently only supports Pandas and Polars DataFrames. 
    """

    def _query_dataframe(
        self, query: str, datasource: ElementDataSource
    ) -> pl.DataFrame:
        dataframe_info = datasource.get("dataframeInfo")
        if dataframe_info is None:
            raise ValueError("Invalid ElementDataSource")

        try:
            result = self._duckdb.conn.sql(query).pl()
        except Exception as e:
            raise RuntimeError(f"An error occurred while querying the DataFrame: {e}")

        return result

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

    # def run_chart(self, config, data):
    #     chart_config = self.chart_generator.generate_highcharts_config(
    #         config=config, data=data
    #     )
    #     return jsonify(chart_config)
