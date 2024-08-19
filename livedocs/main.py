import json
import os
import tempfile
from typing import Dict, List

import polars as pl
import requests

from livedocs.manager.duckdb import DuckDBSingleton
from livedocs.types import (
    DatabaseType,
    ElementDataSource,
    ElementDatasourceType,
    LivedocsChartSpec,
    Schema,
)
from livedocs.utils.common import (
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
        self.is_initialized = False

    """
    Called when the pod is initialized. Fetches the credentials and sets the 
    is_initialized flag to True. The /v1/credentials endpoint is called to fetch the
    DB connection credentials and secrets for the report.
    """

    def initialize(self, report_id: str, token: str):
        self._report_id = report_id
        self._token = token
        print(report_id, token)
        self._credentials = _fetch_credentials(report_id, token)
        self.is_initialized = True

    """
    Central query function. Give it a query and a datasource, and it will return 
    a Polars DataFrame. Simple. 
    """

    def query(self, query: str, datasource: ElementDataSource) -> pl.DataFrame:
        match ElementDatasourceType(datasource["source_type"]):
            case ElementDatasourceType.database | ElementDatasourceType.database_table:
                return self._query_database(query, datasource)
            case ElementDatasourceType.file:
                return self._query_file(query, datasource)
            case ElementDatasourceType.dataframe:
                return self._query_dataframe(query, datasource)
            case _:
                return "Unknown ElementDataSource"

    """
    Gets a Vega spec for a given datasource and settings. 
    """

    def _get_vega_spec(
        self, settings: LivedocsChartSpec, datasource: ElementDataSource
    ) -> dict:
        results: tuple[pl.DataFrame, dict] = self._query_with_schema(
            _get_altair_datasource_query(datasource), datasource
        )
        return create_vega_spec(results[0], settings, results[1])

    """
    Gets a polars table for a given datasource. 
    """

    def _get_table_response(self, datasource: ElementDataSource) -> pl.DataFrame:
        results: tuple[pl.DataFrame, dict] = self._query_with_schema(
            _get_altair_datasource_query(datasource), datasource
        )
        return results[0]

    """
    Query a database and return the result as a DataFrame with schema. Currently only supports Postgres. 
    """

    def _query_with_schema(
        self, query: str, datasource: ElementDataSource
    ) -> tuple[pl.DataFrame, dict]:
        match ElementDatasourceType(datasource["source_type"]):
            case ElementDatasourceType.database_table:
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
                result = self._query_database(query, datasource)

                schema_query = f"""
                    SELECT 
                        column_name, 
                        udt_name
                    FROM 
                        information_schema.columns
                    WHERE 
                        table_name = '{datasource["database_table_info"]["table_name"]}'
                        AND table_schema = '{datasource['database_table_info']['schema_name']}'
                """

                _schema = self._query_database(schema_query, datasource)
                schema = process_postgres_schema(_schema)
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
    Get the schema of any given data source.
    """

    def _get_dataframe_schema(self, datasource: ElementDataSource) -> List[Schema]:
        schema_query = """SELECT column_name as name,
                CASE 
                    WHEN data_type IN ('INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT') THEN 'NUMBER'
                    WHEN data_type IN ('DATE', 'TIMESTAMP') THEN 'DATE'
                    ELSE 'STRING'
                END as type
            FROM information_schema.columns
            WHERE table_name = 'df'
            ORDER BY column_index"""
        raw_schema = self._query_dataframe(schema_query, datasource)
        return [
            {"name": name, "type": type, "children": []} for name, type in raw_schema
        ]
