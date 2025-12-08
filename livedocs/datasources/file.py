from __future__ import annotations

from typing import Any, Callable

import polars as pl

from livedocs.datasources.base import BaseDatasourceConnector
from livedocs.types import ElementDataSource
from livedocs.utils.common import _get_dataframe_schema
from livedocs.utils.lib.internals import (
    livedocs_internal_sanitize_sensitive_data as sanitize_sensitive_data,
)


class FileDatasourceConnector(BaseDatasourceConnector):
    """
    Datasource connector for file-based datasources (CSV, XLSX).
    """

    def read(
        self,
        query: str,
        datasource: ElementDataSource,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
        **kwargs: Any,
    ) -> tuple[pl.DataFrame, dict[str, str]]:
        """
        Execute a query against a file datasource.

        Args:
            query: SQL query string to execute (DuckDB query)
            datasource: Datasource configuration
            get_database_details: Not used for file datasources (required by interface)
            **kwargs: Additional arguments:
                - duckdb_conn: DuckDB connection instance (required)
                - download_file: Function to download file by file_id (required)

        Returns:
            Tuple containing:
            - DataFrame with query results
            - Schema information as a dict mapping column names to Livedocs types
        """
        duckdb_conn = kwargs.get("duckdb_conn")
        download_file = kwargs.get("download_file")

        if duckdb_conn is None:
            raise ValueError("DuckDB connection is required for file datasources")
        if download_file is None:
            raise ValueError("download_file function is required for file datasources")

        try:
            file_info = datasource.get("file_info")
            if file_info is None:
                raise ValueError("Missing required information: 'file_info'")

            # Check if file_path was already provided in kwargs (Case 2: preview scenario)
            # If so, skip download as file was already downloaded
            if kwargs.get("file_path") is None:
                file_id = file_info["file_id"]
                download_file(file_id=file_id)

            result = duckdb_conn.sql(query).pl()
            schema = _get_dataframe_schema(result)
            return result, schema

        except KeyError as e:
            raise ValueError(f"Missing required information in datasource: {e}")
        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(
                    f"An error occurred while querying the file: {e}"
                )
            )

    def write(
        self,
        df: pl.DataFrame,
        save_config: Any,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
    ) -> Any:
        """
        File datasources are read-only. Writing is not supported.

        Raises:
            NotImplementedError: File datasources do not support write operations
        """
        raise NotImplementedError(
            "File datasources are read-only and do not support write operations"
        )

    def teardown(self) -> None:
        """No cleanup needed for file datasources."""
        pass
