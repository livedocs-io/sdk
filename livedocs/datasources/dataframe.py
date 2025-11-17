from __future__ import annotations

from typing import Any, Callable

import polars as pl

from livedocs.datasources.base import BaseDatasourceConnector
from livedocs.types import ElementDataSource
from livedocs.utils.common import _get_dataframe_schema
from livedocs.utils.lib.internals import (
    livedocs_internal_sanitize_sensitive_data as sanitize_sensitive_data,
)


class DataframeDatasourceConnector(BaseDatasourceConnector):
    """
    Datasource connector for DataFrame-based datasources.
    """

    def read(
        self,
        query: str,
        datasource: ElementDataSource,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
        **kwargs: Any,
    ) -> tuple[pl.DataFrame, dict[str, str]]:
        """
        Execute a query against a DataFrame datasource.

        Args:
            query: SQL query string to execute (DuckDB query)
            datasource: Datasource configuration
            get_database_details: Not used for dataframe datasources (required by interface)
            **kwargs: Additional arguments:
                - duckdb_conn: DuckDB connection instance (required)
                - register_dataframe: Function to register a dataframe in DuckDB (df_name, dataframe) (optional)

        Returns:
            Tuple containing:
            - DataFrame with query results
            - Schema information as a dict mapping column names to Livedocs types
        """
        duckdb_conn = kwargs.get("duckdb_conn")

        if duckdb_conn is None:
            raise ValueError("DuckDB connection is required for dataframe datasources")

        try:
            dataframe_info = datasource.get("dataframe_info")
            if dataframe_info is None:
                raise ValueError("Missing required information: 'dataframe_info'")

            # Note: Dataframe should be registered before calling this method
            # The registration happens in the manager/calling code

            result = duckdb_conn.sql(query).pl()
            schema = _get_dataframe_schema(result)
            return result, schema

        except KeyError as e:
            raise ValueError(f"Missing required information in datasource: {e}")
        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(
                    f"An error occurred while querying the DataFrame: {e}"
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
        DataFrame datasources are read-only. Writing is not supported.

        Raises:
            NotImplementedError: DataFrame datasources do not support write operations
        """
        raise NotImplementedError(
            "DataFrame datasources are read-only and do not support write operations"
        )

    def teardown(self) -> None:
        """No cleanup needed for dataframe datasources."""
        pass
