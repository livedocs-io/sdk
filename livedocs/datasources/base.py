from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable

import polars as pl

from livedocs.types import DBSaveConfig, ElementDataSource, LivedocsResult


class BaseDatasourceConnector(ABC):
    """
    Abstract base class for datasource connectors.

    All datasource connectors must extend this class and implement the required methods.
    This ensures type safety and consistency across all datasource implementations.

    The constructor signature is enforced - all subclasses must accept the same parameters.
    """

    def __init__(self, mock: bool = False) -> None:
        """
        Initialize the datasource connector.

        Args:
            mock: If True, use mock/test mode. Defaults to False.
                  This parameter is used for testing and should be supported by all connectors.

        Raises:
            TypeError: If subclass doesn't call super().__init__() with the required parameters.
        """
        self.mock: bool = mock
        self._initialized: bool = True

    @abstractmethod
    def read(
        self,
        query: str,
        datasource: ElementDataSource,
        get_database_details: Callable[[str], tuple[object, dict[str, str]]],
    ) -> tuple[pl.DataFrame, pl.DataFrame | object]:
        """
        Execute a query against the datasource.

        Args:
            query: SQL query string to execute
            datasource: Datasource configuration
            get_database_details: Callable to retrieve database credentials

        Returns:
            Tuple containing:
            - DataFrame with query results
            - Schema information (either DataFrame or schema object)
        """
        pass

    @abstractmethod
    def write(
        self,
        df: pl.DataFrame,
        save_config: DBSaveConfig,
        get_database_details: Callable[[str], tuple[object, dict[str, str]]],
    ) -> LivedocsResult:
        """
        Write a DataFrame to the datasource.

        Args:
            df: DataFrame to write
            save_config: Configuration for saving to database
            get_database_details: Callable to retrieve database credentials

        Returns:
            LivedocsResult containing the write operation result
        """
        pass

    @abstractmethod
    def teardown(self) -> None:
        """
        Clean up resources and close connections.

        This method should be called when the connector is no longer needed.
        It should close any open connections, release resources, and perform cleanup.
        """
        pass


__all__ = ["BaseDatasourceConnector"]
