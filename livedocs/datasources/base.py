from __future__ import annotations

from typing import Protocol, Tuple

import polars as pl

from livedocs.types import ElementDataSource


class DatasourceQuery(Protocol):
    def __call__(self, query: str, datasource: ElementDataSource) -> pl.DataFrame: ...


class DatasourceQueryWithSchema(Protocol):
    def __call__(
        self, query: str, datasource: ElementDataSource
    ) -> Tuple[pl.DataFrame, pl.DataFrame]: ...
