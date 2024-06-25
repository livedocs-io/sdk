from typing import Optional, TypedDict
from enum import Enum


class DatabaseType(Enum):
    Bigquery = "Bigquery"
    Clickhouse = "Clickhouse"
    Mysql = "Mysql"
    Postgres = "Postgres"
    Redshift = "Redshift"
    Snowflake = "Snowflake"
    files = "files"


class ElementDatasourceType(Enum):
    database = "database"
    database_table = "database_table"
    dataframe = "dataframe"
    file = "file"
    managed = "managed"


class DatabaseInfo(TypedDict):
    database_connector_id: str
    database_name: str
    database_type: DatabaseType


class DatabaseTableInfo(TypedDict):
    instance_id: Optional[str]
    schema_name: str
    table_name: str


class DataframeInfo(TypedDict):
    df_element_id: str
    df_name: str


class FileInfo(TypedDict):
    file_id: str
    file_name: str
    file_url: str


class ElementDataSource(TypedDict):
    databaseInfo: Optional[DatabaseInfo]
    databaseTableInfo: Optional[DatabaseTableInfo]
    dataframeInfo: Optional[DataframeInfo]
    fileInfo: Optional[FileInfo]
    sourceType: ElementDatasourceType


__all__ = [
    "DatabaseType",
    "ElementDatasourceType",
    "DatabaseInfo",
    "DatabaseTableInfo",
    "DataframeInfo",
    "FileInfo",
    "ElementDataSource",
]
