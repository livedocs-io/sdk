from typing import Literal, Optional, TypedDict, List
from enum import Enum


class DatabaseType(Enum):
    Bigquery = "Bigquery"
    Clickhouse = "Clickhouse"
    Mysql = "Mysql"
    Postgres = "Postgres"
    Redshift = "Redshift"
    Snowflake = "Snowflake"


class ElementDatasourceType(Enum):
    database = "database"
    database_table = "database_table"
    dataframe = "dataframe"
    file = "file"


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
    file_type: str
    file_has_layers: bool
    layer_name: Optional[str]


class ElementDataSource(TypedDict):
    database_info: Optional[DatabaseInfo]
    database_table_info: Optional[DatabaseTableInfo]
    dataframe_info: Optional[DataframeInfo]
    file_info: Optional[FileInfo]
    source_type: ElementDatasourceType


class DecryptedSecret(TypedDict):
    id: str
    key: str
    value: str


class DatabaseConnector(TypedDict):
    database_connector_id: str
    database_name: str
    connection_details: dict[str, str]


class Credentials(TypedDict):
    workspace_id: str
    workspace_secrets: List[DecryptedSecret]
    databases: List[DatabaseConnector]


class Schema(TypedDict):
    name: str
    type: str
    children: List


# Vega Chart Spec


class ColorBy(TypedDict):
    field: str
    type: str
    sort: Literal["ascending", "descending"]
    aggregate: str


class YAxisSeries(TypedDict):
    field: str
    aggregate: str
    mark: str
    type: str
    color_by: ColorBy
    name: Optional[str]


class XAxis(TypedDict):
    field: str
    type: str
    sort: Literal["ascending", "descending"]


class YAxis(TypedDict):
    primary: List[YAxisSeries]
    secondary: Optional[List[YAxisSeries]]


class LivedocsChartSpec(TypedDict):
    xAxis: XAxis
    yAxis: YAxis


__all__ = [
    "DatabaseType",
    "ElementDatasourceType",
    "DatabaseInfo",
    "DatabaseTableInfo",
    "DataframeInfo",
    "FileInfo",
    "ElementDataSource",
    "Schema",
    "LivedocsChartSpec",
]
