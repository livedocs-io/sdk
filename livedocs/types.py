import base64
import gzip
import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, TypedDict

import msgpack
from IPython.core.display import DisplayObject
from polars import DataFrame
from pydantic import BaseModel, model_validator

from livedocs.utils.serialize import _json_serializer


class GCSBucketType(str, Enum):
    USER_FILES = "user-files"
    CACHE_ARTIFACTS = "cache-artifacts"


class CacheStatus(str, Enum):
    HIT = "hit"
    MISS = "miss"


class CacheInfo(TypedDict):
    id: str
    status: CacheStatus


class QueryResultMetadata(TypedDict, total=False):
    limit: int
    offset: int
    total_rows: int
    cache_info: CacheInfo
    applied_metadata: Optional[Dict[str, Any]]
    calculation_results: Optional[Dict[str, Dict[str, Any]]]


class LivedocsResultInterface(ABC):
    """
    Interface defining the required methods for a Livedocs result class.
    Ideally, any result returned by an element should implement this interface.
    """

    @abstractmethod
    def serialize(self) -> str:
        """
        Serializes the result data into an appropriate format, such as a compressed
        and encoded string representation.
        """
        pass

    @abstractmethod
    def get_metadata(self):
        """
        Returns metadata about the result, including information on compression
        and encoding.
        """
        pass


class QueryResult(LivedocsResultInterface):
    """
    A class to represent the result of a query.

    Attributes:
    ----------
    data : DataFrame
        The data resulting from the query.
    metadata : QueryResultMetadata
        Metadata associated with the query result.
    """

    def __init__(self, data: DataFrame, metadata: QueryResultMetadata):
        self.data = data
        self.metadata = metadata

    def serialize(self) -> str:
        json_str = json.dumps(
            self.data.to_dicts(), default=_json_serializer, separators=(",", ":")
        )
        compressed = gzip.compress(json_str.encode("utf-8"))
        b64_encoded = base64.b64encode(compressed).decode("ascii")
        return b64_encoded

    def get_metadata(self):
        return {
            "text/plain": {
                "compression": "gzip",
                "encoding": "base64",
            },
            "query": self.metadata,
        }


class LivedocsResult:
    def __init__(self, result: LivedocsResultInterface):
        self.result = result

    def _repr_mimebundle_(self, include=None, exclude=None):
        data = {
            "text/plain": self.result.serialize(),
        }

        metadata = self.result.get_metadata()

        return data, metadata


class MsgPackDisplay(DisplayObject):
    """
    Custom display class for msgpack data in IPython
    """

    def __init__(self, data: Dict[str, Any], metadata: Optional[Dict] = None):
        super().__init__(data, metadata=metadata)
        self.data = data
        self.metadata = metadata or {}

    def _pack_data(self) -> bytes:
        """Pack the data using msgpack"""
        return msgpack.packb(self.data, use_bin_type=True)

    def _repr_mimebundle_(self, include=None, exclude=None):
        """
        Return the data as a mime bundle

        This method is called by IPython to get all mime types for the object
        """
        packed = self._pack_data()

        data = {
            "application/vnd.msgpack": packed,
        }

        return data, self.metadata


class JsonDisplay(DisplayObject):
    """
    Simple JSON display class for IPython - no msgpack, just JSON
    """

    def __init__(self, data: Dict[str, Any], metadata: Optional[Dict] = None):
        super().__init__(data, metadata=metadata)
        self.data = data
        self.metadata = metadata or {}

    def _repr_mimebundle_(self, include=None, exclude=None):
        """
        Return the data as JSON only
        """
        data = {
            "application/json": self.data,
        }

        return data, self.metadata


class UserMeta(BaseModel):
    styleSettings: dict
    chartType: str
    colorGroups: Optional[dict] = None
    pieSettings: Optional[dict] = None
    histogramSettings: Optional[dict] = None
    chartSettings: Optional[dict] = None
    swappedChartSettings: Optional[dict] = None

    @model_validator(mode="before")
    def validate_exclusive_chart_settings(cls, values):
        chart_fields = [
            "pieSettings",
            "histogramSettings",
            "chartSettings",
            "swappedChartSettings",
        ]
        provided_fields = [
            field for field in chart_fields if values.get(field) is not None
        ]

        if len(provided_fields) != 1:
            raise ValueError(
                "Exactly one of 'pieSettings', 'histogramSettings', 'chartSettings', or 'swappedChartSettings' must be provided."
            )

        return values


class VegaSpec(BaseModel):
    spec: str
    schema: dict
    status: str
    cache_info: Optional[CacheInfo] = None

    @model_validator(mode="before")
    def validate_usermeta(cls, values):
        status = values.get("status")
        if not status:
            raise ValueError("Missing 'status' in result")

        if status == "SUCCESS":
            spec_str = values.get("spec")
            try:
                spec_dict = json.loads(spec_str)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON format for 'spec'")

            usermeta = spec_dict.get("usermeta")
            if not usermeta:
                raise ValueError("Missing 'usermeta' in spec when status is 'SUCCESS'")

            # Validate usermeta only if status is "SUCCESS"
            UserMeta(**usermeta)

        return values


class DatabaseType(Enum):
    Bigquery = "bigquery"
    Clickhouse = "clickhouse"
    Mysql = "mysql"
    Postgres = "postgres"
    Redshift = "redshift"
    Snowflake = "snowflake"


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


class DBSaveConfig(TypedDict):
    dataframe_name: str
    dataframe_element_id: str
    database_name: str
    database_id: str
    database_type: DatabaseType
    schema_name: str
    table_name: str
    table_is_new: bool
    write_mode: Literal["append", "overwrite"]
    run_settings: List[
        Literal["edit_mode", "view_mode", "scheduled_runs", "webhook_runs"]
    ]


# Vega Chart Spec


class AxisStyleSettings(TypedDict, total=False):
    title: Optional[str]
    format: Optional[str]
    min: Optional[float]
    max: Optional[float]
    ticks: Optional[int]
    grid: Optional[Literal["solid", "dashed", "none"]]
    labelAngle: Optional[int]
    scale: Optional[Literal["linear", "log", "pow", "sqrt"]]


class LegendSettings(TypedDict, total=False):
    show: Optional[bool]
    position: Optional[
        Literal[
            "top",
            "right",
            "bottom",
            "left",
            "top-left",
            "bottom-left",
            "top-right",
            "bottom-right",
        ]
    ]
    title: Optional[str]


class MarkColorSettings(TypedDict):
    hex: List[Dict[str, str]]
    mode: Literal["all_fields"]


class MarkOpacitySettings(TypedDict):
    value: List[Dict[str, float]]
    mode: Literal["all_fields", "by_field", "based_on_field"]


class MarkSettings(TypedDict):
    color: Optional[MarkColorSettings]
    opacity: Optional[MarkOpacitySettings]


class StyleSettings(TypedDict, total=False):
    fontSize: Optional[int]
    tooltip: Optional[bool]
    legend: Optional[LegendSettings]
    markSettings: Optional[Dict[str, MarkSettings]]
    xAxis: Optional[AxisStyleSettings]
    yAxis: Optional[AxisStyleSettings]


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


class LivedocsSwappedChartSpec(TypedDict):
    xAxis: YAxis
    yAxis: YAxis


class PieChartColorBy(TypedDict):
    field: str
    type: str


class PieChartSizeBy(TypedDict):
    field: str
    type: str
    aggregate: Literal["count", "sum", "none"]


class PieChartSpec(TypedDict):
    color_by: PieChartColorBy
    size_by: PieChartSizeBy
    show_as: Literal["value", "percentage"]
    format: str


class HistogramBinBy(TypedDict):
    type: Literal["max_bins", "step_size", "column_value"]
    value: int


class HistogramSpec(TypedDict):
    field: str
    format: Literal["count", "percentage"]
    binBy: HistogramBinBy


class Spec(TypedDict):
    chartType: Literal["main", "histogram", "swapped_main", "pie"]
    styleSettings: Optional[StyleSettings]
    chartSettings: Optional[LivedocsChartSpec]
    swappedChartSettings: Optional[LivedocsSwappedChartSpec]
    histogramSettings: Optional[HistogramSpec]
    pieSettings: Optional[PieChartSpec]


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
    "Spec",
    "DBSaveConfig",
    "JsonDisplay",
]
