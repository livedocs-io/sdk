import base64
import gzip
import json
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Literal, TypedDict
from uuid import UUID

import msgpack
from IPython.core.display import DisplayObject
from polars import DataFrame
from pydantic import BaseModel, Field, SecretStr, model_validator


class GCSBucketType(str, Enum):
    USER_FILES = "user-files"
    CACHE_ARTIFACTS = "cache-artifacts"


class FileManifestAction(str, Enum):
    READ = "read"
    WRITE = "write"


class CacheStatus(str, Enum):
    HIT = "hit"
    MISS = "miss"


class CacheInfo(TypedDict):
    id: str
    status: CacheStatus


class FileManifest(BaseModel):
    file_id: str  # The unique ID of the file (resolved by the API)
    file_name: str  # The display name of the file
    signed_url: str  # The GCS signed URL for download/access
    size: int | None  # File size in bytes, can be None if unknown
    type: str | None  # e.g., 'csv', 'xlsx'
    created_at: str | None  # ISO string for creation timestamp
    bucket: GCSBucketType | None  # The bucket type (user-files or cache-artifacts)
    action: FileManifestAction  # Action type (read/write)


class QueryResultMetadata(TypedDict, total=False):
    limit: int
    offset: int
    total_rows: int
    cache_info: CacheInfo
    applied_metadata: dict[str, Any] | None
    calculation_results: dict[str, dict[str, Any]] | None


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
        from livedocs.utils.common import serializer

        json_str = json.dumps(
            self.data.to_dicts(), default=serializer, separators=(",", ":")
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


class ChartResult(LivedocsResultInterface):
    """
    A class to represent the result of a chart generation.

    Attributes:
    ----------
    data : str
        The encoded chart spec data.
    metadata : dict
        Metadata associated with the chart result including cache info.
    """

    def __init__(self, data: str, cache_info: CacheInfo | None = None):
        self.data = data
        self.cache_info = cache_info

    def serialize(self) -> str:
        return self.data

    def get_metadata(self):
        return {
            "text/plain": {
                "compression": "gzip",
                "encoding": "base64",
            },
            "chart": {
                "cache_info": self.cache_info,
            },
        }


class LivedocsResult:
    def __init__(self, result: LivedocsResultInterface):
        self.result: LivedocsResultInterface = result

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

    def __init__(self, data, metadata: dict[str, Any] | None = None):
        super().__init__(data, metadata=metadata)
        self.data = data
        self.metadata = metadata or {}

    def _pack_data(self) -> bytes:
        """Pack the data using msgpack"""
        from livedocs.utils.common import serializer

        return msgpack.packb(self.data, default=serializer)

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

    def __init__(self, data, metadata: dict[str, Any] | None = None):
        super().__init__(data, metadata=metadata)
        self.data = self._to_json_serializable(data)
        self.metadata = metadata or {}

    def _to_json_serializable(self, obj: Any) -> Any:
        """Recursively convert objects to JSON-serializable format."""
        if obj is None:
            return None
        if isinstance(obj, BaseModel):
            return obj.model_dump(mode="json")
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, dict):
            return {k: self._to_json_serializable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._to_json_serializable(item) for item in obj]
        return obj

    def _repr_mimebundle_(self, include=None, exclude=None):
        """
        Return the data as JSON only
        """
        data = {
            "application/json": self.data,
        }

        return data, self.metadata


class UserMeta(BaseModel):
    styleSettings: dict[str, Any]
    chartType: str
    colorGroups: dict[str, Any] | None = None
    pieSettings: dict[str, Any] | None = None
    histogramSettings: dict[str, Any] | None = None
    chartSettings: dict[str, Any] | None = None
    swappedChartSettings: dict[str, Any] | None = None

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
    schema: dict[str, Any]
    status: str

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


class SourceType(str, Enum):
    workspace = "workspace"
    s3bucket = "s3bucket"
    runtime = "runtime"
    googlesheets = "googlesheets"
    googledrive = "googledrive"
    database = "database"


class FileConnectorType(str, Enum):
    s3bucket = "s3bucket"
    runtime = "runtime"
    googlesheets = "googlesheets"
    googledrive = "googledrive"
    workspace = "workspace"


class DatabaseType(Enum):
    Bigquery = "bigquery"
    Clickhouse = "clickhouse"
    Motherduck = "motherduck"
    Databricks = "databricks"
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
    instance_id: str | None
    catalog_name: str | None
    schema_name: str
    table_name: str


class DataframeInfo(TypedDict):
    df_element_id: str
    df_name: str


class FileConnectorInfo(TypedDict):
    connector_id: str
    connector_name: str
    connector_type: FileConnectorType


class FileInfo(TypedDict):
    file_id: str
    file_name: str
    file_type: str
    file_has_layers: bool
    layer_name: str | None
    connector_info: FileConnectorInfo | None


class ElementDataSource(TypedDict):
    database_info: DatabaseInfo | None
    database_table_info: DatabaseTableInfo | None
    dataframe_info: DataframeInfo | None
    file_info: FileInfo | None
    source_type: ElementDatasourceType


class WorkspaceSecret(BaseModel):
    id: str
    key: str
    value: SecretStr


class DatabaseConnection(BaseModel):
    db_connector_id: str
    db_name: str
    connection_details: SecretStr


class S3ConnectorInfo(TypedDict):
    connector_id: str
    name: str
    endpoint_url: str
    region: str
    provider: str
    access_key: str
    secret_key: str
    bucket_name: str
    path_prefix: str
    is_virtual_hosted_style: bool


class GoogleDriveConnectorInfo(TypedDict):
    connector_id: str
    name: str
    provider: str
    email: str
    access_token: str
    refresh_token: str
    token_expiry: datetime
    scopes: str


class Credentials(BaseModel):
    workspace_id: str
    workspace_secrets: dict[str, WorkspaceSecret]
    databases: dict[str, DatabaseConnection]
    s3_connectors: dict[str, S3ConnectorInfo]
    google_drive_connectors: dict[str, GoogleDriveConnectorInfo]
    built_in_vars: dict[str, Any | None]


class FileAction(str, Enum):
    RENAME = "rename"
    DELETE = "delete"


class SDKContext(str, Enum):
    RELAY = "relay"
    IPYTHON = "ipython"


class Schema(TypedDict):
    name: str
    type: str
    children: list["Schema"]


class SchemaNodeType(str, Enum):
    COLUMN = "COLUMN"
    DATABASE = "DATABASE"
    SCHEMA = "SCHEMA"
    TABLE = "TABLE"
    VIEW = "VIEW"


class MountHealthStatus(str, Enum):
    connected = "connected"
    reconnecting = "reconnecting"
    auth_expired = "auth_expired"
    error = "error"


class MountHealth(TypedDict):
    status: MountHealthStatus
    last_checked: datetime
    error_message: str | None


class FileNodeType(str, Enum):
    root = "root"
    directory = "directory"
    file = "file"


class FileNode(BaseModel):
    id: UUID
    name: str
    type: FileNodeType
    mount_type: FileConnectorType
    connector_id: UUID
    path: str
    parent_id: UUID | None = None
    size: int | None = None
    mime_type: str | None = None
    modified_at: datetime | None = None
    created_at: datetime | None = None
    health: MountHealth


class SchemaNode(BaseModel):
    id: UUID
    connector_id: UUID
    parent_id: UUID | None = None
    path: str
    type: SchemaNodeType
    name: str
    data_type: str | None = None
    livedocs_type: str | None = None
    description: str | None = None
    level: int
    metadata: dict[str, str] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None

    class Config:
        from_attributes = True


class ListPathRequest(BaseModel):
    node_id: str | None = None
    schema_node_type: SchemaNodeType | None = None
    search_string: str | None = None


class ListPathResponse(BaseModel):
    files: list[FileNode]
    schema_nodes: list[SchemaNode]


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
    run_settings: list[
        Literal["edit_mode", "view_mode", "scheduled_runs", "webhook_runs"]
    ]


# Vega Chart Spec


class ReferenceLineSettings(TypedDict):
    label: str | None
    value: str | None
    color: str | None
    labelPosition: (
        Literal[
            "none", "outside", "top-left", "top-right", "bottom-left", "bottom-right"
        ]
        | None
    )
    labelAngle: int | None
    lineWidth: int | None
    lineStyle: Literal["solid", "dashed", "dotted"] | None


class AxisStyleSettings(TypedDict, total=False):
    title: str | None
    format: str | None
    min: float | None
    max: float | None
    ticks: int | None
    grid: Literal["solid", "dashed", "none"] | None
    labelAngle: int | None
    scale: Literal["linear", "log", "pow", "sqrt"] | None
    referenceLines: list[ReferenceLineSettings] | None


class LegendSettings(TypedDict, total=False):
    show: bool | None
    position: (
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
        | None
    )
    title: str | None


class MarkColorSettings(TypedDict):
    hex: list[dict[str, str]]
    mode: Literal["all_fields"]


class MarkOpacitySettings(TypedDict):
    value: list[dict[str, float]]
    mode: Literal["all_fields", "by_field", "based_on_field"]


class MarkDataLabelsSettings(TypedDict):
    show: bool | None
    mode: Literal["per_color", "total"] | None
    position: Literal["inside-top", "center", "outside-top"] | None
    color: Literal["auto", "white", "black"] | None
    angle: int | None
    fontSize: int | None


class MarkSettings(TypedDict):
    color: MarkColorSettings | None
    opacity: MarkOpacitySettings | None
    dataLabels: MarkDataLabelsSettings | None


class StyleSettings(TypedDict, total=False):
    fontSize: int | None
    tooltip: bool | None
    legend: LegendSettings | None
    markSettings: dict[str, MarkSettings] | None
    xAxis: AxisStyleSettings | None
    yAxis: AxisStyleSettings | None
    mode: Literal["light", "dark"] | None


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
    name: str | None


class XAxis(TypedDict):
    field: str
    type: str
    sort: Literal["ascending", "descending"]


class YAxis(TypedDict):
    primary: list[YAxisSeries]
    secondary: list[YAxisSeries] | None


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


class HorizontalSubplotSettings(TypedDict):
    field: str | None
    sort: Literal["ascending", "descending"] | None
    wrap: bool | None
    columns: int | None
    bin: bool | None
    bin_count: int | None


class VerticalSubplotSettings(TypedDict):
    field: str | None
    sort: Literal["ascending", "descending"] | None
    linkYAxis: bool | None
    bin: bool | None
    bin_count: int | None


class SubplotSettings(TypedDict):
    horizontal: HorizontalSubplotSettings
    vertical: VerticalSubplotSettings


class Spec(TypedDict):
    chartType: Literal["main", "histogram", "swapped_main", "pie"]
    styleSettings: StyleSettings | None
    chartSettings: LivedocsChartSpec | None
    swappedChartSettings: LivedocsSwappedChartSpec | None
    histogramSettings: HistogramSpec | None
    pieSettings: PieChartSpec | None
    colorGroups: dict[str, dict[str, str] | str] | None
    subplots: SubplotSettings | None


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
    "SchemaNode",
    "SchemaNodeType",
    "ListPathRequest",
    "ListPathResponse",
    "SDKContext",
]
