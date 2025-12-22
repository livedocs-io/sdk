from __future__ import annotations

import mimetypes
import os
from datetime import datetime, timezone
from typing import Any, Callable, List
from uuid import UUID, uuid5

import polars as pl
import s3fs

from livedocs.datasources.base import BaseDatasourceConnector
from livedocs.types import (
    DBSaveConfig,
    ElementDataSource,
    FileConnectorType,
    FileNode,
    FileNodeType,
    LivedocsResult,
    MountHealth,
    MountHealthStatus,
    QueryResult,
    QueryResultMetadata,
    S3ConnectorInfo,
)
from livedocs.utils.common import (
    _get_dataframe_schema,
    get_xlsx_sheet_names,
    middleman_debug,
)
from livedocs.utils.lib.internals import (
    livedocs_internal_sanitize_sensitive_data as sanitize_sensitive_data,
)


class S3DatasourceConnector(BaseDatasourceConnector):
    """
    Datasource connector for S3 bucket connections.
    """

    @staticmethod
    def _create_s3_filesystem(connector_info: S3ConnectorInfo) -> s3fs.S3FileSystem:
        """
        Create an s3fs S3FileSystem from connector info.

        Supports various S3-compatible services:
        - MinIO, Wasabi, Backblaze B2, DigitalOcean Spaces, Cloudflare R2, GCS Interop

        Args:
            connector_info: S3 connector configuration

        Returns:
            s3fs.S3FileSystem: Configured S3 filesystem
        """
        # Build client_kwargs - region is optional (some providers ignore it)
        client_kwargs: dict[str, Any] = {}
        if connector_info.get("region"):
            client_kwargs["region_name"] = connector_info["region"]

        # Endpoint URL required for S3-compatible services
        endpoint_url = connector_info.get("endpoint_url")
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url

        # Configure addressing style: "virtual" or "path"
        # Virtual-hosted style: bucket.s3.amazonaws.com
        # Path style: s3.amazonaws.com/bucket
        addressing_style = (
            "virtual" if connector_info.get("is_virtual_hosted_style", True) else "path"
        )
        config_kwargs: dict[str, Any] = {"s3": {"addressing_style": addressing_style}}

        # Build S3FileSystem with explicit parameters
        return s3fs.S3FileSystem(
            key=connector_info["access_key"],
            secret=connector_info["secret_key"],
            client_kwargs=client_kwargs,
            config_kwargs=config_kwargs,
        )

    @staticmethod
    def _generate_file_id(connector_id: str, path: str) -> UUID:
        """
        Generate a deterministic UUID from connector_id and path.

        Args:
            connector_id: The connector ID (must be a valid UUID string)
            path: The file path

        Returns:
            UUID: Deterministic UUID based on connector_id and path hash
        """
        # Use connector_id directly as namespace UUID
        namespace = UUID(connector_id)
        # Generate UUID from path
        return uuid5(namespace, path)

    @staticmethod
    def _get_parent_path(path: str) -> str | None:
        """
        Extract parent directory path.

        Args:
            path: The file/directory path

        Returns:
            Parent path or None if root
        """
        path = path.rstrip("/")
        if not path or path == "/":
            return None
        parent = "/".join(path.split("/")[:-1])
        return parent if parent else None

    @staticmethod
    def _guess_mime_type(path: str) -> str | None:
        """
        Best-effort MIME type detection based on file extension.
        """
        name = os.path.basename(path)
        guessed_type, _ = mimetypes.guess_type(name)
        if guessed_type:
            return guessed_type

        ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
        data_format_types = {
            "parquet": "application/vnd.apache.parquet",
            "avro": "application/avro",
            "feather": "application/vnd.apache.arrow.file",
            "arrow": "application/vnd.apache.arrow.file",
            "gpkg": "application/geopackage+sqlite3",
            "geojson": "application/geo+json",
            "shp": "application/x-shapefile",
            "kml": "application/vnd.google-earth.kml+xml",
            "kmz": "application/vnd.google-earth.kmz",
        }
        return data_format_types.get(ext)

    @staticmethod
    def _strip_bucket_and_prefix(
        full_path: str, bucket_name: str, path_prefix_clean: str
    ) -> str:
        """
        Remove bucket name and path_prefix from a full S3 path and return a relative key.
        """
        # Drop scheme if present
        if full_path.startswith("s3://"):
            full_path = full_path[len("s3://") :]

        # Remove leading bucket name
        bucket_prefix = f"{bucket_name}/"
        if full_path.startswith(bucket_prefix):
            full_path = full_path[len(bucket_prefix) :]

        # Remove configured path prefix
        if path_prefix_clean and full_path.startswith(path_prefix_clean):
            full_path = full_path[len(path_prefix_clean) :]

        return full_path.lstrip("/")

    def _list_xlsx_sheets(
        self,
        path: str,
        connector_id: str,
        connector_name: str,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]],
    ) -> List[FileNode]:
        """
        List sheets in an xlsx file as virtual FileNodes.

        Args:
            path: Relative path to the xlsx file in S3
            connector_id: The S3 connector ID
            connector_name: The S3 connector name (for download path organization)
            get_connection_details: Callable to retrieve connection details

        Returns:
            List of FileNode objects representing sheets in the xlsx file
        """
        # Download the xlsx file to a local temp location
        local_path = self.download_file(
            connector_name=connector_name,
            path=path,
            connector_id=connector_id,
            get_connection_details=get_connection_details,
            preview=True,
        )

        if not local_path or not os.path.exists(local_path):
            return []

        sheet_names = get_xlsx_sheet_names(local_path)
        if not sheet_names:
            return []

        now = datetime.now(timezone.utc)
        nodes: list[FileNode] = []

        # Generate parent file ID (the xlsx file itself)
        parent_id = self._generate_file_id(connector_id, path)

        for sheet_name in sheet_names:
            # Use :: as separator to distinguish sheet paths from directory paths
            sheet_path = f"{path}::{sheet_name}"
            sheet_id = self._generate_file_id(connector_id, sheet_path)

            nodes.append(
                FileNode(
                    id=sheet_id,
                    name=sheet_name,
                    type=FileNodeType.file,
                    mount_type=FileConnectorType.s3bucket,
                    connector_id=UUID(connector_id),
                    connector_name=connector_name,
                    path=sheet_path,
                    parent_id=parent_id,
                    size=None,
                    mime_type="application/vnd.ms-excel.sheet",
                    modified_at=None,
                    created_at=None,
                    health=MountHealth(
                        status=MountHealthStatus.connected,
                        last_checked=now,
                        error_message=None,
                    ),
                )
            )

        return nodes

    def search(
        self,
        search_query: str,
        connector_id: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
        max_results: int = 200,
    ) -> List[FileNode]:
        """
        Search files in S3 by name using a glob pattern (non-recursive list kept separate).

        Args:
            search_query: Substring to match in file names.
            connector_id: S3 connector ID.
            get_connection_details: Callable to retrieve connector credentials.
            max_results: Maximum number of results to return.

        Returns:
            List of FileNode objects matching the query.
        """
        if not search_query or connector_id is None or get_connection_details is None:
            return []

        try:
            _, connector_info_dict = get_connection_details(connector_id)
            connector_info: S3ConnectorInfo = connector_info_dict  # type: ignore[assignment]
        except (KeyError, TypeError, ValueError):
            return []
        connector_name = connector_info.get("name", "s3")

        s3_fs = None
        now = datetime.now(timezone.utc)
        nodes: list[FileNode] = []

        try:
            s3_fs = self._create_s3_filesystem(connector_info)
            bucket_name = connector_info["bucket_name"]
            path_prefix_clean = connector_info.get("path_prefix", "").rstrip("/")

            # Build glob pattern to search across connector scope (similar to previous logic)
            if path_prefix_clean:
                glob_prefix = f"{bucket_name}/{path_prefix_clean}"
            else:
                glob_prefix = f"{bucket_name}"

            pattern = f"{glob_prefix}/**/*{search_query}*"
            raw = s3_fs.glob(pattern, detail=True, recursive=True)

            items: list[tuple[str, Any]] = []
            if isinstance(raw, dict):
                items = list(raw.items())
            elif isinstance(raw, list):
                for entry in raw:
                    if isinstance(entry, dict):
                        path_candidate = (
                            entry.get("Key")
                            or entry.get("name")
                            or entry.get("path")
                            or entry.get("key")
                        )
                        items.append((path_candidate, entry))
                    else:
                        items.append((entry, {}))

            for item_key, item_detail in items:
                if not item_key:
                    continue

                # Respect max_results
                if len(nodes) >= max_results:
                    break

                is_directory = item_key.endswith("/") or (
                    isinstance(item_detail, dict)
                    and item_detail.get("type") == "directory"
                )

                # Skip directories
                if is_directory:
                    continue

                relative_path = self._strip_bucket_and_prefix(
                    item_key, bucket_name, path_prefix_clean
                )
                parent_path = self._get_parent_path(relative_path)

                size = None
                if isinstance(item_detail, dict):
                    size_raw = item_detail.get("Size") or item_detail.get("size")
                    if size_raw is not None:
                        try:
                            size = int(size_raw)
                        except (ValueError, TypeError):
                            size = None

                modified_at = None
                if isinstance(item_detail, dict):
                    last_modified = item_detail.get("LastModified")
                    if isinstance(last_modified, datetime):
                        modified_at = last_modified

                file_id = self._generate_file_id(connector_id, relative_path)
                parent_id = (
                    self._generate_file_id(connector_id, parent_path)
                    if parent_path
                    else None
                )

                nodes.append(
                    FileNode(
                        id=file_id,
                        name=relative_path.split("/")[-1],
                        type=FileNodeType.file,
                        mount_type=FileConnectorType.s3bucket,
                        connector_id=UUID(connector_id),
                        connector_name=connector_name,
                        path=relative_path,
                        parent_id=parent_id,
                        size=size,
                        mime_type=self._guess_mime_type(relative_path),
                        modified_at=modified_at,
                        created_at=None,
                        health=MountHealth(
                            status=MountHealthStatus.connected,
                            last_checked=now,
                            error_message=None,
                        ),
                    )
                )

        except Exception as e:
            error_health = MountHealth(
                status=MountHealthStatus.error,
                last_checked=datetime.now(timezone.utc),
                error_message=str(sanitize_sensitive_data(str(e))),
            )
            for node in nodes:
                node.health = error_health

        return nodes

    @staticmethod
    def _construct_s3_path(connector_info: S3ConnectorInfo, relative_path: str) -> str:
        """
        Construct the full S3 path from connector info and relative path.

        Args:
            connector_info: S3 connector configuration
            relative_path: Relative path within the bucket (after path_prefix)

        Returns:
            Full S3 path: bucket_name/path_prefix/relative_path
        """
        bucket_name = connector_info["bucket_name"]
        path_prefix_clean = connector_info.get("path_prefix", "").rstrip("/")
        normalized_path = relative_path.strip("/") if relative_path else ""

        if path_prefix_clean and normalized_path:
            return f"{bucket_name}/{path_prefix_clean}/{normalized_path}"
        elif path_prefix_clean:
            return (
                f"{bucket_name}/{path_prefix_clean}/{normalized_path}"
                if normalized_path
                else f"{bucket_name}/{path_prefix_clean}"
            )
        elif normalized_path:
            return f"{bucket_name}/{normalized_path}"
        else:
            return f"{bucket_name}/"

    def read(
        self,
        query: str,
        datasource: ElementDataSource,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
        **kwargs: Any,
    ) -> tuple[pl.DataFrame, dict[str, str]]:
        """
        Execute a query against an S3 datasource using DuckDB.

        Args:
            query: SQL query string to execute (DuckDB query)
            datasource: Datasource configuration
            get_database_details: Callable to retrieve connection details (works for file connectors too)
            **kwargs: Additional arguments:
                - duckdb_conn: DuckDB connection instance (required)

        Returns:
            Tuple containing:
            - DataFrame with query results
            - Schema information as a dict mapping column names to Livedocs types
        """
        duckdb_conn = kwargs.get("duckdb_conn")
        if duckdb_conn is None:
            raise ValueError("DuckDB connection is required for S3 datasources")

        try:
            file_info = datasource.get("file_info")
            if file_info is None:
                raise ValueError("Missing required information: 'file_info'")

            connector_info = file_info.get("connector_info")
            if connector_info is None:
                raise ValueError("Missing required information: 'connector_info'")

            connector_id = connector_info["connector_id"]
            connector_name = connector_info.get("connector_name")
            file_name = file_info["file_name"]
            # file_id contains the actual S3 path (relative to bucket/prefix)
            file_id = file_info.get("file_id")
            # Use file_id for the S3 path, fall back to file_name for backwards compatibility
            s3_path = file_id if file_id else file_name

            # Check if file_path was already provided in kwargs (Case 2: preview scenario)
            local_file_path = kwargs.get("file_path")

            # Only download if file_path wasn't already provided
            if local_file_path is None:
                # Download the file using get_file with preview=False to download full file
                local_file_path = self.download_file(
                    path=s3_path,
                    connector_id=connector_id,
                    get_connection_details=get_database_details,
                    preview=False,
                    connector_name=connector_name,
                )

                if local_file_path is None:
                    raise ValueError(
                        f"Failed to download file from S3. File may not exist at path: {s3_path}"
                    )

            # Execute query using DuckDB
            result = duckdb_conn.sql(query).pl()
            schema = _get_dataframe_schema(result)
            return result, schema

        except KeyError as e:
            raise ValueError(f"Missing required information in datasource: {e}")
        except Exception as e:
            raise RuntimeError(
                sanitize_sensitive_data(
                    f"An error occurred while querying the S3 file: {e}"
                )
            )

    def write(
        self,
        df: pl.DataFrame,
        save_config: DBSaveConfig,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
    ) -> LivedocsResult:
        """
        Write to an S3 datasource (no-op skeleton).

        Args:
            df: DataFrame to write
            save_config: Configuration for saving to database
            get_database_details: Callable to retrieve database credentials

        Returns:
            LivedocsResult containing a success QueryResult with empty data
        """
        result = QueryResult(
            data=pl.DataFrame(),
            metadata=QueryResultMetadata(),  # type: ignore[typeddict-item]
        )
        return LivedocsResult(result)

    def list(
        self,
        path: str | None = None,
        connector_id: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
        max_depth: int = 2,
    ) -> List[FileNode]:
        """
        List files and directories in an S3 bucket at the given path.

        Args:
            path: The S3 path to list (relative to path_prefix). None means list is skipped.
            connector_id: The S3 connector ID. None means list is skipped.
            get_connection_details: Callable to retrieve connection details

        Returns:
            List of FileNode objects representing files and directories
        """
        if connector_id is None or path is None or get_connection_details is None:
            return []

        try:
            _, connector_info_dict = get_connection_details(connector_id)
            # TypedDict is for type hints only, we can use the dict directly
            connector_info: S3ConnectorInfo = connector_info_dict  # type: ignore[assignment]
        except (KeyError, TypeError, ValueError):
            return []
        connector_name = connector_info.get("name", "s3")

        # Handle xlsx files - list sheets as children
        if path.lower().endswith(".xlsx"):
            return self._list_xlsx_sheets(
                path=path,
                connector_id=connector_id,
                connector_name=connector_name,
                get_connection_details=get_connection_details,
            )

        s3_fs = None
        now = datetime.now(timezone.utc)
        nodes: list[FileNode] = []

        try:
            s3_fs = self._create_s3_filesystem(connector_info)
            bucket_name = connector_info["bucket_name"]
            path_prefix_clean = connector_info.get("path_prefix", "").rstrip("/")
            normalized_path = path.strip("/") if path else ""

            # Construct full S3 path for listing
            if path_prefix_clean and normalized_path:
                s3_path = f"{bucket_name}/{path_prefix_clean}/{normalized_path}/"
            elif path_prefix_clean:
                s3_path = f"{bucket_name}/{path_prefix_clean}/"
            elif normalized_path:
                s3_path = f"{bucket_name}/{normalized_path}/"
            else:
                s3_path = f"{bucket_name}/"  # List from bucket root

            # Use s3fs ls() with detail=True to get file information (non-recursive)
            raw = s3_fs.ls(s3_path, detail=True)
            if isinstance(raw, dict):
                items = [(k, v) for k, v in raw.items()]
            else:
                items = []
                for entry in raw:
                    if isinstance(entry, dict):
                        path_candidate = (
                            entry.get("Key")
                            or entry.get("name")
                            or entry.get("path")
                            or entry.get("key")
                        )
                        items.append((path_candidate, entry))
                    else:
                        items.append((entry, {}))

            for item_key, item_detail in items:
                if not item_key:
                    continue
                # s3fs returns items as dicts when detail=True; glob may return strings
                is_directory = item_key.endswith("/") or (
                    isinstance(item_detail, dict)
                    and item_detail.get("type") == "directory"
                )

                # Remove bucket name prefix to get relative path
                if item_key.startswith(f"{bucket_name}/"):
                    full_path = item_key[len(f"{bucket_name}/") :]
                else:
                    full_path = item_key

                # Calculate relative path by removing path_prefix
                if path_prefix_clean and full_path.startswith(path_prefix_clean):
                    relative_path = full_path[len(path_prefix_clean) :].lstrip("/")
                else:
                    relative_path = full_path.lstrip("/")

                if not relative_path:
                    continue

                # Strip trailing slash for comparison and name extraction
                relative_path_clean = relative_path.rstrip("/")

                # Skip the listing path itself (the parent folder we're listing)
                if relative_path_clean == normalized_path and is_directory:
                    continue

                # Skip empty paths after cleaning
                if not relative_path_clean:
                    continue

                name = relative_path_clean.split("/")[-1]
                parent_path = self._get_parent_path(relative_path)

                file_id = self._generate_file_id(connector_id, relative_path)
                parent_id = (
                    self._generate_file_id(connector_id, parent_path)
                    if parent_path
                    else None
                )

                # Get metadata from s3fs item
                size = (
                    item_detail.get("Size")
                    if not isinstance(item_detail, str)
                    else None
                )
                last_modified = (
                    item_detail.get("LastModified")
                    if not isinstance(item_detail, str)
                    else None
                )
                modified_at = None
                if last_modified:
                    if isinstance(last_modified, datetime):
                        modified_at = last_modified.replace(tzinfo=timezone.utc)
                    else:
                        # If it's a string or timestamp, try to parse
                        try:
                            modified_at = datetime.fromtimestamp(
                                last_modified, tz=timezone.utc
                            )
                        except (TypeError, ValueError):
                            pass

                # Get content type - S3 ListObjectsV2 doesn't return ContentType,
                # so we infer it from the file extension
                content_type = None
                if isinstance(item_detail, dict):
                    content_type = item_detail.get("ContentType")
                if content_type is None and not is_directory:
                    # Use mimetypes to guess from filename
                    guessed_type, _ = mimetypes.guess_type(name)
                    if guessed_type:
                        content_type = guessed_type
                    else:
                        # Fallback for common data formats not in mimetypes
                        ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
                        data_format_types = {
                            "parquet": "application/vnd.apache.parquet",
                            "avro": "application/avro",
                            "feather": "application/vnd.apache.arrow.file",
                            "arrow": "application/vnd.apache.arrow.file",
                            "gpkg": "application/geopackage+sqlite3",
                            "geojson": "application/geo+json",
                            "shp": "application/x-shapefile",
                            "kml": "application/vnd.google-earth.kml+xml",
                            "kmz": "application/vnd.google-earth.kmz",
                        }
                        content_type = data_format_types.get(ext)

                nodes.append(
                    FileNode(
                        id=file_id,
                        name=name,
                        type=FileNodeType.directory
                        if is_directory
                        else FileNodeType.file,
                        mount_type=FileConnectorType.s3bucket,
                        connector_id=UUID(connector_id),
                        connector_name=connector_name,
                        path=relative_path,
                        parent_id=parent_id,
                        size=size if not is_directory else None,
                        mime_type=content_type if not is_directory else None,
                        modified_at=modified_at,
                        created_at=modified_at,  # S3 doesn't have created_at, use modified_at
                        health=MountHealth(
                            status=MountHealthStatus.connected,
                            last_checked=now,
                            error_message=None,
                        ),
                    )
                )
        except Exception as e:
            error_health = MountHealth(
                status=MountHealthStatus.error,
                last_checked=datetime.now(timezone.utc),
                error_message=str(sanitize_sensitive_data(str(e))),
            )
            # Update health for all nodes
            for node in nodes:
                node.health = error_health

        return nodes

    def download_file(
        self,
        connector_name: str | None,
        path: str | None = None,
        connector_id: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
        preview: bool = True,
    ) -> str | None:
        """
        Get a file from S3, downloading it to a local path.

        Only downloads files smaller than 100MB when preview=True. Files are saved to the directory
        specified by the LIVEDOCS_FILES_PATH environment variable (defaults to /tmp/livedocs_files).
        If connector_name is provided, files are saved to a subfolder named after the connector.

        Args:
            connector_type: Type of file connector (should be FileConnectorType.s3bucket)
            file_id: File ID (not used for S3, path is required)
            path: Relative path to the file in S3
            connector_id: S3 connector ID
            get_connection_details: Callable to retrieve connection details
            preview: If True, only download files smaller than 100MB. If False, download regardless of size.
            connector_name: Optional connector name to organize files in a subfolder

        Returns:
            Local file path if downloaded successfully, or None on error or if file is too large (when preview=True)
        """
        if connector_id is None or path is None or get_connection_details is None:
            return None

        try:
            _, connector_info_dict = get_connection_details(connector_id)
            connector_info: S3ConnectorInfo = connector_info_dict  # type: ignore[assignment]
        except (KeyError, TypeError, ValueError):
            return None

        try:
            s3_fs = self._create_s3_filesystem(connector_info)
            s3_path = self._construct_s3_path(connector_info, path)

            # Check if file exists
            if not s3_fs.exists(s3_path):
                return None

            # Get file size and check limit only if preview=True
            if preview:
                file_info = s3_fs.info(s3_path)
                file_size_raw = file_info.get("Size") or file_info.get("size", 0)
                file_size_bytes = int(file_size_raw) if file_size_raw else 0

                # Check if file is less than 100MB (100 * 1024 * 1024 bytes)
                MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024  # 100MB
                if file_size_bytes >= MAX_FILE_SIZE_BYTES:
                    return None

            # Get download path from environment variable
            download_base_path = os.getenv("LIVEDOCS_FILES_PATH", "/tmp/livedocs_files")

            # Create subfolder if connector_name is provided
            if connector_name:
                download_base_path = os.path.join(download_base_path, connector_name)

            os.makedirs(download_base_path, exist_ok=True)

            # Create local file path
            file_name = os.path.basename(path) if path else "file"
            local_file_path = os.path.join(download_base_path, file_name)

            # Check if file already exists locally (caching)
            if os.path.exists(local_file_path):
                return local_file_path

            # Download file
            s3_fs.download(s3_path, local_file_path)

            return local_file_path

        except Exception as e:
            middleman_debug(
                f"S3 download error for path '{path}'", data=e, level="error"
            )
            return None

    def upload_file_to_s3(
        self,
        file_path: str,
        connector_id: str | None = None,
        s3_path: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
    ) -> bool:
        """
        Upload a local file to S3.

        Args:
            file_path: Local file path to upload
            connector_type: Type of file connector (should be FileConnectorType.s3bucket)
            connector_id: S3 connector ID
            s3_path: Destination path in S3 (relative to path_prefix). If None, uses basename of file_path
            get_connection_details: Callable to retrieve connection details

        Returns:
            True if successful, False otherwise
        """
        if connector_id is None or get_connection_details is None:
            return False

        if not os.path.exists(file_path):
            return False

        try:
            _, connector_info_dict = get_connection_details(connector_id)
            connector_info: S3ConnectorInfo = connector_info_dict  # type: ignore[assignment]  # pyright: ignore[reportAssignmentType]
        except (KeyError, TypeError, ValueError):
            return False

        try:
            s3_fs = self._create_s3_filesystem(connector_info)

            # Determine destination path in S3
            if s3_path is None:
                s3_path = os.path.basename(file_path)

            full_s3_path = self._construct_s3_path(connector_info, s3_path)

            # Upload file
            s3_fs.upload(file_path, full_s3_path)

            return True

        except Exception:
            return False

    def delete_file(
        self,
        file_path: str,
        connector_id: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
    ) -> bool:
        """
        Delete a file from S3.

        Args:
            file_path: Relative path to the file in S3
            connector_type: Type of file connector (should be FileConnectorType.s3bucket)
            connector_id: S3 connector ID
            get_connection_details: Callable to retrieve connection details

        Returns:
            True if successful, False otherwise
        """
        if connector_id is None or get_connection_details is None:
            return False

        try:
            _, connector_info_dict = get_connection_details(connector_id)
            connector_info: S3ConnectorInfo = connector_info_dict  # type: ignore[assignment]
        except (KeyError, TypeError, ValueError) as e:
            return False

        try:
            s3_fs = self._create_s3_filesystem(connector_info)
            s3_path = self._construct_s3_path(connector_info, file_path)

            # Check if file exists
            if not s3_fs.exists(s3_path):
                return False

            # Delete file
            s3_fs.rm(s3_path)

            return True

        except Exception as e:
            middleman_debug(
                f"S3 delete error for path '{file_path}'", data=e, level="error"
            )
            return False

    def rename_file(
        self,
        file_path: str,
        new_name: str,
        connector_id: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
    ) -> bool:
        """
        Rename a file in S3.

        Args:
            file_path: Current relative path to the file in S3
            new_name: New name for the file (just the filename, not full path)
            connector_type: Type of file connector (should be FileConnectorType.s3bucket)
            connector_id: S3 connector ID
            get_connection_details: Callable to retrieve connection details

        Returns:
            True if successful, False otherwise
        """

        if connector_id is None or get_connection_details is None:
            return False

        try:
            _, connector_info_dict = get_connection_details(connector_id)
            connector_info: S3ConnectorInfo = connector_info_dict  # type: ignore[assignment]
        except (KeyError, TypeError, ValueError) as e:
            return False

        try:
            s3_fs = self._create_s3_filesystem(connector_info)
            old_s3_path = self._construct_s3_path(connector_info, file_path)

            # Check if file exists
            if not s3_fs.exists(old_s3_path):
                return False

            # Construct new path by replacing the filename
            parent_path = self._get_parent_path(file_path)
            if parent_path:
                new_relative_path = f"{parent_path}/{new_name}"
            else:
                new_relative_path = new_name

            new_s3_path = self._construct_s3_path(connector_info, new_relative_path)

            # Rename by copying and deleting (S3 doesn't have native rename)
            s3_fs.cp(old_s3_path, new_s3_path)
            s3_fs.rm(old_s3_path)

            return True

        except Exception as e:
            middleman_debug(
                f"S3 rename error for path '{file_path}'", data=e, level="error"
            )
            return False

    def get_signed_url(
        self,
        file_path: str,
        connector_id: str | None = None,
        expiration_seconds: int = 3600,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
    ) -> str | None:
        """
        Generate a presigned URL for downloading a file from S3.

        Args:
            file_path: Relative path to the file in S3
            connector_type: Type of file connector (should be FileConnectorType.s3bucket)
            connector_id: S3 connector ID
            expiration_seconds: URL expiration time in seconds (default: 3600 = 1 hour)
            get_connection_details: Callable to retrieve connection details

        Returns:
            Presigned URL string if successful, None otherwise
        """
        if connector_id is None or get_connection_details is None:
            return None

        try:
            _, connector_info_dict = get_connection_details(connector_id)
            connector_info: S3ConnectorInfo = connector_info_dict  # type: ignore[assignment]
        except (KeyError, TypeError, ValueError):
            return None

        try:
            # Create s3fs filesystem (reuses existing pattern)
            s3_fs = self._create_s3_filesystem(connector_info)
            bucket_name = connector_info["bucket_name"]

            # Build S3 object key (relative to bucket, including path_prefix)
            path_prefix_clean = connector_info.get("path_prefix", "").rstrip("/")
            normalized_path = file_path.strip("/") if file_path else ""

            if path_prefix_clean and normalized_path:
                object_key = f"{path_prefix_clean}/{normalized_path}"
            elif path_prefix_clean:
                object_key = path_prefix_clean
            elif normalized_path:
                object_key = normalized_path
            else:
                return None

            # Use underlying botocore client from s3fs to generate presigned URL
            url = s3_fs.s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": object_key},
                ExpiresIn=expiration_seconds,
            )

            return url

        except Exception:
            return None

    def teardown(self) -> None:
        """No cleanup needed for S3 datasources."""
        pass

    @staticmethod
    def connector_info_to_file_node(connector_info: S3ConnectorInfo) -> FileNode:
        """
        Convert S3 connector info to a FileNode representing the root bucket.

        Args:
            connector_info: S3ConnectorInfo containing connector information

        Returns:
            FileNode: FileNode representing the S3 bucket root
        """
        now = datetime.now(timezone.utc)
        connector_name = connector_info["name"]
        return FileNode(
            id=UUID(connector_info["connector_id"]),
            name=connector_info["name"],
            type=FileNodeType.directory,
            mount_type=FileConnectorType.s3bucket,
            connector_id=UUID(connector_info["connector_id"]),
            connector_name=connector_name,
            path=connector_info.get("path_prefix", ""),
            parent_id=None,
            size=None,
            mime_type=None,
            modified_at=None,
            created_at=None,
            health=MountHealth(
                status=MountHealthStatus.connected,
                last_checked=now,
                error_message=None,
            ),
        )
