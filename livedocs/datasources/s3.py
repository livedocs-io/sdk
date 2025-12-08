from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Callable
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
from livedocs.utils.common import _get_dataframe_schema
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

            # Use file_name as the S3 path (relative to bucket/prefix)
            # Download the file using get_file with preview=False to download full file
            local_file_path = self.download_file(
                path=file_name,
                connector_id=connector_id,
                get_connection_details=get_database_details,
                preview=False,
                connector_name=connector_name,
            )

            if local_file_path is None:
                raise ValueError(
                    f"Failed to download file from S3. File may not exist at path: {file_name}"
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
    ) -> list[FileNode]:
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

            # Use s3fs ls() with detail=True to get file information
            # This returns a list of dicts with file/directory info
            items = s3_fs.ls(s3_path, detail=True)

            for item in items:
                # s3fs returns items with 'Key' or 'name' field
                item_key = item.get("Key") or item.get("name", "")

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

                # Determine if it's a directory (ends with /) or file
                is_directory = item_key.endswith("/") or item.get("type") == "directory"

                # Skip the listing path itself
                if relative_path == normalized_path and is_directory:
                    continue

                name = relative_path.split("/")[-1]
                parent_path = self._get_parent_path(relative_path)

                file_id = self._generate_file_id(connector_id, relative_path)
                parent_id = (
                    self._generate_file_id(connector_id, parent_path)
                    if parent_path
                    else None
                )

                # Get metadata from s3fs item
                size = item.get("Size")
                last_modified = item.get("LastModified")
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

                # Get content type
                content_type = item.get("ContentType")

                nodes.append(
                    FileNode(
                        id=file_id,
                        name=name,
                        type=FileNodeType.directory
                        if is_directory
                        else FileNodeType.file,
                        mount_type=FileConnectorType.s3bucket,
                        connector_id=UUID(connector_id),
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
            # On error, return nodes found so far with error health status
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
        connector_name: str,
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

            # Download file
            s3_fs.download(s3_path, local_file_path)

            return local_file_path

        except Exception:
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
        except (KeyError, TypeError, ValueError):
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

        except Exception:
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
        except (KeyError, TypeError, ValueError):
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

        except Exception:
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
