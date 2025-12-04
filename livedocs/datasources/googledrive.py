from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable
from uuid import UUID, uuid5

import polars as pl
from google.oauth2.credentials import Credentials as GoogleCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

from livedocs.datasources.base import BaseDatasourceConnector
from livedocs.types import (
    DBSaveConfig,
    ElementDataSource,
    FileConnectorType,
    FileNode,
    FileNodeType,
    GoogleDriveConnectorInfo,
    LivedocsResult,
    MountHealth,
    MountHealthStatus,
    QueryResult,
    QueryResultMetadata,
)
from livedocs.utils.lib.internals import (
    livedocs_internal_sanitize_sensitive_data as sanitize_sensitive_data,
)


class GoogleDriveDatasourceConnector(BaseDatasourceConnector):
    """
    Datasource connector for Google Drive connections.
    """

    @staticmethod
    def _create_google_drive_client(
        connector_info: GoogleDriveConnectorInfo,
    ) -> GoogleDrive:
        """
        Create a GoogleDrive client from connector info using existing OAuth tokens.

        Args:
            connector_info: Google Drive connector configuration

        Returns:
            GoogleDrive: Authenticated GoogleDrive instance
        """
        # Create GoogleAuth instance
        gauth = GoogleAuth()

        # pydrive2 uses oauth2client internally, which is deprecated but still works
        # We'll create OAuth2Credentials directly from the tokens
        try:
            from oauth2client.client import OAuth2Credentials
        except ImportError:
            # Fallback: try to use google-auth and convert
            # Create Google OAuth2 credentials from existing tokens
            credentials = GoogleCredentials(
                token=connector_info["access_token"],
                refresh_token=connector_info["refresh_token"],
                token_uri="https://oauth2.googleapis.com/token",
                client_id="",  # May be needed for refresh
                client_secret="",  # May be needed for refresh
                scopes=connector_info["scopes"].split(",")
                if connector_info.get("scopes")
                else [],
            )
            # Try to set credentials directly - pydrive2 might accept google-auth credentials
            gauth.credentials = credentials
            return GoogleDrive(gauth)

        # Use oauth2client if available (pydrive2's preferred method)
        oauth2_credentials = OAuth2Credentials(
            access_token=connector_info["access_token"],
            client_id="",  # Not strictly needed for existing tokens
            client_secret="",
            refresh_token=connector_info["refresh_token"],
            token_expiry=connector_info.get("token_expiry"),
            token_uri="https://oauth2.googleapis.com/token",
            user_agent=None,
        )
        gauth.credentials = oauth2_credentials

        return GoogleDrive(gauth)

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

    def _get_folder_id_from_path(self, drive: GoogleDrive, path: str | None) -> str:
        """
        Convert a path string to a Google Drive folder ID by traversing the folder hierarchy.

        Args:
            drive: Authenticated GoogleDrive instance
            path: Path string (e.g., "folder1/subfolder2") or None for root

        Returns:
            str: Folder ID, or "root" if path is empty/None
        """
        if not path or path.strip() == "" or path.strip() == "/":
            return "root"

        # Normalize path: remove leading/trailing slashes and split
        path_parts = [p for p in path.strip("/").split("/") if p]
        if not path_parts:
            return "root"

        current_folder_id = "root"

        # Traverse path parts to find the target folder
        for folder_name in path_parts:
            # Query for folders with this name in the current parent
            # Escape single quotes in folder name for the query
            escaped_folder_name = folder_name.replace("'", "\\'")
            query = (
                f"'{current_folder_id}' in parents and "
                f"title='{escaped_folder_name}' and "
                f"mimeType='application/vnd.google-apps.folder' and "
                f"trashed=false"
            )
            file_list = drive.ListFile({"q": query}).GetList()

            if not file_list:
                # Folder not found, return root as fallback
                return "root"

            # Use the first matching folder (assuming unique names per parent)
            current_folder_id = file_list[0]["id"]

        return current_folder_id

    def read(
        self,
        query: str,
        datasource: ElementDataSource,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
        **kwargs: Any,
    ) -> tuple[pl.DataFrame, dict[str, str]]:
        """
        Execute a query against a Google Drive datasource (no-op skeleton).

        Args:
            query: SQL query string to execute
            datasource: Datasource configuration
            get_database_details: Callable to retrieve database credentials
            **kwargs: Additional arguments (not used)

        Returns:
            Tuple containing:
            - Empty DataFrame
            - Empty schema dict
        """
        return pl.DataFrame(), {}

    def write(
        self,
        df: pl.DataFrame,
        save_config: DBSaveConfig,
        get_database_details: Callable[[str], tuple[object, dict[str, Any]]],
    ) -> LivedocsResult:
        """
        Write to a Google Drive datasource (no-op skeleton).

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
        List files and directories in Google Drive at the given path.

        Args:
            path: The Google Drive path to list. None means list is skipped.
            connector_id: The Google Drive connector ID. None means list is skipped.
            get_connection_details: Callable to retrieve connection details

        Returns:
            List of FileNode objects representing files and directories
        """
        if connector_id is None or path is None or get_connection_details is None:
            return []

        try:
            _, connector_info_dict = get_connection_details(connector_id)
            # TypedDict is for type hints only, we can use the dict directly
            connector_info: GoogleDriveConnectorInfo = connector_info_dict  # type: ignore[assignment,arg-type]
        except (KeyError, TypeError, ValueError):
            return []

        drive = None
        now = datetime.now(timezone.utc)
        nodes: list[FileNode] = []

        try:
            drive = self._create_google_drive_client(connector_info)
            normalized_path = path.strip("/") if path else ""

            # Convert path to folder ID
            folder_id = self._get_folder_id_from_path(drive, normalized_path)

            # Query for files and folders in the target folder
            query = f"'{folder_id}' in parents and trashed=false"
            file_list = drive.ListFile({"q": query}).GetList()

            for file_item in file_list:
                title = file_item["title"]
                mime_type = file_item.get("mimeType", "")
                is_directory = mime_type == "application/vnd.google-apps.folder"

                # Build relative path
                # For now, use title as the path (we can enhance this later with full path resolution)
                if normalized_path:
                    relative_path = (
                        f"{normalized_path}/{title}" if normalized_path else title
                    )
                else:
                    relative_path = title

                # Skip if this is the listing path itself (a directory with same name)
                if relative_path == normalized_path and is_directory:
                    continue

                parent_path = self._get_parent_path(relative_path)

                # Generate deterministic UUIDs
                file_id = self._generate_file_id(connector_id, relative_path)
                parent_id = (
                    self._generate_file_id(connector_id, parent_path)
                    if parent_path
                    else None
                )

                # Parse dates
                modified_at = None
                created_at = None

                modified_date_str = file_item.get("modifiedDate")
                if modified_date_str:
                    try:
                        # Google Drive dates are in RFC 3339 format
                        modified_at = datetime.fromisoformat(
                            modified_date_str.replace("Z", "+00:00")
                        )
                        if modified_at.tzinfo is None:
                            modified_at = modified_at.replace(tzinfo=timezone.utc)
                    except (ValueError, AttributeError):
                        pass

                created_date_str = file_item.get("createdDate")
                if created_date_str:
                    try:
                        created_at = datetime.fromisoformat(
                            created_date_str.replace("Z", "+00:00")
                        )
                        if created_at.tzinfo is None:
                            created_at = created_at.replace(tzinfo=timezone.utc)
                    except (ValueError, AttributeError):
                        pass

                # Use modified_at as fallback for created_at
                if created_at is None:
                    created_at = modified_at

                # Get file size (not available for folders)
                size = None
                if not is_directory:
                    size = file_item.get("fileSize")
                    if size:
                        try:
                            size = int(size)
                        except (ValueError, TypeError):
                            size = None

                nodes.append(
                    FileNode(
                        id=file_id,
                        name=title,
                        type=FileNodeType.directory
                        if is_directory
                        else FileNodeType.file,
                        mount_type=FileConnectorType.googledrive,
                        connector_id=UUID(connector_id),
                        path=relative_path,
                        parent_id=parent_id,
                        size=size,
                        mime_type=mime_type if not is_directory else None,
                        modified_at=modified_at,
                        created_at=created_at,
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

    def _get_file_id_from_path(self, drive: GoogleDrive, file_path: str) -> str | None:
        """
        Get a file ID from a file path by traversing the folder hierarchy.

        Args:
            drive: Authenticated GoogleDrive instance
            file_path: Full path to the file (e.g., "folder1/subfolder2/file.txt")

        Returns:
            File ID if found, None otherwise
        """
        if not file_path or file_path.strip() == "":
            return None

        # Split path into directory and filename
        path_parts = [p for p in file_path.strip("/").split("/") if p]
        if not path_parts:
            return None

        filename = path_parts[-1]
        parent_path = "/".join(path_parts[:-1]) if len(path_parts) > 1 else ""

        # Get parent folder ID
        parent_folder_id = self._get_folder_id_from_path(drive, parent_path)

        # Query for file with this name in the parent folder
        # Escape single quotes in filename for the query
        escaped_filename = filename.replace("'", "\\'")
        query = (
            f"'{parent_folder_id}' in parents and "
            f"title='{escaped_filename}' and "
            f"trashed=false"
        )
        file_list = drive.ListFile({"q": query}).GetList()

        if not file_list:
            return None

        # Return the first matching file (assuming unique names per parent)
        return file_list[0]["id"]

    def delete_file(
        self,
        file_path: str,
        connector_type: FileConnectorType,
        connector_id: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
    ) -> bool:
        """
        Delete a file from Google Drive.

        Args:
            file_path: Relative path to the file in Google Drive
            connector_type: Type of file connector (should be FileConnectorType.googledrive)
            connector_id: Google Drive connector ID
            get_connection_details: Callable to retrieve connection details

        Returns:
            True if successful, False otherwise
        """
        if connector_type != FileConnectorType.googledrive:
            return False

        if connector_id is None or get_connection_details is None:
            return False

        try:
            _, connector_info_dict = get_connection_details(connector_id)
            connector_info: GoogleDriveConnectorInfo = connector_info_dict  # type: ignore[assignment,arg-type]
        except (KeyError, TypeError, ValueError):
            return False

        try:
            drive = self._create_google_drive_client(connector_info)
            file_id = self._get_file_id_from_path(drive, file_path)

            if file_id is None:
                return False

            # Get file and delete it
            file_obj = drive.CreateFile({"id": file_id})
            file_obj.Delete()

            return True

        except Exception:
            return False

    def rename_file(
        self,
        file_path: str,
        new_name: str,
        connector_type: FileConnectorType,
        connector_id: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
    ) -> bool:
        """
        Rename a file in Google Drive.

        Args:
            file_path: Current relative path to the file in Google Drive
            new_name: New name for the file (just the filename, not full path)
            connector_type: Type of file connector (should be FileConnectorType.googledrive)
            connector_id: Google Drive connector ID
            get_connection_details: Callable to retrieve connection details

        Returns:
            True if successful, False otherwise
        """
        if connector_type != FileConnectorType.googledrive:
            return False

        if connector_id is None or get_connection_details is None:
            return False

        try:
            _, connector_info_dict = get_connection_details(connector_id)
            connector_info: GoogleDriveConnectorInfo = connector_info_dict  # type: ignore[assignment,arg-type]
        except (KeyError, TypeError, ValueError):
            return False

        try:
            drive = self._create_google_drive_client(connector_info)
            file_id = self._get_file_id_from_path(drive, file_path)

            if file_id is None:
                return False

            # Get file and update its title
            file_obj = drive.CreateFile({"id": file_id})
            file_obj["title"] = new_name
            file_obj.Upload()

            return True

        except Exception:
            return False

    def save_file(
        self,
        file_path: str,
        connector_type: FileConnectorType,
        connector_id: str | None = None,
        drive_path: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
    ) -> bool:
        """
        Upload a local file to Google Drive.

        Args:
            file_path: Local file path to upload
            connector_type: Type of file connector (should be FileConnectorType.googledrive)
            connector_id: Google Drive connector ID
            drive_path: Destination folder path in Google Drive. If None, uploads to root
            get_connection_details: Callable to retrieve connection details

        Returns:
            True if successful, False otherwise
        """
        if connector_type != FileConnectorType.googledrive:
            return False

        if connector_id is None or get_connection_details is None:
            return False

        import os

        if not os.path.exists(file_path):
            return False

        try:
            _, connector_info_dict = get_connection_details(connector_id)
            connector_info: GoogleDriveConnectorInfo = connector_info_dict  # type: ignore[assignment,arg-type]
        except (KeyError, TypeError, ValueError):
            return False

        try:
            drive = self._create_google_drive_client(connector_info)

            # Get destination folder ID
            folder_id = self._get_folder_id_from_path(drive, drive_path)

            # Get filename from local path
            filename = os.path.basename(file_path)

            # Create file metadata
            file_metadata = {"title": filename}
            if folder_id != "root":
                file_metadata["parents"] = [{"id": folder_id}]

            # Create and upload file
            file_obj = drive.CreateFile(file_metadata)
            file_obj.SetContentFile(file_path)
            file_obj.Upload()

            return True

        except Exception:
            return False

    def get_signed_url(
        self,
        file_path: str,
        connector_type: FileConnectorType,
        connector_id: str | None = None,
        expiration_seconds: int = 3600,  # noqa: ARG002
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
    ) -> str | None:
        """
        Get a download URL for a file in Google Drive.

        Note: Unlike S3 signed URLs, this URL requires OAuth authentication.
        The URL is valid as long as the OAuth session is valid.
        The expiration_seconds parameter is kept for API compatibility with S3
        but is not used (Google Drive URLs don't have time-based expiration).

        Args:
            file_path: Relative path to the file in Google Drive
            connector_type: Type of file connector (should be FileConnectorType.googledrive)
            connector_id: Google Drive connector ID
            expiration_seconds: Not used for Google Drive (kept for API compatibility with S3)
            get_connection_details: Callable to retrieve connection details

        Returns:
            Download URL string if successful, None otherwise
        """
        if connector_type != FileConnectorType.googledrive:
            return None

        if connector_id is None or get_connection_details is None:
            return None

        try:
            _, connector_info_dict = get_connection_details(connector_id)
            connector_info: GoogleDriveConnectorInfo = connector_info_dict  # type: ignore[assignment,arg-type]
        except (KeyError, TypeError, ValueError):
            return None

        try:
            drive = self._create_google_drive_client(connector_info)
            file_id = self._get_file_id_from_path(drive, file_path)

            if file_id is None:
                return None

            # Get file and fetch metadata to get webContentLink
            file_obj = drive.CreateFile({"id": file_id})
            file_obj.FetchMetadata()

            # Get the webContentLink (download URL)
            # This URL requires OAuth authentication to access
            # Format: https://drive.google.com/uc?id=FILE_ID&export=download
            download_url = file_obj.get("webContentLink")  # type: ignore[call-overload]

            if not download_url:
                return None

            return download_url

        except Exception:
            return None

    def teardown(self) -> None:
        """No cleanup needed for Google Drive datasources."""
        pass
