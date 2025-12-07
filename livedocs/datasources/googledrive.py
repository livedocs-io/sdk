from __future__ import annotations

import io
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from uuid import UUID, uuid5

import polars as pl
from google.oauth2.credentials import Credentials as GoogleCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

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
    def _create_google_drive_service(
        connector_info: GoogleDriveConnectorInfo,
    ) -> Any:
        """
        Create a Google Drive API service client from connector info using existing OAuth tokens.

        Args:
            connector_info: Google Drive connector configuration

        Returns:
            Google Drive API service object
        """
        # Parse scopes from string
        scopes = (
            connector_info["scopes"].split(",") if connector_info.get("scopes") else []
        )
        scopes = [s.strip() for s in scopes if s.strip()]

        # Handle token_expiry conversion
        token_expiry = connector_info.get("token_expiry")
        if token_expiry is not None:
            # Handle string format from API (format: "2025-12-04 18:11:15.122967-08")
            if isinstance(token_expiry, str):
                try:
                    token_expiry = datetime.fromisoformat(token_expiry)
                except (ValueError, AttributeError):
                    token_expiry = None

            # Convert timezone-aware datetime to naive UTC datetime for google-auth
            if token_expiry is not None and isinstance(token_expiry, datetime):
                if token_expiry.tzinfo is not None:
                    token_expiry = token_expiry.astimezone(timezone.utc).replace(
                        tzinfo=None
                    )

        # Create Google OAuth2 credentials from existing tokens
        credentials = GoogleCredentials(
            token=connector_info["access_token"],
            refresh_token=connector_info["refresh_token"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id="",  # Not strictly needed for existing tokens
            client_secret="",  # Not strictly needed for existing tokens
            scopes=scopes,
            expiry=token_expiry,
        )

        # Build and return the Drive API service
        return build("drive", "v3", credentials=credentials)

    @staticmethod
    def _is_token_expired_or_expiring(
        token_expiry: datetime | str | None, buffer_minutes: int = 5
    ) -> bool:
        """
        Check if token is expired or about to expire.

        Args:
            token_expiry: Token expiry datetime (can be timezone-aware, naive, or string)
            buffer_minutes: Number of minutes before expiry to consider as "expiring"

        Returns:
            True if token is expired or will expire within buffer_minutes
        """
        if token_expiry is None:
            # If no expiry info, assume it might be expired and refresh proactively
            return True

        # Parse string format if needed
        if isinstance(token_expiry, str):
            try:
                token_expiry = datetime.fromisoformat(token_expiry)
            except (ValueError, AttributeError):
                return True

        if not isinstance(token_expiry, datetime):
            return True

        # Make timezone-aware if naive (assume UTC)
        if token_expiry.tzinfo is None:
            token_expiry = token_expiry.replace(tzinfo=timezone.utc)
        else:
            # Convert to UTC for comparison
            token_expiry = token_expiry.astimezone(timezone.utc)

        # Check if expired or expiring soon
        now = datetime.now(timezone.utc)
        buffer_time = timedelta(minutes=buffer_minutes)
        return token_expiry <= (now + buffer_time)

    @staticmethod
    def _get_connector_info_with_refresh(
        connector_id: str,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]],
        refresh_token_callback: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
    ) -> GoogleDriveConnectorInfo | None:
        """
        Get connector info and refresh token proactively if expired or expiring.

        Args:
            connector_id: Google Drive connector ID
            get_connection_details: Callable to retrieve connection details
            refresh_token_callback: Optional callback to refresh tokens if expired.
                Receives GoogleDriveConnectorInfo and returns updated one.

        Returns:
            GoogleDriveConnectorInfo dict, or None if retrieval failed
        """
        try:
            _, connector_info_dict = get_connection_details(connector_id)
            connector_info: GoogleDriveConnectorInfo = connector_info_dict  # type: ignore[assignment,arg-type]
        except (KeyError, TypeError, ValueError):
            return None

        # Check if token is expired or expiring, and refresh proactively if needed
        if refresh_token_callback:
            token_expiry = connector_info.get("token_expiry")
            if GoogleDriveDatasourceConnector._is_token_expired_or_expiring(
                token_expiry
            ):
                print(
                    f"DEBUG: Token expired or expiring (expiry: {token_expiry}), calling refresh callback..."
                )
                try:
                    connector_info = refresh_token_callback(connector_info)
                    print("DEBUG: Token refresh callback completed successfully")
                except Exception as e:
                    import traceback

                    print(
                        f"WARNING: Token refresh callback failed: {type(e).__name__}: {str(e)}"
                    )
                    print(traceback.format_exc())
                    # Continue with original connector_info if refresh fails

        return connector_info

    @staticmethod
    def _is_auth_error(error: Exception) -> bool:
        """
        Check if an HttpError is an authentication error.

        Args:
            error: Exception (should be HttpError)

        Returns:
            True if the error is a 401 or 403 authentication error
        """
        if not isinstance(error, HttpError):
            return False
        try:
            status_code = (
                error.resp.status
                if hasattr(error, "resp") and hasattr(error.resp, "status")
                else None
            )
            return status_code in (401, 403)
        except (AttributeError, TypeError):
            return False

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

    def _get_folder_id_from_path(self, service: Any, path: str | None) -> str:
        """
        Convert a path string to a Google Drive folder ID by traversing the folder hierarchy.

        Args:
            service: Authenticated Google Drive API service object
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
                f"name='{escaped_folder_name}' and "
                f"mimeType='application/vnd.google-apps.folder' and "
                f"trashed=false"
            )

            try:
                results = (
                    service.files()
                    .list(
                        q=query,
                        fields="files(id, name)",
                        pageSize=1,
                    )
                    .execute()
                )
                file_list = results.get("files", [])

                if not file_list:
                    # Folder not found, return root as fallback
                    return "root"

                # Use the first matching folder (assuming unique names per parent)
                current_folder_id = file_list[0]["id"]
            except Exception:
                # On any error, return root as fallback
                return "root"

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
        refresh_token_callback: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
    ) -> list[FileNode]:
        """
        List files and directories in Google Drive at the given path.

        Args:
            path: The Google Drive path to list. None means list is skipped.
            connector_id: The Google Drive connector ID. None means list is skipped.
            get_connection_details: Callable to retrieve connection details
            refresh_token_callback: Optional callback to refresh tokens if expired.
                Receives GoogleDriveConnectorInfo and returns updated one.

        Returns:
            List of FileNode objects representing files and directories
        """
        if connector_id is None or path is None or get_connection_details is None:
            return []

        connector_info = self._get_connector_info_with_refresh(
            connector_id, get_connection_details, refresh_token_callback
        )
        if connector_info is None:
            return []

        now = datetime.now(timezone.utc)
        nodes: list[FileNode] = []

        try:
            service = self._create_google_drive_service(connector_info)
            normalized_path = path.strip("/") if path else ""

            # Convert path to folder ID
            folder_id = self._get_folder_id_from_path(service, normalized_path)

            # Query for files and folders in the target folder
            query = f"'{folder_id}' in parents and trashed=false"

            results = (
                service.files()
                .list(
                    q=query,
                    fields="files(id, name, mimeType, modifiedTime, createdTime, size)",
                    pageSize=1000,
                )
                .execute()
            )
            file_list = results.get("files", [])

            for file_item in file_list:
                title = file_item.get("name", "")
                mime_type = file_item.get("mimeType", "")
                is_directory = mime_type == "application/vnd.google-apps.folder"

                # Build relative path
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

                modified_date_str = file_item.get("modifiedTime")
                if modified_date_str:
                    try:
                        # Google Drive dates are in RFC 3339 format
                        # Handle both "Z" suffix and timezone offsets
                        date_str = modified_date_str.replace("Z", "+00:00")
                        modified_at = datetime.fromisoformat(date_str)
                        if modified_at.tzinfo is None:
                            modified_at = modified_at.replace(tzinfo=timezone.utc)
                    except (ValueError, AttributeError, TypeError):
                        pass

                created_date_str = file_item.get("createdTime")
                if created_date_str:
                    try:
                        # Google Drive dates are in RFC 3339 format
                        # Handle both "Z" suffix and timezone offsets
                        date_str = created_date_str.replace("Z", "+00:00")
                        created_at = datetime.fromisoformat(date_str)
                        if created_at.tzinfo is None:
                            created_at = created_at.replace(tzinfo=timezone.utc)
                    except (ValueError, AttributeError, TypeError):
                        pass

                # Use modified_at as fallback for created_at
                if created_at is None:
                    created_at = modified_at

                # Get file size (not available for folders)
                size = None
                if not is_directory:
                    size_str = file_item.get("size")
                    if size_str:
                        try:
                            size = int(size_str)
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

    def _get_file_id_from_path(self, service: Any, file_path: str) -> str | None:
        """
        Get a file ID from a file path by traversing the folder hierarchy.

        Args:
            service: Authenticated Google Drive API service object
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
        parent_folder_id = self._get_folder_id_from_path(service, parent_path)

        # Query for file with this name in the parent folder
        # Escape single quotes in filename for the query
        escaped_filename = filename.replace("'", "\\'")
        query = (
            f"'{parent_folder_id}' in parents and "
            f"name='{escaped_filename}' and "
            f"trashed=false"
        )

        try:
            results = (
                service.files()
                .list(
                    q=query,
                    fields="files(id, name)",
                    pageSize=1,
                )
                .execute()
            )
            file_list = results.get("files", [])

            if not file_list:
                return None

            # Return the first matching file (assuming unique names per parent)
            return file_list[0]["id"]
        except Exception:
            return None

    def delete_file(
        self,
        file_path: str,
        connector_id: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
        refresh_token_callback: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
    ) -> bool:
        """
        Delete a file from Google Drive.

        Args:
            file_path: Relative path to the file in Google Drive
            connector_type: Type of file connector (should be FileConnectorType.googledrive)
            connector_id: Google Drive connector ID
            get_connection_details: Callable to retrieve connection details
            refresh_token_callback: Optional callback to refresh tokens if expired.
                Receives GoogleDriveConnectorInfo and returns updated one.

        Returns:
            True if successful, False otherwise
        """
        if connector_id is None or get_connection_details is None:
            return False

        connector_info = self._get_connector_info_with_refresh(
            connector_id, get_connection_details, refresh_token_callback
        )
        if connector_info is None:
            return False

        try:
            service = self._create_google_drive_service(connector_info)
            file_id = self._get_file_id_from_path(service, file_path)

            if file_id is None:
                return False

            # Delete file
            service.files().delete(fileId=file_id).execute()

            return True

        except Exception as e:
            import traceback

            print(f"ERROR in delete_file: {type(e).__name__}: {str(e)}")
            print(traceback.format_exc())
            return False

    def rename_file(
        self,
        file_path: str,
        new_name: str,
        connector_id: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
        refresh_token_callback: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
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
            refresh_token_callback: Optional callback to refresh tokens if expired.
                Receives GoogleDriveConnectorInfo and returns updated one.

        Returns:
            True if successful, False otherwise
        """
        if connector_id is None or get_connection_details is None:
            return False

        connector_info = self._get_connector_info_with_refresh(
            connector_id, get_connection_details, refresh_token_callback
        )
        if connector_info is None:
            return False

        try:
            service = self._create_google_drive_service(connector_info)
            file_id = self._get_file_id_from_path(service, file_path)

            if file_id is None:
                return False

            # Update file name
            service.files().update(
                fileId=file_id, body={"name": new_name}, fields="id"
            ).execute()

            return True

        except Exception as e:
            import traceback

            print(f"ERROR in rename_file: {type(e).__name__}: {str(e)}")
            print(traceback.format_exc())
            return False

    def upload_file_to_googledrive(
        self,
        file_path: str,
        connector_id: str | None = None,
        drive_path: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
        refresh_token_callback: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
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
            refresh_token_callback: Optional callback to refresh tokens if expired.
                Receives GoogleDriveConnectorInfo and returns updated one.

        Returns:
            True if successful, False otherwise
        """
        if connector_id is None or get_connection_details is None:
            return False

        import os

        if not os.path.exists(file_path):
            return False

        connector_info = self._get_connector_info_with_refresh(
            connector_id, get_connection_details, refresh_token_callback
        )
        if connector_info is None:
            return False

        try:
            service = self._create_google_drive_service(connector_info)

            # Get destination folder ID
            folder_id = self._get_folder_id_from_path(service, drive_path)
            print(f"DEBUG: Uploading to folder_id: {folder_id}")

            # Get filename from local path
            filename = os.path.basename(file_path)
            print(f"DEBUG: Uploading file: {filename} from {file_path}")

            # Create file metadata
            file_metadata = {"name": filename}
            if folder_id != "root":
                file_metadata["parents"] = [folder_id]
            print(f"DEBUG: File metadata: {file_metadata}")

            # Create media upload
            media = MediaFileUpload(file_path, resumable=True)

            # Upload file
            print(f"DEBUG: Starting file upload...")
            result = (
                service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            print(f"DEBUG: Upload successful: {result}")

            return True

        except Exception as e:
            import traceback

            error_msg = f"ERROR in save_file: {type(e).__name__}: {str(e)}"
            print(error_msg)
            if isinstance(e, HttpError):
                if hasattr(e, "resp"):
                    status = getattr(e.resp, "status", None)
                    print(f"  HTTP Status: {status}")
                if hasattr(e, "content"):
                    content_str = str(e.content)
                    print(
                        f"  Error Content: {content_str[:500]}"
                    )  # Limit to first 500 chars
            print(traceback.format_exc())
            return False

    def download_file(
        self,
        file_path: str,
        connector_type: FileConnectorType,
        connector_id: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
        refresh_token_callback: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
        | None = None,
    ) -> str | None:
        """
        Download a file from Google Drive and save it locally if it's less than 100MB.

        Args:
            file_path: Google Drive file path to download
            connector_type: Type of file connector (should be FileConnectorType.googledrive)
            connector_id: Google Drive connector ID
            get_connection_details: Callable to retrieve connection details
            refresh_token_callback: Optional callback to refresh tokens if expired.
                Receives GoogleDriveConnectorInfo and returns updated one.

        Returns:
            Local file path if successful, None otherwise
        """
        if connector_type != FileConnectorType.googledrive:
            return None

        if connector_id is None or get_connection_details is None:
            return None

        # Get the directory path from environment variable
        files_path = os.getenv("LIVEDOCS_FILES_PATH")
        if not files_path:
            print("ERROR: LIVEDOCS_FILES_PATH environment variable not set")
            return None

        # Ensure the directory exists
        os.makedirs(files_path, exist_ok=True)

        connector_info = self._get_connector_info_with_refresh(
            connector_id, get_connection_details, refresh_token_callback
        )
        if connector_info is None:
            return None

        try:
            service = self._create_google_drive_service(connector_info)
            file_id = self._get_file_id_from_path(service, file_path)

            if file_id is None:
                print(f"ERROR: File not found at path: {file_path}")
                return None

            # Get file metadata to check size
            file_metadata = (
                service.files().get(fileId=file_id, fields="name, size").execute()
            )

            file_name = file_metadata.get("name", os.path.basename(file_path))
            size_str = file_metadata.get("size")

            # Check if file size is available and less than 100MB
            if size_str:
                try:
                    file_size = int(size_str)
                    # 100MB in bytes
                    max_size = 100 * 1024 * 1024
                    if file_size >= max_size:
                        print(
                            f"ERROR: File size ({file_size} bytes) exceeds 100MB limit"
                        )
                        return None
                except (ValueError, TypeError):
                    print(
                        "WARNING: Could not parse file size, proceeding with download"
                    )

            # Download the file
            request = service.files().get_media(fileId=file_id)
            file_content = io.BytesIO()
            downloader = MediaIoBaseDownload(file_content, request)

            done = False
            while not done:
                status, done = downloader.next_chunk()

            # Save to local directory
            local_file_path = os.path.join(files_path, file_name)
            with open(local_file_path, "wb") as f:
                _ = f.write(file_content.getvalue())

            return local_file_path

        except Exception as e:
            import traceback

            error_msg = f"ERROR in download_file: {type(e).__name__}: {str(e)}"
            print(error_msg)
            if isinstance(e, HttpError):
                if hasattr(e, "resp"):
                    status = getattr(e.resp, "status", None)
                    print(f"  HTTP Status: {status}")
                if hasattr(e, "content"):
                    content_str = str(e.content)
                    print(
                        f"  Error Content: {content_str[:500]}"
                    )  # Limit to first 500 chars
            print(traceback.format_exc())
            return None

    def get_signed_url(
        self,
        file_path: str,
        connector_id: str | None = None,
        get_connection_details: Callable[[str], tuple[object, dict[str, Any]]]
        | None = None,
        refresh_token_callback: Callable[
            [GoogleDriveConnectorInfo], GoogleDriveConnectorInfo
        ]
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
            refresh_token_callback: Optional callback to refresh tokens if expired.
                Receives GoogleDriveConnectorInfo and returns updated one.

        Returns:
            Download URL string if successful, None otherwise
        """
        if connector_id is None or get_connection_details is None:
            return None

        connector_info = self._get_connector_info_with_refresh(
            connector_id, get_connection_details, refresh_token_callback
        )
        if connector_info is None:
            return None

        try:
            service = self._create_google_drive_service(connector_info)
            file_id = self._get_file_id_from_path(service, file_path)

            if file_id is None:
                return None

            # Get file metadata to get webContentLink
            file_metadata = (
                service.files().get(fileId=file_id, fields="webContentLink").execute()
            )

            # Get the webContentLink (download URL)
            # This URL requires OAuth authentication to access
            download_url = file_metadata.get("webContentLink")

            if not download_url:
                return None

            return download_url

        except Exception:
            return None

    def teardown(self) -> None:
        """No cleanup needed for Google Drive datasources."""
        pass
