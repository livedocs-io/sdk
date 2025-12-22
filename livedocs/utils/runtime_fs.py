import mimetypes
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List
from uuid import UUID, uuid5

from livedocs.types import (
    FileConnectorType,
    FileNode,
    FileNodeType,
    MountHealth,
    MountHealthStatus,
)
from livedocs.utils.common import get_xlsx_sheet_names


def _get_mime_type(file_path: Path) -> str | None:
    """Get mime type from file extension."""
    if file_path.is_dir():
        return None

    # Use mimetypes to guess from filename
    guessed_type, _ = mimetypes.guess_type(str(file_path))
    if guessed_type:
        return guessed_type

    # Fallback for common data formats not in mimetypes
    ext = file_path.suffix.lower().lstrip(".")
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


def list_runtime_files_top_level() -> list[FileNode]:
    """
    List all files and directories at the top level of the configured files path.

    Returns:
        list[FileNode]: List of FileNode objects representing files and directories
                       at the top level. Returns empty list if path doesn't exist
                       or can't be accessed.
    """
    runtime_file_nodes = []
    files_path = os.getenv("LIVEDOCS_FILES_PATH")

    if not files_path:
        return runtime_file_nodes

    list_path_obj = Path(files_path)

    # Check if path exists
    if not list_path_obj.exists() or not list_path_obj.is_dir():
        return runtime_file_nodes

    now = datetime.now(timezone.utc)

    # List all items in the directory
    try:
        items = list_path_obj.iterdir()
        for item in items:
            # Determine if it's a directory or file
            is_directory = item.is_dir()
            name = item.name

            # For top level, relative path is just the name
            relative_path = name

            # Normalize path separators
            relative_path = relative_path.replace("\\", "/")

            # Generate IDs (parent_id is None for top level)
            file_id = _generate_file_id(relative_path)
            parent_id = None

            # Get file metadata
            size = None
            modified_at = None
            created_at = None

            if item.is_file():
                try:
                    stat = item.stat()
                    size = stat.st_size
                    modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                    created_at = datetime.fromtimestamp(stat.st_ctime, tz=timezone.utc)
                except (OSError, ValueError):
                    pass
            elif item.is_dir():
                try:
                    stat = item.stat()
                    modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                    created_at = datetime.fromtimestamp(stat.st_ctime, tz=timezone.utc)
                except (OSError, ValueError):
                    pass

            runtime_file_nodes.append(
                FileNode(
                    id=file_id,
                    name=name,
                    type=FileNodeType.directory if is_directory else FileNodeType.file,
                    mount_type=FileConnectorType.runtime,
                    connector_id=None,  # Runtime doesn't have a connector_id
                    connector_name="runtime",
                    path=relative_path,
                    parent_id=parent_id,
                    size=size,
                    mime_type=_get_mime_type(item),
                    modified_at=modified_at,
                    created_at=created_at,
                    health=MountHealth(
                        status=MountHealthStatus.connected,
                        last_checked=now,
                        error_message=None,
                    ),
                )
            )
    except (OSError, PermissionError):
        # If we can't list the directory, just return empty list
        # Don't raise error to avoid breaking the entire function
        pass

    return runtime_file_nodes


# Fixed namespace UUID for runtime files (generated from "runtime" string using DNS namespace)
# Using DNS namespace UUID and "runtime" as the name to create a deterministic namespace
_DNS_NAMESPACE = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
_RUNTIME_NAMESPACE = uuid5(_DNS_NAMESPACE, "runtime")


def _generate_file_id(file_path: str) -> UUID:
    """Generate a deterministic UUID from file_path using fixed runtime namespace."""
    return uuid5(_RUNTIME_NAMESPACE, file_path)


def _get_parent_path(file_path: str) -> str | None:
    """Extract parent directory path."""
    file_path = file_path.rstrip("/")
    if not file_path or file_path == "/":
        return None
    parent = "/".join(file_path.split("/")[:-1])
    return parent if parent else None


def _list_xlsx_sheets(xlsx_path: Path, relative_path: str) -> list[FileNode]:
    """
    List sheets in an xlsx file as virtual FileNodes.

    Args:
        xlsx_path: Absolute path to the xlsx file
        relative_path: Relative path from the configured files path

    Returns:
        list[FileNode]: List of FileNode objects representing sheets
    """
    sheet_names = get_xlsx_sheet_names(str(xlsx_path))
    if not sheet_names:
        return []

    now = datetime.now(timezone.utc)
    nodes = []

    # Generate parent file ID (the xlsx file itself)
    parent_id = _generate_file_id(relative_path)

    for sheet_name in sheet_names:
        # Use :: as separator to distinguish sheet paths from directory paths
        sheet_path = f"{relative_path}::{sheet_name}"
        sheet_id = _generate_file_id(sheet_path)

        nodes.append(
            FileNode(
                id=sheet_id,
                name=sheet_name,
                type=FileNodeType.file,
                mount_type=FileConnectorType.runtime,
                connector_id=None,
                connector_name="runtime",
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


def list_runtime_files_in_path(
    path: str, search_string: str | None = None, max_depth: int = 5
) -> List[FileNode]:
    """
    List files and directories at the specified path relative to the configured files path.
    Optionally filter by search_string.

    Args:
        path: Relative path from configured files path (e.g., "folder/subfolder")
        search_string: Optional search string to filter file/directory names

    Returns:
        list[FileNode]: List of FileNode objects representing files and directories

    Raises:
        ValueError: If path doesn't exist or is a file (not a directory)
    """
    files_path = os.getenv("LIVEDOCS_FILES_PATH")
    if not files_path:
        raise ValueError("LIVEDOCS_FILES_PATH environment variable is not set")

    # Construct full path
    base_path = Path(files_path)
    if path:
        # Normalize path separators
        normalized_path = path.replace("\\", "/").strip("/")
        full_path = base_path / normalized_path
    else:
        full_path = base_path

    # Check if path exists
    if not full_path.exists():
        raise ValueError(f"Path not found: {path}")

    # Check if this is an xlsx file - return sheets as children
    if full_path.is_file() and full_path.suffix.lower() == ".xlsx":
        normalized_path = path.replace("\\", "/").strip("/") if path else ""
        return _list_xlsx_sheets(full_path, normalized_path)

    # Ensure it's a directory
    if not full_path.is_dir():
        raise ValueError(f"Path is not a directory: {path}")

    now = datetime.now(timezone.utc)
    runtime_file_nodes = []

    def walk(current_path: Path, depth: int):
        nonlocal runtime_file_nodes
        if depth > max_depth:
            return
        try:
            for item in current_path.iterdir():
                name = item.name
                # Always recurse dirs even if name doesn't match; filter files by search
                is_directory = item.is_dir()

                try:
                    relative_path = str(item.relative_to(base_path))
                except ValueError:
                    normalized_path = path.replace("\\", "/").strip("/") if path else ""
                    relative_path = (
                        f"{normalized_path}/{name}" if normalized_path else name
                    )
                relative_path = relative_path.replace("\\", "/")

                matches = search_string is None or search_string.lower() in name.lower()

                file_id = _generate_file_id(relative_path)
                parent_path = _get_parent_path(relative_path)
                parent_id = _generate_file_id(parent_path) if parent_path else None

                size = None
                modified_at = None
                created_at = None
                try:
                    stat = item.stat()
                    size = stat.st_size if item.is_file() else None
                    modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
                    created_at = datetime.fromtimestamp(stat.st_ctime, tz=timezone.utc)
                except (OSError, ValueError):
                    pass

                if is_directory:
                    if matches:
                        runtime_file_nodes.append(
                            FileNode(
                                id=file_id,
                                name=name,
                                type=FileNodeType.directory,
                                mount_type=FileConnectorType.runtime,
                                connector_id=None,
                                connector_name="runtime",
                                path=relative_path
                                if relative_path.startswith("/")
                                else f"/{relative_path}",
                                parent_id=parent_id,
                                size=None,
                                mime_type=None,
                                modified_at=modified_at,
                                created_at=created_at,
                                health=MountHealth(
                                    status=MountHealthStatus.connected,
                                    last_checked=now,
                                    error_message=None,
                                ),
                            )
                        )
                    walk(item, depth + 1)
                else:
                    if search_string and not matches:
                        continue
                    runtime_file_nodes.append(
                        FileNode(
                            id=file_id,
                            name=name,
                            type=FileNodeType.file,
                            mount_type=FileConnectorType.runtime,
                            connector_id=None,
                            connector_name="runtime",
                            path=relative_path
                            if relative_path.startswith("/")
                            else f"/{relative_path}",
                            parent_id=parent_id,
                            size=size,
                            mime_type=_get_mime_type(item),
                            modified_at=modified_at,
                            created_at=created_at,
                            health=MountHealth(
                                status=MountHealthStatus.connected,
                                last_checked=now,
                                error_message=None,
                            ),
                        )
                    )
        except (OSError, PermissionError) as e:
            raise ValueError(f"Error listing directory '{current_path}': {str(e)}")

    walk(full_path, 0)

    return runtime_file_nodes
