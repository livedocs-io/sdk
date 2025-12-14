import os
import re
import tempfile
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any
from uuid import UUID, uuid5

import requests

from livedocs.types import (
    FileAction,
    FileConnectorType,
    FileManifest,
    FileManifestAction,
    FileNode,
    FileNodeType,
    GCSBucketType,
    ListPathResponse,
    MountHealth,
    MountHealthStatus,
)
from livedocs.utils.common import middleman_debug, get_xlsx_sheet_names

# Regex patterns for sanitizing sensitive data
_URI_CREDENTIALS_RE = re.compile(
    r"([a-zA-Z][a-zA-Z0-9+\-.]*://)([^:@/]+):([^@]+)@", re.IGNORECASE
)
_KEY_VALUE_RE = re.compile(
    r"(?P<prefix>(?:^|[^a-zA-Z0-9_])(?:password|secret|token|api[_-]?key|private_key)\s*(?:=|:)\s*)(?P<value>[^\s,;]+)",
    re.IGNORECASE,
)
_JSON_SECRET_RE = re.compile(
    r'("(?P<key>[^"]*(?:password|secret|token|private_key|apiKey)[^"]*)"\s*:\s*")(?P<value>[^"]*)(")',
    re.IGNORECASE,
)
_PEM_RE = re.compile(r"-----BEGIN [^-]+-----[\s\S]+?-----END [^-]+-----", re.IGNORECASE)


def livedocs_internal_sanitize_sensitive_data(message: str | None) -> str:
    """
    Best-effort scrubbing of secrets from error/log messages.
    Redacts credentials in URIs, obvious password/secret key patterns,
    and PEM/private key blobs.
    """
    if not message:
        return ""

    sanitized = _URI_CREDENTIALS_RE.sub(r"\1***:***@", message)
    sanitized = _KEY_VALUE_RE.sub(r"\g<prefix>***", sanitized)
    sanitized = _JSON_SECRET_RE.sub(r'\1***"', sanitized)
    sanitized = _PEM_RE.sub("-----REDACTED PRIVATE KEY-----", sanitized)

    return sanitized


def livedocs_internal_persist_built_in_vars(
    report_id: str | None, token: str | None, vars: dict[str, str]
) -> dict[str, str]:
    if report_id is None or token is None:
        raise ValueError("Report ID and token are required")

    CORE_URL = os.getenv("LIVEDOCS_CORE_BASE_URL")
    if not CORE_URL:
        raise ValueError("LIVEDOCS_CORE_BASE_URL environment variable not set")

    response = requests.post(
        f"{CORE_URL}/v1/vars/{report_id}",
        json=vars,
        headers={"authorization": token},
    )
    if response.status_code == 200:
        return response.json() if response.json() is not None else {}
    else:
        raise Exception(
            f"Failed to persist built-in vars. Status code: {response.status_code}"
        )


def livedocs_internal_fetch_credentials(report_id: str, token: str) -> dict[str, str]:
    CORE_URL = os.getenv("LIVEDOCS_CORE_BASE_URL")
    if not CORE_URL:
        raise ValueError("LIVEDOCS_CORE_BASE_URL environment variable not set")

    response = requests.get(
        f"{CORE_URL}/v1/credentials/{report_id}",
        headers={"authorization": token},
    )

    if response.status_code == 200:
        return response.json() if response.json() is not None else {}
    else:
        raise Exception(
            f"Failed to fetch credentials. Status code: {response.status_code}"
        )


def livedocs_internal_file_operation(
    report_id: str,
    token: str,
    file_id: str,
    action: FileAction,
    new_name: str | None = None,
) -> bool:
    CORE_URL = os.getenv("LIVEDOCS_CORE_BASE_URL")
    if not CORE_URL:
        raise ValueError("LIVEDOCS_CORE_BASE_URL environment variable not set")
    response = requests.post(
        f"{CORE_URL}/v1/files/{report_id}",
        json={
            "file_id": file_id,
            "action": action,
            "new_name": new_name if new_name else None,
        },
        headers={"authorization": token, "Content-Type": "application/json"},
    )

    if response.status_code == 200:
        return True
    else:
        raise Exception(f"Failed to {action} file. Status code: {response.status_code}")


@lru_cache(maxsize=128)
def livedocs_internal_fetch_file_manifest(
    report_id: str,
    token: str,
    action: FileManifestAction,
    bucket: GCSBucketType,
    file_id: str | None = None,
    file_name: str | None = None,
) -> FileManifest:
    CORE_URL = os.getenv("LIVEDOCS_CORE_BASE_URL")
    if not CORE_URL:
        raise ValueError("LIVEDOCS_CORE_BASE_URL environment variable not set")

    if not file_id and not file_name:
        raise ValueError(
            "Either file_id or file_name must be provided to fetch manifest."
        )

    if action not in {"write", "read"}:
        raise ValueError("Invalid action. Must be 'write' or 'read'.")

    payload: dict[str, Any] = {
        "action": action,
        "bucket": bucket,
    }

    if file_id:
        payload["file_id"] = file_id
    if file_name:
        payload["file_name"] = file_name

    try:
        api_url = f"{CORE_URL}/v1/manifest/{report_id}"
        response = requests.post(
            api_url,
            json=payload,
            headers={"authorization": token, "Content-Type": "application/json"},
        )

        response.raise_for_status()
        return FileManifest(**response.json())
    except requests.exceptions.HTTPError as e:
        if e.response is not None:
            status_code = e.response.status_code
            error_response_json: dict[str, Any] | None = None
            try:
                error_response_json = e.response.json()
                api_error_message = (
                    error_response_json.get("message", e.response.text)
                    if error_response_json
                    else e.response.text
                )
            except ValueError:
                api_error_message = e.response.text
            api_error_message = livedocs_internal_sanitize_sensitive_data(
                api_error_message
            )

            if status_code == 404:
                identifier = file_id or file_name
                raise FileNotFoundError(
                    f"File '{identifier}' not found. Error: {api_error_message}"
                ) from e
            elif status_code == 409:  # Should only occur if file_name was used
                conflicting_files_info = ""
                if (
                    error_response_json
                    and "files" in error_response_json
                    and isinstance(error_response_json["files"], list)
                ):
                    details = []
                    for f_info in error_response_json["files"]:
                        details.append(
                            f"  - ID: {f_info.get('id')}, Created: {f_info.get('created_at', 'N/A')}, Size: {f_info.get('size', 'N/A')} bytes"
                        )
                    if details:
                        conflicting_files_info = (
                            "\nConflicting file details:\n" + "\n".join(details)
                        )

                raise ValueError(
                    f"Ambiguous file name: '{file_name}'. Multiple files with this name exist.{conflicting_files_info}\n"
                    f"To resolve this, you can call livedocs.get_file(file_id='file_id') to download a specific file by ID."
                ) from e
            else:
                raise RuntimeError(
                    livedocs_internal_sanitize_sensitive_data(
                        f"Failed to get file manifest for '{file_name}'. Status: {status_code}. Error: {api_error_message}"
                    )
                )
        else:
            raise RuntimeError(
                livedocs_internal_sanitize_sensitive_data(
                    f"Failed to get file manifest for '{file_name}': {e}"
                )
            )
    except Exception as e:
        raise RuntimeError(
            livedocs_internal_sanitize_sensitive_data(
                f"An unexpected error occurred while fetching manifest for '{file_name}': {e}"
            )
        )


def _get_xlsx_sheet_nodes(
    xlsx_file: FileNode,
    report_id: str,
    token: str,
    signed_url: str | None = None,
) -> list[FileNode]:
    """
    Download an xlsx file and return FileNodes for each sheet.

    Args:
        xlsx_file: The xlsx FileNode from workspace
        report_id: Report ID for API calls
        token: Auth token for API calls

    Returns:
        List of FileNode objects representing sheets in the xlsx file
    """

    try:
        if not signed_url:
            # Get signed URL for the workspace file
            manifest = livedocs_internal_fetch_file_manifest(
                report_id=report_id,
                token=token,
                action="read",
                bucket=GCSBucketType.USER_FILES,
                file_name=xlsx_file.path.lstrip("/"),
            )

            if not manifest or not manifest.signed_url:
                return []

            file_signed_url = manifest.signed_url
        else:
            file_signed_url = signed_url

        # Download to temp file
        response = requests.get(file_signed_url)
        if response.status_code != 200:
            return []

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name

        # Get sheet names
        sheet_names = get_xlsx_sheet_names(tmp_path)

        # Clean up temp file
        os.unlink(tmp_path)

        if not sheet_names:
            return []

    except Exception:
        return []

    # Create FileNodes for each sheet
    now = datetime.now(timezone.utc)
    nodes: list[FileNode] = []

    # Use the xlsx file's ID as parent_id for sheets
    parent_id = xlsx_file.id

    # Create a namespace for generating sheet IDs based on parent file ID
    # This ensures sheet IDs are deterministic and unique per xlsx file
    for sheet_name in sheet_names:
        # Use :: as separator to distinguish sheet paths from directory paths
        sheet_path = f"{xlsx_file.path}::{sheet_name}"
        # Generate deterministic UUID for sheet based on parent ID and sheet name
        sheet_id = uuid5(parent_id, sheet_name)

        nodes.append(
            FileNode(
                id=sheet_id,
                name=sheet_name,
                type=FileNodeType.file,
                mount_type=FileConnectorType.workspace,
                connector_id=xlsx_file.connector_id,
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


def _get_xlsx_sheet_nodes_from_path(
    xlsx_path: str,
    report_id: str,
    token: str,
    signed_url: str | None = None,
) -> list[FileNode]:
    """
    Download an xlsx file by path and return FileNodes for each sheet.
    Used when expanding an xlsx file in the explorer (no FileNode available).

    Args:
        xlsx_path: The workspace file path (e.g., '/folder_id/stocks.xlsx')
        report_id: Report ID for API calls
        token: Auth token for API calls

    Returns:
        List of FileNode objects representing sheets in the xlsx file
    """
    from urllib.parse import unquote

    from livedocs.utils.common import get_xlsx_sheet_names

    # The path is encoded_name (e.g., "f28123c4-...%2Fstocks.xlsx")
    # Core API expects display_name (just the filename), not encoded_name
    # So we URL-decode and extract just the filename part
    decoded_path = unquote(xlsx_path.lstrip("/"))
    # Extract display_name (filename) from path like "workspace_id/filename.xlsx"
    file_name = decoded_path.split("/")[-1] if "/" in decoded_path else decoded_path

    try:
        # Save to LIVEDOCS_FILES_PATH for caching - preview will reuse this file
        files_path = os.getenv("LIVEDOCS_FILES_PATH")
        if not files_path:
            return []

        local_file_path = os.path.join(files_path, file_name)

        # Check if file already exists (cached)
        if os.path.exists(local_file_path):
            pass
        else:
            if not signed_url:
                # Get signed URL using display_name
                manifest = livedocs_internal_fetch_file_manifest(
                    report_id=report_id,
                    token=token,
                    action="read",
                    bucket=GCSBucketType.USER_FILES,
                    file_name=file_name,
                )

                if not manifest or not manifest.signed_url:
                    return []
                file_signed_url = manifest.signed_url
            else:
                file_signed_url = signed_url

            # Download to LIVEDOCS_FILES_PATH
            response = requests.get(file_signed_url)
            if response.status_code != 200:
                return []

            os.makedirs(files_path, exist_ok=True)
            with open(local_file_path, "wb") as f:
                f.write(response.content)

        # Get sheet names from local file
        sheet_names = get_xlsx_sheet_names(local_file_path)

        if not sheet_names:
            return []

    except Exception as e:
        middleman_debug(f"Error processing xlsx file at path: {xlsx_path}", e)
        return []

    # Create FileNodes for each sheet
    now = datetime.now(timezone.utc)
    nodes: list[FileNode] = []

    # Generate parent ID from path (deterministic)
    _DNS_NAMESPACE = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    _WORKSPACE_NAMESPACE = uuid5(_DNS_NAMESPACE, "workspace")
    parent_id = uuid5(_WORKSPACE_NAMESPACE, xlsx_path)

    for sheet_name in sheet_names:
        # Use :: as separator to distinguish sheet paths from directory paths
        sheet_path = f"{xlsx_path}::{sheet_name}"
        # Generate deterministic UUID for sheet
        sheet_id = uuid5(parent_id, sheet_name)

        nodes.append(
            FileNode(
                id=sheet_id,
                name=sheet_name,
                type=FileNodeType.file,
                mount_type=FileConnectorType.workspace,
                connector_id=None,
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


def livedocs_internal_list_files(
    report_id: str | None,
    token: str | None,
    database_parent_id: str | None = None,
    search_string: str | None = None,
) -> ListPathResponse:
    if report_id is None or token is None:
        raise ValueError("Report ID and token are required in IPython context")

    CORE_URL = os.getenv("LIVEDOCS_CORE_BASE_URL")
    if not CORE_URL:
        raise ValueError("LIVEDOCS_CORE_BASE_URL environment variable not set")

    # Handle xlsx file expansion - when user expands an xlsx file to see sheets
    # The Core API doesn't support this, so we handle it locally
    if database_parent_id and database_parent_id.lower().endswith(".xlsx"):
        sheet_nodes = _get_xlsx_sheet_nodes_from_path(
            database_parent_id, report_id, token
        )
        return ListPathResponse(files=sheet_nodes, schema_nodes=[])

    # Build request payload with optional parameters
    payload: dict[str, Any] = {}
    if database_parent_id:
        payload["parent_node_id"] = database_parent_id
    if search_string:
        payload["search_string"] = search_string

    # Make POST request with report_id in query string
    api_url = f"{CORE_URL}/v1/datasources/list?report_id={report_id}"
    response = requests.post(
        api_url,
        json=payload,
        headers={"authorization": token, "Content-Type": "application/json"},
    )

    if response.status_code == 200:
        response_data = response.json()
        result = ListPathResponse(**response_data)

        # Expand xlsx files to include their sheets as children
        xlsx_files = [f for f in result.files if f.path.lower().endswith(".xlsx")]
        for xlsx_file in xlsx_files:
            sheet_nodes = _get_xlsx_sheet_nodes(xlsx_file, report_id, token)
            result.files.extend(sheet_nodes)

        return result
    else:
        raise Exception(f"Failed to list path. Status code: {response.status_code}")


__all__ = [
    "livedocs_internal_persist_built_in_vars",
    "livedocs_internal_fetch_credentials",
    "livedocs_internal_sanitize_sensitive_data",
    "livedocs_internal_fetch_file_manifest",
    "livedocs_internal_list_files",
]
