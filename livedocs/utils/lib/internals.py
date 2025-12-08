import os
import re
from functools import lru_cache, wraps
from typing import Any

import requests
import sentry_sdk

from livedocs.types import (
    FileManifest,
    FileManifestAction,
    GCSBucketType,
    ListPathResponse,
    SchemaNodeType,
    FileAction,
)


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


def livedocs_internal_instrument(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            sanitized_args = tuple(
                livedocs_internal_sanitize_sensitive_data(str(arg)) for arg in e.args
            )
            if sanitized_args:
                e.args = sanitized_args
            sentry_sdk.capture_exception(e)
            raise  # Re-raise the exception after capturing it

    return wrapper


@livedocs_internal_instrument
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


@livedocs_internal_instrument
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


@livedocs_internal_instrument
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


@livedocs_internal_instrument
def livedocs_internal_list_files(
    report_id: str,
    token: str,
    database_parent_id: str | None = None,
    search_string: str | None = None,
) -> ListPathResponse:
    CORE_URL = os.getenv("LIVEDOCS_CORE_BASE_URL")
    if not CORE_URL:
        raise ValueError("LIVEDOCS_CORE_BASE_URL environment variable not set")

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
        return ListPathResponse(**response_data)
    else:
        raise Exception(f"Failed to list path. Status code: {response.status_code}")


def livedocs_internal_setup_sentry():
    """
    Initializes Sentry for error tracking and performance monitoring.
    """
    dsn = os.getenv("LIVEDOCS_PY_SDK_SENTRY_DSN")
    if not dsn:
        return

    try:
        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=1
            if os.getenv("LIVEDOCS_APP_ENV") != "production"
            else 0.2,
            profiles_sample_rate=1
            if os.getenv("LIVEDOCS_APP_ENV") != "production"
            else 0.2,
            environment=os.getenv("LIVEDOCS_APP_ENV"),
        )
    except Exception as e:
        raise RuntimeError("Failed to initialize Sentry") from e


__all__ = [
    "livedocs_internal_persist_built_in_vars",
    "livedocs_internal_instrument",
    "livedocs_internal_fetch_credentials",
    "livedocs_internal_sanitize_sensitive_data",
    "livedocs_internal_fetch_file_manifest",
    "livedocs_internal_list_files",
    "livedocs_internal_setup_sentry",
]
