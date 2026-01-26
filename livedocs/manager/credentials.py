import threading
import time
from typing import Any

from livedocs.types import (
    Credentials,
    DatabaseConnection,
    GoogleDriveConnectorInfo,
    S3ConnectorInfo,
    WorkspaceSecret,
)
from livedocs.utils.lib.internals import livedocs_internal_fetch_credentials


class CredentialStore:
    """Lazy credential loader with basic TTL caching."""

    def __init__(self, report_id: str, token: str, ttl_seconds: int = 300) -> None:
        self._report_id: str = report_id
        self._token: str = token
        self._ttl: int = ttl_seconds
        self._lock: threading.Lock = threading.Lock()
        self._bundle: Credentials | None = None
        self._loaded_at: float | None = None

    def load(self, force: bool = False) -> Credentials:
        with self._lock:
            now = time.time()
            if (
                not force
                and self._bundle is not None
                and self._loaded_at is not None
                and now - self._loaded_at < self._ttl
            ):
                return self._bundle

            raw = livedocs_internal_fetch_credentials(self._report_id, self._token)
            bundle = Credentials.model_validate(raw)
            self._bundle = bundle
            self._loaded_at = now
            return bundle

    def refresh(self) -> Credentials:
        return self.load(force=True)

    def get_secret(self, key: str) -> WorkspaceSecret | None:
        bundle = self.load()
        for secret in bundle.workspace_secrets.values():
            if secret.key == key:
                return secret
        return None

    def get_database(self, connector_id: str) -> DatabaseConnection | None:
        bundle = self.load()
        return bundle.databases.get(connector_id)

    def get_s3_connector(self, connector_id: str) -> S3ConnectorInfo | None:
        bundle = self.load()
        return bundle.s3_connectors.get(connector_id)

    def get_all_s3_connectors(self) -> list[S3ConnectorInfo]:
        bundle = self.load()
        return list[S3ConnectorInfo](bundle.s3_connectors.values())

    def get_all_google_drive_connectors(self) -> list[GoogleDriveConnectorInfo]:
        bundle = self.load()
        return list[GoogleDriveConnectorInfo](bundle.google_drive_connectors.values())

    def get_google_drive_connector(
        self, connector_id: str
    ) -> GoogleDriveConnectorInfo | None:
        bundle = self.load()
        return bundle.google_drive_connectors.get(connector_id)

    def get_built_in_vars(self) -> dict[str, Any | None]:
        bundle = self.load()
        return dict(bundle.built_in_vars)


class StaticCredentialStore(CredentialStore):
    """Minimal credential store that serves a pre-built bundle without network access."""

    def __init__(self, bundle: Credentials):
        super().__init__(report_id="static", token="static", ttl_seconds=0)
        self._static_bundle = bundle.model_copy(deep=True)

    def load(self, force: bool = False) -> Credentials:
        return self._static_bundle

    def refresh(self) -> Credentials:
        return self._static_bundle

    def get_secret(self, key: str) -> WorkspaceSecret | None:
        for secret in self._static_bundle.workspace_secrets.values():
            if secret.key == key:
                return secret
        return None

    def get_database(self, connector_id: str) -> DatabaseConnection | None:
        return self._static_bundle.databases.get(connector_id)

    def get_built_in_vars(self) -> dict[str, Any | None]:
        return dict(self._static_bundle.built_in_vars)
