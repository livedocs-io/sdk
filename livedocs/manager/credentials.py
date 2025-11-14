import threading
import time
from typing import Any, Optional

from livedocs.types import Credentials, DatabaseConnection, WorkspaceSecret
from livedocs.utils.common import _fetch_credentials


class CredentialStore:
    """Lazy credential loader with basic TTL caching."""

    def __init__(self, report_id: str, token: str, ttl_seconds: int = 300) -> None:
        self._report_id = report_id
        self._token = token
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        self._bundle: Optional[Credentials] = None
        self._loaded_at: Optional[float] = None

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

            raw = _fetch_credentials(self._report_id, self._token)

            bundle = Credentials.model_validate(raw)

            self._bundle = bundle
            self._loaded_at = now
            return bundle

    def refresh(self) -> Credentials:
        return self.load(force=True)

    def get_secret(self, key: str) -> Optional[WorkspaceSecret]:
        bundle = self.load()
        return bundle.workspace_secrets.get(key)

    def get_database(self, connector_id: str) -> Optional[DatabaseConnection]:
        bundle = self.load()
        return bundle.databases.get(connector_id)

    def get_built_in_vars(self) -> dict[str, Optional[Any]]:
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

    def get_secret(self, key: str) -> Optional[WorkspaceSecret]:
        return self._static_bundle.workspace_secrets.get(key)

    def get_database(self, connector_id: str) -> Optional[DatabaseConnection]:
        return self._static_bundle.databases.get(connector_id)

    def get_built_in_vars(self) -> dict[str, Optional[Any]]:
        return dict(self._static_bundle.built_in_vars)
