import os
from functools import wraps

import requests
import sentry_sdk


def livedocs_internal_instrument(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            sanitized_args = tuple(sanitize_sensitive_data(str(arg)) for arg in e.args)
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

    CORE_URL = os.getenv("CORE_BASE_URL")
    if not CORE_URL:
        raise ValueError("CORE_BASE_URL environment variable not set")

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
    CORE_URL = os.getenv("CORE_BASE_URL")
    if not CORE_URL:
        raise ValueError("CORE_BASE_URL environment variable not set")

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


__all__ = [
    "livedocs_internal_persist_built_in_vars",
    "livedocs_internal_instrument",
    "livedocs_internal_fetch_credentials",
]
