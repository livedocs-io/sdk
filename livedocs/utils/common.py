import requests

from livedocs.types import Credentials

# TODO: Change this to the actual URL
CORE_URL = "http://localhost:4000"


def _fetch_credentials(report_id: str, token: str) -> Credentials:
    response = requests.get(
        f"{CORE_URL}/v1/credentials/{report_id}",
        headers={"authorization": token},
    )
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(
            f"Failed to fetch credentials. Status code: {response.status_code}"
        )


def _fetch_file_manifest(file_id: str, report_id: str, token: str) -> str:
    response = requests.post(
        f"{CORE_URL}/v1/manifest/{report_id}",
        json={"file_id": file_id},
        headers={"authorization": token},
    )
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(
            f"Failed to fetch file manifest. Status code: {response.status_code}"
        )


__all__ = [
    "_fetch_credentials",
    "_fetch_file_manifest",
]
