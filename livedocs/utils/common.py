from typing import Dict
import requests
from datetime import datetime
import polars as pl

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


def _get_dataframe_schema(df: pl.DataFrame) -> Dict[str, str]:
    date_formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%B %d, %Y",
        "%d %b %Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y/%m/%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
    ]

    def is_date(value: str) -> bool:
        for fmt in date_formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except ValueError:
                continue
        return False

    def is_number(value: str) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False

    def map_column_type(series: pl.Series) -> str:
        # Drop nulls
        non_empty_series = series.drop_nulls()

        # Only filter out empty strings if the series is of string type
        if series.dtype == pl.Utf8:
            non_empty_series = non_empty_series.filter(non_empty_series != "")

        sample_values = non_empty_series.to_list()

        if not sample_values:
            return "STRING"

        # Check if all non-empty sample values can be numbers
        if all(
            isinstance(val, (int, float)) or (isinstance(val, str) and is_number(val))
            for val in sample_values
        ):
            return "NUMBER"

        # Check if all non-empty sample values can be dates
        if all(
            isinstance(val, datetime) or (isinstance(val, str) and is_date(val))
            for val in sample_values
        ):
            return "DATE"

        # Check the series' inherent type if it's not a string series
        if series.dtype in {pl.Float32, pl.Float64, pl.Int32, pl.Int64}:
            return "NUMBER"
        elif series.dtype in {pl.Date, pl.Datetime}:
            return "DATE"
        else:
            return "STRING"

    column_types = {col: map_column_type(df[col]) for col in df.columns}

    return column_types


__all__ = [
    "_fetch_credentials",
    "_fetch_file_manifest",
    "_get_dataframe_schema",
]
