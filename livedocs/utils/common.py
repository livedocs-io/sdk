from typing import Dict
import requests
from datetime import datetime
import polars as pl
import altair as alt

from livedocs.types import Credentials

_LIVEDOCS_COLORS = [
    "#0094ff",
    "#079250",
    "#dc6903",
    "#d92d21",
    "#6938ef",
    "#e04f15",
    "#ca8505",
    "#ba24d5",
    "#434ce7",
    "#109384",
    "#e31a54",
    "#068ab2",
    "#dd2690",
    "#4ca30e",
    "#7839ee",
]


def _get_color(index: int) -> str:
    return _LIVEDOCS_COLORS[index % len(_LIVEDOCS_COLORS)]


def _get_color_group_key(value):
    if value is None or value == "":
        return "Unnamed"
    return str(value)


def _get_user_defined_color(custom_key, value, style_settings, color_index) -> str:
    mark_settings = style_settings.get("markSettings", {})
    color_settings = mark_settings.get(custom_key, {}).get("color", {})

    if color_settings.get("mode") == "all_fields":
        return color_settings.get("hex", {}).get(value, _get_color(color_index))
    return _get_color(color_index)


def _get_user_defined_opacity(custom_key, style_settings, fallback_field):
    mark_settings = style_settings.get("markSettings", {})
    opacity_settings = mark_settings.get(custom_key, {}).get("opacity", {})

    if opacity_settings.get("mode") == "all_fields":
        return alt.value(int(opacity_settings.get("value", "100")) / 100)
    elif opacity_settings.get("mode") == "based_on_field":
        opacity_field = opacity_settings.get("field", "no-field-found")
        return alt.Opacity(
            field=opacity_field
            if opacity_field != "" or opacity_field != "no-field-found"
            else fallback_field[0],
            type="quantitative"
            if opacity_field != "" or opacity_field != "no-field-found"
            else fallback_field[1],
        )
    return alt.value(1)


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
    "_LIVEDOCS_COLORS",
    "_get_color",
    "_get_color_group_key",
    "_get_user_defined_color",
    "_get_user_defined_opacity",
]
