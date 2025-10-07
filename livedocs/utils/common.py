import os
import re
from datetime import datetime
from functools import lru_cache, wraps
from typing import Any, Dict, Optional

import altair as alt
import dateutil.parser
import polars as pl
import requests
import sentry_sdk
from tqdm.auto import tqdm

from livedocs.types import (
    FileManifest,
    FileManifestAction,
    GCSBucketType,
    StyleSettings,
)

_LIVEDOCS_COLORS = [
    "#713E5A",
    "#D57A66",
    "#6564A6",
    "#CBD20F",
    "#F1BB4F",
    "#22577A",
    "#63A375",
    "#E46B62",
]

_DARKMODE_COLORS = {
    "background": "#0C0A09",
    "grid lines": "#292524",
    "axis labels": "#93715A",
    "tick labels": "#D3C3B6",
}

_LIVEDOCS_PROTECTED_VARS = {"run_context", "last_scheduled_run"}


REF_STROKE_DASH = {"solid": [0, 0], "dashed": [5, 5], "dotted": [2, 5]}

REF_BASELINE = {
    "outside": "bottom",
    "top-left": "bottom",
    "top-right": "bottom",
    "bottom-left": "top",
    "bottom-right": "top",
}
REF_ALIGN = {
    "outside": "center",
    "top-left": "right",
    "top-right": "left",
    "bottom-left": "right",
    "bottom-right": "left",
}


def get_run_context() -> str:
    current_run_context = "edit_mode"
    match os.getenv("RUN_CONTEXT"):
        case "logic":
            current_run_context = "edit_mode"
        case "scheduled":
            current_run_context = "scheduled_runs"
        case "webhook":
            current_run_context = "webhook_runs"
        case _:
            current_run_context = "unknown_run_context"
    return current_run_context


def _capture_exceptions(func):
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


def _get_color(index: int) -> str:
    return _LIVEDOCS_COLORS[index % len(_LIVEDOCS_COLORS)]


def _get_darkmode_color(feature: str) -> str:
    return _DARKMODE_COLORS.get(feature, "")


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


@_capture_exceptions
def _fetch_credentials(report_id: str, token: str) -> Dict[str, Any]:
    CORE_URL = os.getenv("CORE_BASE_URL")
    if not CORE_URL:
        raise ValueError("CORE_BASE_URL environment variable not set")

    response = requests.get(
        f"{CORE_URL}/v1/credentials/{report_id}",
        headers={"authorization": token},
    )

    CORE_URL = os.getenv("CORE_BASE_URL")
    if not CORE_URL:
        raise ValueError("CORE_BASE_URL environment variable not set")

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(
            f"Failed to fetch credentials. Status code: {response.status_code}"
        )


@lru_cache(maxsize=128)
def _fetch_file_manifest(
    report_id: str,
    token: str,
    action: FileManifestAction,
    bucket: GCSBucketType,
    file_id: Optional[str] = None,
    file_name: Optional[str] = None,
) -> FileManifest:
    CORE_URL = os.getenv("CORE_BASE_URL")
    if not CORE_URL:
        raise ValueError("CORE_BASE_URL environment variable not set")

    if not file_id and not file_name:
        raise ValueError(
            "Either file_id or file_name must be provided to fetch manifest."
        )

    if action not in {"write", "read"}:
        raise ValueError("Invalid action. Must be 'write' or 'read'.")

    payload = {
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
            try:
                error_response_json = e.response.json()
                api_error_message = error_response_json.get("message", e.response.text)
            except ValueError:
                api_error_message = e.response.text
            api_error_message = sanitize_sensitive_data(api_error_message)

            if status_code == 404:
                identifier = file_id or file_name
                raise FileNotFoundError(
                    f"File '{identifier}' not found. Error: {api_error_message}"
                ) from e
            elif status_code == 409:  # Should only occur if file_name was used
                conflicting_files_info = ""
                if "files" in error_response_json and isinstance(
                    error_response_json["files"], list
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
                    sanitize_sensitive_data(
                        f"Failed to get file manifest for '{file_name}'. Status: {status_code}. Error: {api_error_message}"
                    )
                )
        else:
            raise RuntimeError(
                sanitize_sensitive_data(
                    f"Failed to get file manifest for '{file_name}': {e}"
                )
            )
    except Exception as e:
        raise RuntimeError(
            sanitize_sensitive_data(
                f"An unexpected error occurred while fetching manifest for '{file_name}': {e}"
            )
        )


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


def sanitize_sensitive_data(message: Optional[str]) -> str:
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


@_capture_exceptions
def _persist_built_in_vars(report_id: str, token: str, vars: dict) -> dict:
    CORE_URL = os.getenv("CORE_BASE_URL")
    if not CORE_URL:
        raise ValueError("CORE_BASE_URL environment variable not set")

    response = requests.post(
        f"{CORE_URL}/v1/vars/{report_id}",
        json=vars,
        headers={"authorization": token},
    )
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(
            f"Failed to persist built-in vars. Status code: {response.status_code}"
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


def get_axis_format(timeunit: str) -> str:
    format_map = {
        "year": "%Y",
        "yearquarter": "%Y Q%q",
        "yearmonth": "%b %Y",
        "yearweek": "%Y W%W",
        "yearmonthdate": "%b %d, %Y",
        "yearmonthdatehours": "%b %d, %Y %I:%M %p",
        "yearmonthdatehoursminutes": "%b %d, %Y %I:%M",
        "yearmonthdatehoursminutesseconds": "%b %d, %Y %I:%M:%S",
    }
    return format_map.get(timeunit, "")


def iso_to_alt_datetime(iso_string):
    """Convert ISO date string to alt.DateTime object"""
    dt = dateutil.parser.parse(iso_string)
    return alt.DateTime(
        year=dt.year,
        month=dt.month,
        date=dt.day,
        hours=dt.hour,
        minutes=dt.minute,
        seconds=dt.second,
    )


def num_converter(num):
    try:
        return float(num)
    except ValueError:
        pass

    try:
        return int(num)
    except ValueError:
        pass

    try:
        return iso_to_alt_datetime(num)
    except ValueError:
        pass

    return num


"""
Generates an altair line plot and altair label plot to layer on base plot
"""


def create_line(
    df: pl.DataFrame,
    axis: str,  # "x" or "y"
    style_settings: StyleSettings,
):
    if axis not in ["x", "y"]:
        raise ValueError("Invalid value for 'axis'. Expected 'x' or 'y'.")

    ref_list = style_settings.get(f"{axis}Axis", {}).get("referenceLines", [])
    ref_chart_list = []

    if len(ref_list) > 0:
        for line in ref_list:
            val = num_converter(line.get("value", ""))

            if line["labelPosition"] == "none":
                line["labelPosition"] = "outside"

            ref_line = (
                alt.Chart(df)
                .mark_rule(
                    color=line.get("color", "#93715A"),
                    strokeDash=REF_STROKE_DASH[line.get("lineStyle", "solid")],
                    strokeWidth=line.get("lineWidth", 1),
                )
                .encode(**{axis: alt.datum(val)})
            )

            ref_chart_list.append(ref_line)

            label = ref_line.mark_text(
                baseline=REF_BASELINE[line.get("labelPosition", "outside")],
                align=REF_ALIGN[line.get("labelPosition", "outside")],
                size=12,
                angle=line.get("labelAngle", 0),
                dx=-5,
                dy=5.5,
            ).encode(
                text=alt.value(line.get("label", "Reference Line")),
                **({"y": alt.value(0)} if axis == "x" else {"x": alt.value(0)}),
            )

            ref_chart_list.append(label)

    return ref_chart_list


def _setup_sentry():
    """
    Initializes Sentry for error tracking and performance monitoring.
    """
    try:
        sentry_sdk.init(
            dsn=os.getenv("VMLIB_SENTRY_DSN"),
            traces_sample_rate=1 if os.getenv("APP_ENV") != "prd" else 0.2,
            profiles_sample_rate=1 if os.getenv("APP_ENV") != "prd" else 0.2,
            environment=os.getenv("APP_ENV"),
        )
    except Exception as e:
        raise f"Failed to initialize Sentry: {e}"


def _setup_dirs():
    """
    Sets up the user files directory if it doesn't exist.
    """
    user_files_dir = os.getenv("LIVEDOCS_FILES_PATH")
    if not user_files_dir:
        raise ValueError("LIVEDOCS_FILES_PATH environment variable not set")

    os.makedirs(user_files_dir, exist_ok=True)


def _get_chunk_size(file_size_bytes: Optional[int]) -> int:
    """
    Determines a dynamic chunk size based on the file size.
    """
    MB = 1024 * 1024
    if file_size_bytes is None:
        return 128 * 1024  # Default to 128KB if size is unknown

    if file_size_bytes < 1 * MB:  # Files < 1MB
        return 64 * 1024  # 64KB chunks
    elif file_size_bytes < 10 * MB:  # Files < 10MB
        return 128 * 1024  # 128KB chunks
    elif file_size_bytes < 100 * MB:  # Files < 100MB
        return 256 * 1024  # 256KB chunks
    elif file_size_bytes < 500 * MB:  # Files < 500MB
        return 512 * 1024  # 512KB chunks
    else:  # Files >= 500MB
        return 1 * MB  # 1MB chunks


def _download_file(
    signed_url: str,
    local_path: str,
    file_description: str,
    expected_size_bytes: Optional[int],
):
    """
    Downloads a file from a signed URL to a local path.
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    try:
        response = requests.get(signed_url, stream=True)
        response.raise_for_status()

        total_size = expected_size_bytes
        chunk_size = _get_chunk_size(total_size)

        with (
            open(local_path, "wb") as f,
            tqdm(
                desc=f"Downloading {file_description}",
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                disable=total_size is None or total_size == 0,
                leave=True,
            ) as bar,
        ):
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

    except requests.exceptions.HTTPError as e:
        if os.path.exists(local_path):
            os.remove(local_path)
        error_detail = f"HTTP status {e.response.status_code}."
        error_detail += f" Response: {e.response.text}"
        raise RuntimeError(
            f"Failed to download {file_description}. {error_detail}"
        ) from e
    except requests.exceptions.RequestException as e:
        if os.path.exists(local_path):
            os.remove(local_path)
        raise RuntimeError(
            f"Network error while downloading {file_description}: {e}"
        ) from e
    except Exception as e:
        if os.path.exists(local_path):
            os.remove(local_path)
        raise RuntimeError(
            f"An error occurred during download of {file_description}: {e}"
        ) from e
