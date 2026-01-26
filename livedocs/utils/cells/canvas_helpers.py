"""
Canvas cell helper functions for serializing Python data to JSON for the frontend.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any

import polars as pl


def serialize_canvas_data(context: dict) -> dict:
    """
    Serialize context values to JSON-compatible Python objects.

    Converts Python objects to JSON-serializable equivalents that can be
    passed to the frontend and accessed via livedocs.data.variableName.

    Examples:
        - DataFrame → [{"col": "val"}, ...]
        - Series → [1, 2, 3]
        - dict → {"key": "value"}
        - list → [1, 2, 3]
        - str → "string"
        - int/float → 123 / 1.5
        - bool → True/False
        - None → None
        - datetime → "2024-01-15T10:30:00"
        - NaN/Infinity → None
    """
    serialized = {}

    for key, value in context.items():
        serialized[key] = serialize_value_to_json(value)

    # Estimate size for warning
    try:
        size_estimate = len(json.dumps(serialized))
        if size_estimate > 1_000_000:  # 1MB warning threshold
            logging.warning(
                f"Canvas data size ({size_estimate / 1_000_000:.1f}MB) exceeds 1MB. "
                "Consider reducing data size for better performance."
            )
    except Exception:
        pass

    return serialized


def serialize_value_to_json(value: Any) -> Any:
    """Serialize a single value to a JSON-compatible Python object."""
    # Handle None
    if value is None:
        return None

    # Handle NaN and Infinity (not valid JSON) - must check before other float ops
    if isinstance(value, float):
        if value != value:  # NaN check
            return None
        if value == float("inf") or value == float("-inf"):
            return None
        return value

    # Handle datetime/date - convert to ISO string
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()

    # Handle Polars DataFrame
    if isinstance(value, pl.DataFrame):
        return _replace_nan_inf(value.to_dicts())

    # Handle Polars Series
    if isinstance(value, pl.Series):
        return _replace_nan_inf(value.to_list())

    # Handle Pandas DataFrame (duck typing)
    if hasattr(value, "to_dict") and hasattr(value, "to_records"):
        return _replace_nan_inf(value.to_dict(orient="records"))

    # Handle numpy arrays (duck typing)
    if hasattr(value, "tolist"):
        return _replace_nan_inf(value.tolist())

    # Handle basic JSON-serializable types
    if isinstance(value, (str, int, bool)):
        return value

    if isinstance(value, list):
        return [serialize_value_to_json(item) for item in value]

    if isinstance(value, dict):
        return {k: serialize_value_to_json(v) for k, v in value.items()}

    # Fallback: convert to string representation
    return str(value)


def _replace_nan_inf(value: Any) -> Any:
    """Replace NaN and Infinity with None recursively."""
    if isinstance(value, float):
        if value != value or value == float("inf") or value == float("-inf"):
            return None
        return value
    if isinstance(value, list):
        return [_replace_nan_inf(v) for v in value]
    if isinstance(value, dict):
        return {k: _replace_nan_inf(v) for k, v in value.items()}
    return value
