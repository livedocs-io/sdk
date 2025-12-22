import polars as pl
import re
import dateutil.parser
from typing import Any
import altair as alt
from livedocs.types import Spec, StyleSettings


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

_LIVEDOCS_PROTECTED_VARS = {"run_context", "last_scheduled_run"}


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


def apply_chart_filters(
    df: pl.DataFrame, schema: dict, settings: Spec, chart_metadata: dict
) -> pl.DataFrame:
    """
    Apply chart filters based on user interactions (brush selections or point selections).

    Args:
        df: The input DataFrame to filter
        schema: Column schema information
        settings: Chart settings containing axis information
        chart_metadata: Filter metadata from user interaction containing:
            - action: "keep" or "remove"
            - range: [min, max] values for brush selections
            - row_index: index for point selections

    Returns:
        Filtered DataFrame
    """
    if not chart_metadata:
        return df

    # Extract filters from metadata structure
    filters = chart_metadata.get("filters", [])
    if not filters:
        return df

    # Get the filter column name based on chart type
    filter_column_name = _get_filter_column_name(settings)
    if not filter_column_name or filter_column_name not in df.columns:
        return df

    # Apply all filters sequentially
    current_df = df
    for i, filter_data in enumerate(filters):
        action = filter_data.get("action")
        if action not in ["keep", "remove"]:
            continue

        # Determine filter type and apply appropriate filtering
        if "range" in filter_data and filter_data["range"] is not None:
            current_df = _apply_range_filter(
                current_df, filter_column_name, filter_data["range"], action
            )
        elif "row_index" in filter_data and filter_data["row_index"] is not None:
            current_df = _apply_row_filter(current_df, filter_data["row_index"], action)
        else:
            continue

    return current_df


def _get_filter_column_name(settings: Spec) -> str | None:
    """
    Extract the column name to filter on based on chart type and settings.

    Args:
        settings: Chart settings

    Returns:
        Column name to filter on, or None if not found
    """
    chart_type = settings.get("chartType")

    if chart_type == "main":
        return settings.get("chartSettings", {}).get("xAxis", {}).get("field")
    elif chart_type == "swapped_main":
        return settings.get("swappedChartSettings", {}).get("yAxis", {}).get("field")
    else:
        # Fallback: try to get any axis field
        chart_settings = settings.get("chartSettings", {})
        x_field = chart_settings.get("xAxis", {}).get("field")
        y_field = chart_settings.get("yAxis", {}).get("field")
        return x_field or y_field


def _apply_range_filter(
    df: pl.DataFrame, column_name: str, range_values: list[Any], action: str
) -> pl.DataFrame:
    """
    Apply range-based filtering for brush selections.

    Args:
        df: Input DataFrame
        column_name: Name of column to filter on
        range_values: [min, max] or [[min, max]] values from brush selection
        action: "keep" or "remove"

    Returns:
        Filtered DataFrame
    """
    # Safety check for None values
    if range_values is None:
        return df

    # Handle nested array structure from Vega: [[min, max]] -> [min, max]
    if len(range_values) == 1 and isinstance(range_values[0], (list, tuple)):
        range_values = range_values[0]

    # Handle different selection types
    if len(range_values) == 2:
        # Standard brush selection: [min, max]
        min_val, max_val = range_values
    elif len(range_values) > 2:
        # Multi-point selection: convert to min/max range
        min_val, max_val = min(range_values), max(range_values)
    else:
        return df
    column_type = _get_column_type(df, column_name)

    try:
        # Create the appropriate filter mask based on column type
        if column_type == "number":
            filter_mask = _create_numeric_range_mask(df, column_name, min_val, max_val)
        elif column_type == "date":
            filter_mask = _create_date_range_mask(df, column_name, min_val, max_val)
        elif column_type == "string":
            filter_mask = _create_string_range_mask(df, column_name, min_val, max_val)
        else:
            # For boolean or unknown types, fall back to string comparison
            filter_mask = _create_string_range_mask(
                df, column_name, str(min_val), str(max_val)
            )

        if filter_mask is None:
            return df

        # Apply the filter based on action
        if action == "keep":
            return df.filter(filter_mask)
        else:  # action == "remove"
            return df.filter(~filter_mask)

    except Exception as e:
        return df


def _apply_row_filter(df: pl.DataFrame, row_index: int, action: str) -> pl.DataFrame:
    """
    Apply row-based filtering for point selections.

    Args:
        df: Input DataFrame
        row_index: Index of selected row
        action: "keep" or "remove"

    Returns:
        Filtered DataFrame
    """
    try:
        # Add row index to dataframe for filtering
        df_with_index = df.with_row_index("__chart_filter_idx")

        # Check if row_index is valid
        if row_index >= df_with_index.shape[0] or row_index < 0:
            return df

        # Create filter mask
        filter_mask = pl.col("__chart_filter_idx") == row_index

        # Apply filter based on action
        if action == "keep":
            filtered_df = df_with_index.filter(filter_mask)
        else:  # action == "remove"
            filtered_df = df_with_index.filter(~filter_mask)

        # Remove the temporary index column
        result = filtered_df.drop("__chart_filter_idx")
        return result

    except Exception as e:
        return df


def _create_numeric_range_mask(
    df: pl.DataFrame, column_name: str, min_val: Any, max_val: Any
) -> pl.Expr | None:
    """Create filter mask for numeric range."""
    try:
        min_num = float(min_val)
        max_num = float(max_val)
        return pl.col(column_name).is_between(min_num, max_num, closed="both")
    except (ValueError, TypeError):
        return None


def _create_date_range_mask(
    df: pl.DataFrame, column_name: str, min_val: Any, max_val: Any
) -> pl.Expr | None:
    """Create filter mask for date range."""
    try:
        # Convert values to dates
        min_date = _parse_date_value(min_val)
        max_date = _parse_date_value(max_val)

        if min_date is None or max_date is None:
            return None

        # Ensure column is treated as date for comparison
        date_col = pl.col(column_name).cast(pl.Date)
        return date_col.is_between(min_date, max_date, closed="both")

    except Exception as e:
        return None


def _create_string_range_mask(
    df: pl.DataFrame, column_name: str, min_val: Any, max_val: Any
) -> pl.Expr | None:
    """Create filter mask for string range (alphabetical)."""
    try:
        min_str = str(min_val)
        max_str = str(max_val)

        # For strings, use alphabetical range comparison
        return (pl.col(column_name) >= min_str) & (pl.col(column_name) <= max_str)

    except Exception as e:
        return None


def _parse_date_value(value: Any) -> pl.Expr | None:
    """Parse a date value into a Polars date expression."""
    try:
        # Handle Unix timestamp in milliseconds (JavaScript Date format)
        if isinstance(value, (int, float)) and value > 1000000000000:  # Timestamp in ms
            # Convert to datetime then to date
            import datetime

            dt = datetime.datetime.fromtimestamp(value / 1000.0)
            date_str = dt.strftime("%Y-%m-%d")
            return pl.lit(date_str).cast(pl.Date)

        # If it's already a string that looks like a date, try direct casting
        if isinstance(value, str):
            # Try direct Polars parsing first
            try:
                return pl.lit(value).cast(pl.Date)
            except:
                # Fall back to dateutil parsing
                parsed_date = dateutil.parser.parse(value)
                date_str = parsed_date.strftime("%Y-%m-%d")
                return pl.lit(date_str).cast(pl.Date)
        else:
            # Convert to string and try parsing
            return _parse_date_value(str(value))

    except Exception as e:
        return None


def _get_column_type(df: pl.DataFrame, column: str) -> str:
    """
    Determine column type with intelligent date format detection.
    Reuses logic similar to table_helpers.py

    Args:
        df: DataFrame containing the data
        column: Column name to check

    Returns:
        String representing the column type: "boolean", "number", "date", or "string"
    """
    try:
        dtype = df[column].dtype

        # Direct type checking
        if isinstance(dtype, pl.Boolean):
            return "boolean"
        elif isinstance(
            dtype,
            (
                pl.Int8,
                pl.Int16,
                pl.Int32,
                pl.Int64,
                pl.UInt8,
                pl.UInt16,
                pl.UInt32,
                pl.UInt64,
                pl.Float32,
                pl.Float64,
            ),
        ):
            return "number"
        elif isinstance(dtype, (pl.Date, pl.Datetime)):
            return "date"

        # For string columns, check if they look like dates
        if isinstance(dtype, pl.Utf8):
            sample = df[column].drop_nulls().head(5)
            if len(sample) > 0:
                # Check for common date patterns
                date_patterns = [
                    r"\d{4}-\d{2}-\d{2}",  # YYYY-MM-DD
                    r"\d{1,2}/\d{1,2}/\d{4}",  # MM/DD/YYYY or DD/MM/YYYY
                    r"\d{1,2}-\d{1,2}-\d{4}",  # DD-MM-YYYY or MM-DD-YYYY
                    r"[A-Za-z]{3,9} \d{1,2},? \d{4}",  # Month DD, YYYY
                ]

                for pattern in date_patterns:
                    if any(re.match(pattern, str(val)) for val in sample):
                        return "date"

                # Check if ISO format (with time component)
                iso_pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
                if any(re.match(iso_pattern, str(val)) for val in sample):
                    return "date"

        return "string"
    except Exception:
        return "string"


__all__ = ["apply_chart_filters"]
