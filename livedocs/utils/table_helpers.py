import re
from typing import Any

import dateutil.parser
import polars as pl


def apply_sort(df: pl.DataFrame, sort_operation: dict[str, Any]) -> pl.DataFrame:
    """
    Apply sorting to a DataFrame based on sort operation metadata.

    Args:
        df: Input DataFrame
        sort_operation: Dictionary containing sort configuration with 'column' and optional 'direction'

    Returns:
        Sorted DataFrame
    """
    if not sort_operation or "column" not in sort_operation:
        return df

    column = sort_operation["column"]
    direction = sort_operation.get("direction", "asc")

    if column not in df.columns:
        return df

    is_descending = direction.lower() == "desc"
    return df.sort(column, descending=is_descending)


def apply_table_operations(
    df: pl.DataFrame, metadata: dict[str, Any]
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """
    Apply all table operations from metadata to a DataFrame.

    Args:
        df: Input DataFrame
        metadata: Table metadata containing operations (filters, styles, sort, calculations)

    Returns:
        Tuple of (processed DataFrame, additional metadata)
    """
    if not metadata:
        return df, {}

    result_df = df
    additional_metadata = {"column_types": _get_column_types(result_df)}

    # Process filters and collect style matches in a single pass
    if metadata.get("filters") or metadata.get("styles"):
        result_df, style_results = process_conditions(
            df,
            filters=metadata.get("filters", []) or [],
            styles=metadata.get("styles", []) or [],
        )
        additional_metadata["style_results"] = style_results

    # Apply sort after filters
    if metadata.get("sort"):
        result_df = apply_sort(result_df, metadata["sort"])

    # Apply calculations if present
    if metadata.get("calculations"):
        calculation_results = _compute_calculations(result_df, metadata["calculations"])
        additional_metadata["calculation_results"] = calculation_results

    return result_df, additional_metadata


def process_conditions(
    df: pl.DataFrame,
    filters: list[dict[str, Any]] = [],
    styles: list[dict[str, Any]] = [],
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """
    Process both filter conditions and style rules in a single pass.

    Args:
        df: Input DataFrame
        filters: List of filter condition dictionaries
        styles: List of style rule dictionaries

    Returns:
        Tuple of (filtered DataFrame, style results dictionary)
    """
    filters = filters or []
    styles = styles or []

    # Process filter conditions
    filter_mask = pl.lit(True)
    current_conjunction = "AND"

    for i, condition in enumerate(filters):
        column = condition.get("column")
        operator = condition.get("operator")
        value = condition.get("value")
        conjunction = condition.get("conjunction", "AND").upper()

        if column not in df.columns:
            continue

        column_type = _get_column_type(df, column)

        try:
            condition_mask = create_condition_mask(
                df, column, operator, value, column_type
            )

            if condition_mask is None:
                continue

            # Apply the condition based on conjunction
            if i == 0:
                filter_mask = condition_mask
            else:
                if current_conjunction == "AND":
                    filter_mask = filter_mask & condition_mask
                else:  # OR
                    filter_mask = filter_mask | condition_mask

            # Store this condition's conjunction for the next iteration
            current_conjunction = conjunction

        except Exception:
            continue

    # Apply the filter mask to get the filtered DataFrame
    filtered_df = df.filter(filter_mask)

    # Process style rules and collect matching rows
    style_results = {}
    with_index = filtered_df.with_row_index("__index")

    for rule in styles:
        rule_id = rule.get("id")
        conditions = rule.get("conditions", [])
        color = rule.get("color", "#FFFFFF")

        if not conditions:
            continue

        # Process conditions within this rule, respecting conjunctions
        rule_mask = None
        rule_conjunction = "AND"

        for i, condition in enumerate(conditions):
            column = condition.get("column")
            operator = condition.get("operator")
            value = condition.get("value")
            next_conjunction = condition.get("conjunction", "AND").upper()

            # If column is not specified but there's a default column in the rule, use that
            if not column and "column" in rule:
                column = rule.get("column")

            if not column or column not in df.columns:
                continue

            column_type = _get_column_type(df, column)

            try:
                condition_mask = create_condition_mask(
                    df, column, operator, value, column_type
                )

                if condition_mask is None:
                    continue

                # Apply the condition based on THIS iteration's conjunction
                if rule_mask is None:
                    rule_mask = condition_mask
                else:
                    if rule_conjunction == "AND":
                        rule_mask = rule_mask & condition_mask
                    else:  # OR
                        rule_mask = rule_mask | condition_mask

                # Store conjunction for NEXT condition
                rule_conjunction = next_conjunction

            except Exception:
                continue

        # Apply the rule mask to get matching rows
        if rule_mask is not None:
            try:
                matching_rows = with_index.filter(rule_mask).select("__index")

                # Record style information for each matching row
                for row in matching_rows.to_dicts():
                    idx = row["__index"]
                    idx_str = str(idx)
                    if idx_str not in style_results:
                        style_results[idx_str] = []
                    style_results[idx_str].append({"rule_id": rule_id, "color": color})
            except Exception:
                pass

    # Handle unique filters separately if needed
    unique_columns = [c.get("column") for c in filters if c.get("operator") == "unique"]
    for column in unique_columns:
        if column in filtered_df.columns:
            try:
                # Get counts of each value
                counts = filtered_df.select(pl.col(column)).group_by(column).count()
                # Get values that appear exactly once
                unique_values = counts.filter(pl.col("count") == 1).select(
                    pl.col(column)
                )
                # Filter the dataframe to only include rows with those values
                filtered_df = filtered_df.join(unique_values, on=column, how="inner")
            except Exception:
                pass

    return filtered_df, style_results


def create_condition_mask(
    df: pl.DataFrame, column: str, operator: str, value: Any, column_type: str
) -> pl.Expr | None:
    """
    Create a filter mask for a specific condition.

    Args:
        df: DataFrame containing the data
        column: Column name to filter on
        operator: Operator type (eq, gt, lt, contains, etc.)
        value: Value to compare against
        column_type: Type of the column (number, string, date, boolean)

    Returns:
        A Polars expression for filtering or None if filter couldn't be created
    """
    # Handle common operators across all types
    if operator == "null":
        return pl.col(column).is_null()
    elif operator == "notNull":
        return pl.col(column).is_not_null()
    elif operator == "empty":
        return (pl.col(column).is_null()) | (pl.col(column).cast(pl.Utf8) == "")
    elif operator == "notEmpty":
        return (pl.col(column).is_not_null()) & (pl.col(column).cast(pl.Utf8) != "")
    elif operator == "unique":
        # This will be handled separately after all other filters
        return None

    # Type-specific operators
    if column_type == "number":
        try:
            num_value = float(value)
            if operator == "eq":
                return pl.col(column) == num_value
            elif operator == "gt":
                return pl.col(column) > num_value
            elif operator == "lt":
                return pl.col(column) < num_value
            elif operator == "gte":
                return pl.col(column) >= num_value
            elif operator == "lte":
                return pl.col(column) <= num_value
        except ValueError:
            return None

    elif column_type == "string":
        if operator == "eq":
            return pl.col(column) == value
        elif operator == "contains":
            return pl.col(column).str.contains(value, strict=False)
        elif operator == "startsWith":
            return pl.col(column).str.starts_with(value)
        elif operator == "endsWith":
            return pl.col(column).str.ends_with(value)

    elif column_type == "date":
        return create_date_condition_mask(df, column, operator, value)

    elif column_type == "boolean":
        if operator == "true":
            return pl.col(column) is True
        elif operator == "false":
            return pl.col(column) is False
        elif operator == "eq":
            bool_val = value.lower() in ("true", "t", "yes", "y", "1")
            return pl.col(column) == bool_val

    return None


def create_date_condition_mask(
    df: pl.DataFrame, column: str, operator: str, value: str
) -> pl.Expr | None:
    """
    Create a filter mask for date conditions with robust date parsing.

    Args:
        df: DataFrame containing the data
        column: Column name to filter on
        operator: Date operator (before, after, eq)
        value: Date value to compare against

    Returns:
        A Polars expression for filtering or None if filter couldn't be created
    """
    try:
        # Ensure the column is treated as a date for consistent comparisons
        date_col = pl.col(column).cast(pl.Date)

        # Try direct parsing first
        try:
            date_expr = pl.lit(value).cast(pl.Date)

            if operator == "before":
                return date_col < date_expr
            elif operator == "after":
                return date_col > date_expr
            elif operator == "eq":
                return date_col == date_expr
            else:
                return None
        except Exception:
            # If direct parsing fails, try via dateutil
            try:
                parsed_date = dateutil.parser.parse(value)
                date_str = parsed_date.strftime("%Y-%m-%d")
                date_expr = pl.lit(date_str).cast(pl.Date)

                if operator == "before":
                    return date_col < date_expr
                elif operator == "after":
                    return date_col > date_expr
                elif operator == "eq":
                    return date_col == date_expr
                else:
                    return None
            except Exception:
                return None
    except Exception:
        return None


def _get_column_types(df: pl.DataFrame) -> dict[str, str]:
    """
    Get the data types for all columns in the dataframe.

    Args:
        df: The dataframe to analyze

    Returns:
        Dictionary with column names as keys and type names as values
    """
    return {col: _get_column_type(df, col) for col in df.columns}


def _get_column_type(df: pl.DataFrame, column: str) -> str:
    """
    Determine column type with intelligent date format detection.

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


def _compute_calculations(
    df: pl.DataFrame, calculations: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """
    Compute statistical calculations for columns in the dataframe.

    Args:
        df: The dataframe to perform calculations on
        calculations: List of calculation configurations

    Returns:
        Dictionary mapping column IDs to calculation results
    """
    results = {}

    for calc in calculations:
        column = calc.get("column")
        calc_type = calc.get("calculation_type")

        if not column or not calc_type or column not in df.columns:
            continue

        try:
            # Count calculations
            if calc_type == "Count all":
                results[column] = {"type": calc_type, "value": len(df)}
            elif calc_type == "Count values":
                count = df.filter(pl.col(column).is_not_null()).height
                results[column] = {"type": calc_type, "value": count}
            elif calc_type == "Count unique values":
                unique_count = df[column].n_unique()
                results[column] = {"type": calc_type, "value": unique_count}
            elif calc_type == "Count empty":
                empty_count = df.filter(pl.col(column).is_null()).height
                results[column] = {"type": calc_type, "value": empty_count}

            # Percentage calculations
            elif calc_type == "Percent empty":
                empty_count = df.filter(pl.col(column).is_null()).height
                total_count = len(df)
                percent = (empty_count / total_count) * 100 if total_count > 0 else 0
                results[column] = {"type": calc_type, "value": f"{percent:.2f}%"}
            elif calc_type == "Percent not empty":
                not_empty_count = df.filter(pl.col(column).is_not_null()).height
                total_count = len(df)
                percent = (
                    (not_empty_count / total_count) * 100 if total_count > 0 else 0
                )
                results[column] = {"type": calc_type, "value": f"{percent:.2f}%"}

            # Date calculations
            elif calc_type in ["Earliest date", "Latest date", "Date range"]:
                date_col = df[column].cast(pl.Date)

                if calc_type == "Earliest date":
                    min_date = date_col.min()
                    results[column] = {
                        "type": calc_type,
                        "value": min_date.strftime("%Y-%m-%d") if min_date else None,
                    }
                elif calc_type == "Latest date":
                    max_date = date_col.max()
                    results[column] = {
                        "type": calc_type,
                        "value": max_date.strftime("%Y-%m-%d") if max_date else None,
                    }
                elif calc_type == "Date range":
                    min_date = date_col.min()
                    max_date = date_col.max()

                    if min_date and max_date:
                        min_str = min_date.strftime("%Y-%m-%d")
                        max_str = max_date.strftime("%Y-%m-%d")
                        results[column] = {
                            "type": calc_type,
                            "value": f"{min_str} - {max_str}",
                        }
                    else:
                        results[column] = {"type": calc_type, "value": None}

            # Numeric calculations
            elif calc_type in ["Sum", "Mean", "Median", "Mode", "Min", "Max", "Range"]:
                numeric_col = df[column].cast(pl.Float64, strict=False)

                if calc_type == "Sum":
                    value = numeric_col.sum()
                elif calc_type == "Mean":
                    value = numeric_col.mean()
                elif calc_type == "Median":
                    value = numeric_col.median()
                elif calc_type == "Mode":
                    value = (
                        numeric_col.value_counts()
                        .sort(by="counts", descending=True)
                        .select("values")
                        .head(1)[0, 0]
                    )
                elif calc_type == "Min":
                    value = numeric_col.min()
                elif calc_type == "Max":
                    value = numeric_col.max()
                elif calc_type == "Range":
                    min_value = numeric_col.min()
                    max_value = numeric_col.max()

                    if min_value is not None and max_value is not None:
                        value = float(max_value) - float(min_value)
                    else:
                        value = None

                # Format the result
                if value is not None and calc_type != "Range":
                    value = round(float(value), 2)
                elif value is not None:
                    value = round(value, 2)

                results[column] = {"type": calc_type, "value": value}

            elif calc_type == "None":
                results[column] = {"type": calc_type, "value": None}

        except Exception as e:
            results[column] = {"type": calc_type, "value": None, "error": str(e)}

    return results
