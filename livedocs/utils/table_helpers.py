from typing import Any, Dict, List, Tuple

import polars as pl


def apply_sort(df: pl.DataFrame, sort_operation: Dict[str, Any]) -> pl.DataFrame:
    """
    Apply sorting to a DataFrame based on sort operation metadata.
    """
    if not sort_operation or "column" not in sort_operation:
        return df

    column = sort_operation["column"]
    direction = sort_operation.get("direction", "asc")

    if column not in df.columns:
        print(f"Warning: Sort column '{column}' not found in DataFrame")
        return df

    is_descending = direction.lower() == "desc"
    return df.sort(column, descending=is_descending)


def apply_filters(
    df: pl.DataFrame, filter_conditions: List[Dict[str, Any]]
) -> pl.DataFrame:
    """
    Apply filter conditions to a DataFrame.

    Args:
        df: Input DataFrame
        filter_conditions: List of filter condition dictionaries

    Returns:
        Filtered DataFrame
    """
    if not filter_conditions:
        return df

    result_df = df

    for condition in filter_conditions:
        column = condition.get("column")
        operator = condition.get("operator")
        value = condition.get("value")

        if column not in df.columns:
            print(f"Warning: Column {column} not found in DataFrame")
            continue

        try:
            if operator == "eq":
                try:
                    num_value = float(value)
                    result_df = result_df.filter(pl.col(column) == num_value)
                except ValueError:
                    result_df = result_df.filter(pl.col(column) == value)
            elif operator == "gt":
                try:
                    num_value = float(value)
                    result_df = result_df.filter(pl.col(column) > num_value)
                except ValueError:
                    result_df = result_df.filter(pl.col(column) > value)
            elif operator == "lt":
                try:
                    num_value = float(value)
                    result_df = result_df.filter(pl.col(column) < num_value)
                except ValueError:
                    result_df = result_df.filter(pl.col(column) < value)
            elif operator == "gte":
                try:
                    num_value = float(value)
                    result_df = result_df.filter(pl.col(column) >= num_value)
                except ValueError:
                    result_df = result_df.filter(pl.col(column) >= value)
            elif operator == "lte":
                try:
                    num_value = float(value)
                    result_df = result_df.filter(pl.col(column) <= num_value)
                except ValueError:
                    result_df = result_df.filter(pl.col(column) <= value)
            elif operator == "contains":
                result_df = result_df.filter(
                    pl.col(column).str.contains(value, strict=False)
                )
            elif operator == "startsWith":
                result_df = result_df.filter(pl.col(column).str.starts_with(value))
            elif operator == "endsWith":
                result_df = result_df.filter(pl.col(column).str.ends_with(value))
            elif operator == "notNull":
                result_df = result_df.filter(pl.col(column).is_not_null())
            elif operator == "notEmpty":
                result_df = result_df.filter(
                    (pl.col(column).cast(pl.Utf8).is_not_null())
                    & (pl.col(column).cast(pl.Utf8) != "")
                )
            elif operator == "empty":
                result_df = result_df.filter(
                    (pl.col(column).cast(pl.Utf8).is_null())
                    | (pl.col(column).cast(pl.Utf8) == "")
                )
            elif operator == "null":
                result_df = result_df.filter(pl.col(column).is_null())
            else:
                print(f"Warning: Unsupported operator {operator}")
                continue

        except Exception as e:
            print(f"Error applying filter {operator} on column {column}: {e}")
            continue

    return result_df


def apply_table_operations(
    df: pl.DataFrame, metadata: Dict[str, Any]
) -> Tuple[pl.DataFrame, Dict[str, Any]]:
    """
    Apply all table operations from metadata to a DataFrame.

    Args:
        df: Input DataFrame
        metadata: Table metadata containing operations

    Returns:
        Tuple of processed DataFrame and additional metadata
    """
    if not metadata:
        return df, {}

    result_df = df
    additional_metadata = {}

    # Apply filters first
    if "filters" in metadata and metadata["filters"]:
        result_df = apply_filters(result_df, metadata["filters"])

    # Apply sort after filters
    if "sort" in metadata and metadata["sort"]:
        result_df = apply_sort(result_df, metadata["sort"])

    # Apply calculations if present
    if "calculations" in metadata and metadata["calculations"]:
        # Note: calculations don't modify the dataframe, they produce stats
        # Calculate and return results separately
        calculation_results = _compute_calculations(result_df, metadata["calculations"])
        additional_metadata["calculation_results"] = calculation_results

    return result_df, additional_metadata


def _compute_calculations(
    df: pl.DataFrame, calculations: List[Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """
    Compute calculations for columns in the dataframe.

    Args:
        df (pl.DataFrame): The dataframe to perform calculations on
        calculations (list): List of calculation configurations

    Returns:
        dict: A dictionary mapping column IDs to calculation results
    """
    results = {}

    for calc in calculations:
        column = calc.get("column")
        calc_type = calc.get("calculation_type")

        if not column or not calc_type or column not in df.columns:
            continue

        try:
            # Perform calculation based on type
            if calc_type == "Count all":
                results[column] = {"type": calc_type, "value": len(df)}

            elif calc_type == "Count values":
                try:
                    count = df.filter(pl.col(column).is_not_null()).height
                    results[column] = {"type": calc_type, "value": count}
                except Exception as e:
                    print(f"Error computing Count values for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Count unique values":
                try:
                    unique_count = df[column].n_unique()
                    results[column] = {"type": calc_type, "value": unique_count}
                except Exception as e:
                    print(
                        f"Error computing Count unique values for column {column}: {e}"
                    )
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Count empty":
                try:
                    empty_count = df.filter(pl.col(column).is_null()).height
                    results[column] = {"type": calc_type, "value": empty_count}
                except Exception as e:
                    print(f"Error computing Count empty for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Count not empty":
                try:
                    not_empty_count = df.filter(pl.col(column).is_not_null()).height
                    results[column] = {"type": calc_type, "value": not_empty_count}
                except Exception as e:
                    print(f"Error computing Count not empty for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Percent empty":
                try:
                    empty_count = df.filter(pl.col(column).is_null()).height
                    total_count = len(df)
                    percent = (
                        (empty_count / total_count) * 100 if total_count > 0 else 0
                    )
                    results[column] = {"type": calc_type, "value": f"{percent:.2f}%"}
                except Exception as e:
                    print(f"Error computing Percent empty for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Percent not empty":
                try:
                    not_empty_count = df.filter(pl.col(column).is_not_null()).height
                    total_count = len(df)
                    percent = (
                        (not_empty_count / total_count) * 100 if total_count > 0 else 0
                    )
                    results[column] = {"type": calc_type, "value": f"{percent:.2f}%"}
                except Exception as e:
                    print(f"Error computing Percent not empty for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Earliest date":
                try:
                    # Try to convert column to date
                    date_col = df[column].cast(pl.Date)
                    min_date = date_col.min()
                    results[column] = {
                        "type": calc_type,
                        "value": min_date.strftime("%Y-%m-%d") if min_date else None,
                    }
                except Exception as e:
                    print(f"Error computing Earliest date for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Latest date":
                try:
                    # Try to convert column to date
                    date_col = df[column].cast(pl.Date)
                    max_date = date_col.max()
                    results[column] = {
                        "type": calc_type,
                        "value": max_date.strftime("%Y-%m-%d") if max_date else None,
                    }
                except Exception as e:
                    print(f"Error computing Latest date for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Date range":
                try:
                    # Try to convert column to date
                    date_col = df[column].cast(pl.Date)
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
                except Exception as e:
                    print(f"Error computing Date range for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            # Number calculations
            elif calc_type == "Sum":
                try:
                    # Try to convert column to numeric and calculate sum
                    numeric_col = df[column].cast(pl.Float64, strict=False)
                    sum_value = numeric_col.sum()
                    results[column] = {
                        "type": calc_type,
                        "value": round(float(sum_value), 2)
                        if sum_value is not None
                        else None,
                    }
                except Exception as e:
                    print(f"Error computing Sum for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Mean":
                try:
                    # Try to convert column to numeric and calculate mean
                    numeric_col = df[column].cast(pl.Float64, strict=False)
                    mean_value = numeric_col.mean()
                    results[column] = {
                        "type": calc_type,
                        "value": round(float(mean_value), 2)
                        if mean_value is not None
                        else None,
                    }
                except Exception as e:
                    print(f"Error computing Mean for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Median":
                try:
                    # Try to convert column to numeric and calculate median
                    numeric_col = df[column].cast(pl.Float64, strict=False)
                    median_value = numeric_col.median()
                    results[column] = {
                        "type": calc_type,
                        "value": round(float(median_value), 2)
                        if median_value is not None
                        else None,
                    }
                except Exception as e:
                    print(f"Error computing Median for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Mode":
                try:
                    # Try to convert column to numeric and calculate mode
                    numeric_col = df[column].cast(pl.Float64, strict=False)
                    # Calculate the most frequent value
                    mode_value = (
                        numeric_col.value_counts()
                        .sort(by="counts", descending=True)
                        .select("values")
                        .head(1)[0, 0]
                    )
                    results[column] = {
                        "type": calc_type,
                        "value": round(float(mode_value), 2)
                        if mode_value is not None
                        else None,
                    }
                except Exception as e:
                    print(f"Error computing Mode for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Min":
                try:
                    # Try to convert column to numeric and calculate min
                    numeric_col = df[column].cast(pl.Float64, strict=False)
                    min_value = numeric_col.min()
                    results[column] = {
                        "type": calc_type,
                        "value": round(float(min_value), 2)
                        if min_value is not None
                        else None,
                    }
                except Exception as e:
                    print(f"Error computing Min for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Max":
                try:
                    # Try to convert column to numeric and calculate max
                    numeric_col = df[column].cast(pl.Float64, strict=False)
                    max_value = numeric_col.max()
                    results[column] = {
                        "type": calc_type,
                        "value": round(float(max_value), 2)
                        if max_value is not None
                        else None,
                    }
                except Exception as e:
                    print(f"Error computing Max for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "Range":
                try:
                    # Try to convert column to numeric and calculate range
                    numeric_col = df[column].cast(pl.Float64, strict=False)
                    min_value = numeric_col.min()
                    max_value = numeric_col.max()

                    if min_value is not None and max_value is not None:
                        range_value = float(max_value) - float(min_value)
                        results[column] = {
                            "type": calc_type,
                            "value": round(range_value, 2),
                        }
                    else:
                        results[column] = {"type": calc_type, "value": None}
                except Exception as e:
                    print(f"Error computing Range for column {column}: {e}")
                    results[column] = {
                        "type": calc_type,
                        "value": None,
                        "error": str(e),
                    }

            elif calc_type == "None":
                results[column] = {"type": calc_type, "value": None}

        except Exception as e:
            print(f"Error computing calculation for column {column}: {e}")
            results[column] = {"type": calc_type, "value": None, "error": str(e)}

    return results
