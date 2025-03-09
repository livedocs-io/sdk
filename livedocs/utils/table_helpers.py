import polars as pl
from typing import Dict, List, Optional, Any, Union

def apply_sort(df: pl.DataFrame, sort_operation: Dict[str, Any]) -> pl.DataFrame:
    """
    Apply sorting to a DataFrame based on sort operation metadata.
    
    Args:
        df: Input DataFrame
        sort_operation: Dictionary containing sort configuration
            {
                "column": "column_name",
                "direction": "asc" or "desc"
            }
    
    Returns:
        Sorted DataFrame
    """
    if not sort_operation or "column" not in sort_operation:
        return df
    
    column = sort_operation["column"]
    direction = sort_operation.get("direction", "asc")
    
    # Check if column exists in DataFrame
    if column not in df.columns:
        print(f"Warning: Sort column '{column}' not found in DataFrame")
        return df 
    
    # Apply sort
    reverse = direction.lower() == "desc"
    return df.sort(column, reverse=reverse)

def apply_filters(df: pl.DataFrame, filter_conditions: List[Dict[str, Any]]) -> pl.DataFrame:
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
            if operator == 'eq':
                result_df = result_df.filter(pl.col(column) == value)
            elif operator == 'gt':
                try:
                    num_value = float(value)
                    result_df = result_df.filter(pl.col(column) > num_value)
                except ValueError:
                    result_df = result_df.filter(pl.col(column) > value)
            elif operator == 'lt':
                try:
                    num_value = float(value)
                    result_df = result_df.filter(pl.col(column) < num_value)
                except ValueError:
                    result_df = result_df.filter(pl.col(column) < value)
            elif operator == 'gte':
                try:
                    num_value = float(value)
                    result_df = result_df.filter(pl.col(column) >= num_value)
                except ValueError:
                    result_df = result_df.filter(pl.col(column) >= value)
            elif operator == 'lte':
                try:
                    num_value = float(value)
                    result_df = result_df.filter(pl.col(column) <= num_value)
                except ValueError:
                    result_df = result_df.filter(pl.col(column) <= value)
            elif operator == 'contains':
                result_df = result_df.filter(pl.col(column).str.contains(value, strict=False))
            elif operator == 'startsWith':
                result_df = result_df.filter(pl.col(column).str.starts_with(value))
            elif operator == 'endsWith':
                result_df = result_df.filter(pl.col(column).str.ends_with(value))
            elif operator == 'notNull':
                result_df = result_df.filter(pl.col(column).is_not_null())
            elif operator == 'notEmpty':
                result_df = result_df.filter((pl.col(column).cast(pl.Utf8).is_not_null()) & (pl.col(column).cast(pl.Utf8) != ""))
            elif operator == 'empty':
                result_df = result_df.filter((pl.col(column).cast(pl.Utf8).is_null()) | (pl.col(column).cast(pl.Utf8) == ""))
            elif operator == 'null':
                result_df = result_df.filter(pl.col(column).is_null())
            else:
                print(f"Warning: Unsupported operator {operator}")
                continue
                
        except Exception as e:
            print(f"Error applying filter {operator} on column {column}: {e}")
            continue
    
    return result_df

def apply_table_operations(df: pl.DataFrame, metadata: Dict[str, Any]) -> pl.DataFrame:
    """
    Apply all table operations from metadata to a DataFrame.
    
    Args:
        df: Input DataFrame
        metadata: Table metadata containing operations
    
    Returns:
        Processed DataFrame
    """
    if not metadata:
        return df
    
    result_df = df
    
    # Apply filters first
    if "filters" in metadata and metadata["filters"]:
        result_df = apply_filters(result_df, metadata["filters"])
    
    # Apply sort after filters
    if "sort" in metadata and metadata["sort"]:
        result_df = apply_sort(result_df, metadata["sort"])
    
    # Future: Apply calculations
    # Future: Apply styles (these don't affect the data, just the presentation)
    
    return result_df