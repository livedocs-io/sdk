import json
from typing import Any, Dict, Optional, Union


def format_single_value(
    value: Any, format_type: str, fixed_decimals: Optional[int] = None
) -> str:
    """
    Format a value according to the specified format type

    Args:
        value: The value to format
        format_type: The format type (plain, number, percent)
        fixed_decimals: Number of decimal places to display (optional)

    Returns:
        str: The formatted value
    """
    if value is None:
        return "None"

    # Convert to number for numeric formats
    if format_type in ["number", "percent"]:
        try:
            numeric_value = float(value)

            # Format as number with commas
            if format_type == "number":
                if fixed_decimals is not None:
                    return f"{numeric_value:,.{fixed_decimals}f}"
                return (
                    f"{numeric_value:,.0f}"
                    if numeric_value.is_integer()
                    else f"{numeric_value:,.2f}"
                )

            # Format as percentage
            elif format_type == "percent":
                if fixed_decimals is not None:
                    return f"{numeric_value * 100:.{fixed_decimals}f}%"
                return (
                    f"{numeric_value * 100:.0f}%"
                    if (numeric_value * 100).is_integer()
                    else f"{numeric_value * 100:.2f}%"
                )
        except (ValueError, TypeError):
            return str(value)

    # Plain text format
    return str(value)


def process_comparison(
    main_value: Union[int, float],
    compare_value: Union[int, float],
    compare_type: str,
    compare_format: Optional[str] = None,
    fixed_decimals: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Process comparison between main value and comparison value

    Args:
        main_value: The main value
        compare_value: The comparison value
        compare_type: The comparison type (compare_value, compare_percent, absolute_change)
        compare_format: The comparison format (compare_value, absolute_change)
        fixed_decimals: Number of decimal places to display (optional)

    Returns:
        dict: Comparison result with value, formatted_value, direction, and label
    """
    try:
        # Convert values to float
        main_num = float(main_value)
        comp_num = float(compare_value)

        # Calculate difference and direction
        difference = main_num - comp_num
        direction = "up" if difference > 0 else "down" if difference < 0 else "none"

        # Calculate comparison based on type
        if compare_type == "compare_value":
            comparison_value = comp_num
            formatted_value = (
                f"{abs(comparison_value):.{fixed_decimals}f}"
                if fixed_decimals is not None
                else f"{abs(comparison_value):.0f}"
                if comparison_value.is_integer()
                else f"{abs(comparison_value):.2f}"
            )

        elif compare_type == "compare_percent":
            if comp_num == 0:
                return {
                    "value": 0,
                    "formatted_value": "N/A",
                    "direction": "none",
                    "label": "",
                }
            comparison_value = difference / comp_num
            formatted_value = (
                f"{abs(comparison_value) * 100:.{fixed_decimals}f}%"
                if fixed_decimals is not None
                else f"{abs(comparison_value) * 100:.0f}%"
                if (comparison_value * 100).is_integer()
                else f"{abs(comparison_value) * 100:.1f}%"
            )

        elif compare_type == "absolute_change":
            comparison_value = difference
            formatted_value = (
                f"{abs(comparison_value):.{fixed_decimals}f}"
                if fixed_decimals is not None
                else f"{abs(comparison_value):.0f}"
                if comparison_value.is_integer()
                else f"{abs(comparison_value):.2f}"
            )
        else:
            return {
                "value": 0,
                "formatted_value": "0",
                "direction": "none",
                "label": "",
            }

        return {
            "value": comparison_value,
            "formatted_value": formatted_value,
            "direction": direction,
            "label": "",
        }
    except (ValueError, TypeError):
        return {
            "value": 0,
            "formatted_value": "Error",
            "direction": "none",
            "label": "",
        }


def process_single_value(config: str, context: Optional[dict] = None) -> Dict[str, Any]:
    """
    Process a SingleValue element with formatting and comparison calculations

    Args:
        config (str): JSON string containing single value configuration
        context (dict, optional): Context containing variables. Defaults to None.

    Returns:
        dict: Formatted result with main value and comparison data
    """
    try:
        # Use provided context or fall back to globals
        variable_context = context if context is not None else globals()

        # Parse the configuration
        single_value_config = json.loads(config)

        # Initialize result structure
        result = {"formatted_value": "", "comparison": None, "error": None}

        # Get main value from variable
        if not single_value_config.get("valueVariable"):
            return result

        var_name = single_value_config["valueVariable"]
        if var_name not in variable_context:
            result["error"] = f"Variable '{var_name}' not found"
            return result

        main_value = variable_context[var_name]

        # Format the main value
        format_type = single_value_config.get("format", "plain")
        fixed_decimals = single_value_config.get("fixedDecimals")

        result["formatted_value"] = format_single_value(
            main_value, format_type, fixed_decimals
        )

        # Handle comparison if enabled
        if not single_value_config.get("showComparison", False):
            return result

        compare_var_name = single_value_config.get("compareVariable")

        # Validation: Prevent using the same variable for both main value and comparison
        if compare_var_name == var_name:
            result["error"] = (
                "Cannot use the same variable for both value and comparison"
            )
            return result

        if not compare_var_name or compare_var_name not in variable_context:
            result["error"] = f"Comparison variable '{compare_var_name}' not found"
            return result

        compare_value = variable_context[compare_var_name]

        # Get comparison type directly - no mapping needed
        compare_type = single_value_config.get("compareType", "compare_value")

        # Get comparison format directly
        compare_format = single_value_config.get("comparisonFormat", "compare_value")

        # Calculate comparison
        comparison_result = process_comparison(
            main_value,
            compare_value,
            compare_type,
            compare_format,
            fixed_decimals,
        )

        # Add label if provided
        if single_value_config.get("compareLabel"):
            comparison_result["label"] = single_value_config["compareLabel"]

        result["comparison"] = comparison_result
        return result

    except json.JSONDecodeError:
        return {
            "formatted_value": "",
            "comparison": None,
            "error": "Invalid JSON configuration",
        }
    except Exception as e:
        return {
            "formatted_value": "",
            "comparison": None,
            "error": f"Error processing single value: {str(e)}",
        }
