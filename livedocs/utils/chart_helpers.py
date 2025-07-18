import polars as pl

from livedocs.types import Spec
from livedocs.utils.debug import debug


def apply_chart_filters(
    df: pl.DataFrame, schema: dict, settings: Spec, chart_metadata: dict
) -> pl.DataFrame:
    # debug(df)
    if settings.get("chartType") == "main":
        filter_column_name = settings.get("chartSettings", {}).get("xAxis").get("field")
        filter_column_type = schema.get(filter_column_name, {})

    elif settings.get("chartType") == "swapped_main":
        filter_column_name = (
            settings.get("swappedChartSettings", {}).get("yAxis").get("field")
        )
        filter_column_type = schema.get(filter_column_name, {})

    debug(filter_column_name)
    debug(filter_column_type)
    debug(chart_metadata)

    return df


__all__ = ["apply_chart_filters"]
