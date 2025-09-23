import json

import altair as alt
import polars as pl

from livedocs.types import (
    CacheInfo,
    DatabaseType,
    ElementDataSource,
    ElementDatasourceType,
    HistogramSpec,
    LivedocsChartSpec,
    LivedocsSwappedChartSpec,
    PieChartSpec,
    Spec,
    StyleSettings,
    SubplotSettings,
    VegaSpec,
)
from livedocs.utils.common import (
    _get_color,
    _get_color_group_key,
    _get_user_defined_color,
    _get_user_defined_opacity,
    _get_darkmode_color,
    create_line,
    get_axis_format,
    iso_to_alt_datetime,
)


def get_altair_datasource_query(datasource: ElementDataSource):
    """
    Prepares the DuckDB query for each datasource
    """
    match datasource["source_type"]:
        case ElementDatasourceType.dataframe.value:
            return f"SELECT * FROM {datasource['dataframe_info']['df_name']}"
        case ElementDatasourceType.database_table.value:
            if (
                datasource["database_info"]["database_type"]
                == DatabaseType.Bigquery.value
            ):
                return f"SELECT * FROM {datasource['database_table_info']['schema_name']}.{datasource['database_table_info']['table_name']} LIMIT 500000;"
            elif (
                datasource["database_info"]["database_type"]
                == DatabaseType.Clickhouse.value
            ):
                return f'SELECT * FROM "{datasource["database_table_info"]["schema_name"]}"."{datasource["database_table_info"]["table_name"]}" LIMIT 500000;'
            else:
                return f'SELECT * FROM "{datasource["database_info"]["database_name"]}"."{datasource["database_table_info"]["schema_name"]}"."{datasource["database_table_info"]["table_name"]}" LIMIT 500000;'
        ###
        case "file":
            # print(datasource)
            return (
                f"SELECT * FROM read_csv_auto('{datasource['file_info']['file_name']}')"
            )
        ###

        case _:
            raise ValueError(
                f"Unsupported datasource type: {datasource['source_type']}"
            )


def create_vega_spec(df: pl.DataFrame, spec: Spec, schema: dict):
    """
    Returns a Vega spec for a given Livedocs chart configuration and a dataframe
    retrieved using get_altair_datasource_query method. In production, the vegafusion
    data transformer should be enabled, although that obscures the spec.

    This method only delegates the spec generation to other more specific functions
    based on the chartType parameter of the Livedocs chart config.
    """
    alt.data_transformers.enable("vegafusion")

    style_settings = spec.get("styleSettings", {})

    if spec.get("chartType"):
        subplots = spec.get("subplots", {})
        if spec["chartType"] == "main":
            (vega_spec, status) = main_chart(
                df, spec["chartSettings"], schema, style_settings, subplots
            )
        if spec["chartType"] == "swapped_main":
            (vega_spec, status) = swapped_main_chart(
                df, spec["swappedChartSettings"], schema, style_settings, subplots
            )
        elif spec["chartType"] == "histogram":
            (vega_spec, status) = histogram(
                df, spec["histogramSettings"], schema, style_settings, subplots
            )
        elif spec["chartType"] == "pie":
            (vega_spec, status) = pie(
                df, spec["pieSettings"], schema, style_settings, subplots
            )

        validated_spec = VegaSpec(
            **{
                "spec": vega_spec,
                "schema": schema,
                "status": status,
            }
        )
        return validated_spec.model_dump_json()
    else:
        empty_chart = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "usermeta": {
                "styleSettings": style_settings,
                "chartType": "main",
            },
        }
        validated_spec = VegaSpec(
            **{
                "spec": json.dumps(empty_chart, separators=(",", ":")),
                "schema": schema,
                "status": "EMPTY",
            }
        )
        return validated_spec.model_dump_json()


def pie(
    df: pl.DataFrame,
    settings: PieChartSpec,
    schema: dict,
    style: StyleSettings,
    subplots: SubplotSettings,
) -> tuple[str, str]:
    """
    Generates the Vega spec from a pie chart configuration.
    """
    usermeta = settings
    if "color_by" in settings:
        color_by_field = settings["color_by"].get("field", "")
        color_by_type = settings["color_by"].get("type", "")
        usermeta["color_by"] = {"field": color_by_field, "type": color_by_type}
    if "size_by" in settings:
        size_by_field = settings["size_by"].get("field", "")
        size_by_type = settings["size_by"].get("type", "")
        size_by_aggregate = settings["size_by"].get("aggregate", "none")
        usermeta["size_by"] = {
            "field": size_by_field,
            "type": size_by_type,
            "aggregate": size_by_aggregate,
        }

        # For dark mode
    color_mode = style.get("mode", "light")
    bg_col = "white"
    grid_col = "lightgray"
    axis_col = "black"
    tick_col = "black"

    if color_mode == "dark":
        bg_col = _get_darkmode_color("background")
        grid_col = _get_darkmode_color("grid lines")
        axis_col = _get_darkmode_color("axis labels")
        tick_col = _get_darkmode_color("tick labels")

    # Use x-axis style for label angle
    x_axis_settings = style.get("xAxis", {})
    labelAngle = x_axis_settings.get("labelAngle", 0)

    labelFontSize = style.get("fontSize", 10)

    format_type = settings.get("format", "")
    show_as = settings.get("show_as", "value")

    usermeta["format"] = format_type
    usermeta["show_as"] = show_as

    # Legend settings
    legend_show = style.get("legend", {}).get("show", True)
    legend_position = style.get("legend", {}).get("position", "right")
    legend_title = style.get("legend", {}).get("title", alt.Undefined)
    legend_font_size = style.get("fontSize", 10)

    legend = None
    if legend_show:
        legend = alt.Legend(
            labelFontSize=legend_font_size,
            titleFontSize=legend_font_size,
            title=legend_title
            if legend_show and legend_title
            else (color_by_field if legend_show else alt.Undefined),
            orient=legend_position,
        )

    tooltip_show = style.get("tooltip", True)

    if not usermeta.get("color_by", {}).get("field") or not usermeta.get(
        "size_by", {}
    ).get("field"):
        # Return an empty spec with usermeta if necessary fields are missing
        empty_spec = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "usermeta": {
                "chartType": "pie",
                "pieSettings": usermeta,
            },
        }
        return (json.dumps(empty_spec), "EMPTY")

    # Create tooltip for chart
    tooltip = create_tooltip(
        axis1_field=color_by_field,
        axis1_type="nominal",
        temporal_format=alt.Undefined,
        axis2_field=size_by_field,
        axis2_type="quantitative",
        aggregate=size_by_aggregate,
        tooltip_show=tooltip_show,
    )

    # Determine the theta encoding based on the aggregation type
    theta_encoding = alt.Theta(field=size_by_field, type="quantitative", stack=True)
    if size_by_aggregate != "none":
        theta_encoding = alt.Theta(
            field=size_by_field,
            type="quantitative",
            aggregate=size_by_aggregate,
            stack=True,
        )
    base = alt.Chart(df)

    ## Create calculated column based on selected aggregation
    if show_as == "percentage":
        if size_by_aggregate == "count":
            df = df.with_columns([pl.lit(len(df)).alias("__totalCount")])
            base = alt.Chart(df).transform_calculate(
                calculate="1/datum.__totalCount", as_="__percentOfTotal"
            )
        else:
            base = (
                alt.Chart(df)
                .transform_joinaggregate(
                    joinaggregate=[
                        {"op": "sum", "field": size_by_field, "as": "__totalCount"}
                    ]
                )
                .transform_calculate(
                    calculate=f"datum['{size_by_field}']/datum.__totalCount",
                    as_="__percentOfTotal",
                )
            )

    ## Aggregate variable to sum percentages instead of count
    text_aggregate = "none"

    if size_by_aggregate == "count" and show_as == "percentage":
        text_aggregate = "sum"
    elif size_by_aggregate and size_by_aggregate != "none":
        text_aggregate = size_by_aggregate
    else:
        text_aggregate = alt.Undefined

    # Generate the pie chart using Altair
    chart = base.mark_arc(cursor="pointer").encode(
        theta=theta_encoding,
        color=alt.Color(
            legend=legend,
            field=color_by_field,
            type=map_datatype_to_scale_type(settings["color_by"]["type"]),
            scale=alt.Scale(range=[_get_color(i) for i in range(8)]),
        ),
        tooltip=tooltip,
        opacity=alt.value(1),
    )

    text = base.mark_text(radius=150, angle=labelAngle, size=labelFontSize).encode(
        text=alt.Text(
            field=size_by_field if show_as != "percentage" else "__percentOfTotal",
            format=format_type,
            aggregate=text_aggregate,
            type="quantitative",
        ),
        color=alt.value("black"),
        detail=color_by_field,
        theta=theta_encoding,
    )

    # Nest the chart within a layer
    outer_layer = alt.layer(chart, text).properties(
        description="outer data layer",
    )

    final_chart = (
        alt.layer(outer_layer)
        .properties(
            width="container",
            height="container",
            usermeta={
                "chartType": "pie",
                "pieSettings": usermeta,
                "styleSettings": style,
                "subplots": subplots,
            },
        )
        .configure(background=bg_col)
    )

    # Convert the chart to Vega JSON spec
    vega_spec = final_chart.to_json(format="vega")
    return (vega_spec, "SUCCESS")


def histogram(
    df: pl.DataFrame,
    settings: HistogramSpec,
    schema: dict,
    style: StyleSettings,
    subplots: SubplotSettings,
) -> tuple[str, str]:
    """
    Generates the Vega spec from a histogram chart configuration.
    """
    if "field" not in settings:
        empty_chart = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "usermeta": {
                "chartType": "histogram",
                "histogramSettings": settings,
            },
        }
        return (json.dumps(empty_chart), "EMPTY")

    field = settings["field"]
    bin_type = settings.get("binBy", {}).get("type", "max_bins")
    bin_value = settings.get("binBy", {}).get("value", 10)
    format_type = settings.get("format", "count")

    x_axis_settings = style.get("xAxis", {})
    y_axis_settings = style.get("yAxis", {})

    usermeta = settings
    usermeta["field"] = field
    usermeta["binBy"] = {"type": bin_type, "value": bin_value}
    usermeta["format"] = format_type

    bin_params = {}

    if bin_type == "max_bins":
        bin_params["maxbins"] = bin_value
    elif bin_type == "step_size":
        bin_params["step"] = bin_value

    tooltip_show = style.get("tooltip", True)

    tooltip = create_tooltip(
        axis1_field="__count" if format_type == "count" else "__PercentOfTotal",
        axis1_type="quantitative",
        temporal_format=None,
        axis2_field="__bin_field_name",
        axis2_type="nominal",
        aggregate="none",
        tooltip_show=tooltip_show,
        axis1_title="Count of Records"
        if format_type == "count"
        else "Percentage of Records",
        axis2_title=field,
        axis1_format=alt.Undefined if format_type == "count" else ".1%",
    )

    base = (
        alt.Chart(df)
        .transform_bin(as_="__bin_field_name", field=field, bin=bin_params)
        .transform_aggregate(
            aggregate=[{"op": "count", "as": "__count"}],
            groupby=["__bin_field_name", "__bin_field_name_end"],
        )
    )

    if format_type == "percent":
        base = base.transform_joinaggregate(
            joinaggregate=[{"op": "sum", "field": "__count", "as": "__totalCount"}],
            groupby=[],
        ).transform_calculate(
            calculate="datum.__count / datum.__totalCount", as_="__PercentOfTotal"
        )

    # For dark mode
    color_mode = style.get("mode", "light")
    bg_col = "white"
    grid_col = "lightgray"
    axis_col = "black"
    tick_col = "black"

    if color_mode == "dark":
        bg_col = _get_darkmode_color("background")
        grid_col = _get_darkmode_color("grid lines")
        axis_col = _get_darkmode_color("axis labels")
        tick_col = _get_darkmode_color("tick labels")

    base = (
        base.transform_calculate(
            calculate="'[' + toString(datum['__bin_field_name']) + ', ' + toString(datum['__bin_field_name_end']) + ')'",
            as_="__bin_range",
        )
        .mark_bar(clip=True, filled=True, cursor="pointer")
        .encode(
            x=alt.X(
                field="__bin_field_name",
                type="quantitative",
                title=field,
                axis=alt.Axis(
                    tickMinStep=bin_value if bin_type == "step_size" else alt.Undefined,
                    title=field
                    if "xAxis" not in style
                    else style["xAxis"].get("title", field),
                    titleFontSize=style.get("fontSize", 10),
                    labelFontSize=style.get("fontSize", 10),
                    labelAngle=x_axis_settings.get("labelAngle", alt.Undefined),
                    tickCount=x_axis_settings.get("ticks", alt.Undefined),
                    grid=True
                    if x_axis_settings.get("grid", "none") != "none"
                    else True,
                    format=x_axis_settings.get("format", alt.Undefined),
                    gridDash=[4, 4]
                    if x_axis_settings.get("grid", "none") == "dashed"
                    else alt.Undefined,
                    labelOverlap=True,
                    labelColor=axis_col,  # Dark mode
                    gridColor=grid_col,  # Dark mode
                    tickColor=tick_col,  # Dark mode
                ),
                scale=alt.Scale(
                    domainMax=int(x_axis_settings["max"])
                    if "max" in x_axis_settings
                    else alt.Undefined,
                    domainMin=int(x_axis_settings["min"])
                    if "min" in x_axis_settings
                    else alt.Undefined,
                    type=x_axis_settings.get("scale", alt.Undefined),
                ),
            ),
            x2="__bin_field_name_end",
            y=alt.Y(
                field="__count" if format_type == "count" else "__PercentOfTotal",
                type="quantitative",
                title="Count of Records"
                if format_type == "count"
                else "Percentage of Records",
                axis=alt.Axis(
                    title=field
                    if "yAxis" not in style
                    else style["yAxis"].get("title", field),
                    titleFontSize=style.get("fontSize", 10),
                    labelFontSize=style.get("fontSize", 10),
                    labelAngle=y_axis_settings.get("labelAngle", alt.Undefined),
                    tickCount=y_axis_settings.get("ticks", alt.Undefined),
                    grid=True
                    if y_axis_settings.get("grid", "none") != "none"
                    else True,
                    format=y_axis_settings.get("format", alt.Undefined)
                    if format_type == "count"
                    else "%",
                    gridDash=[4, 4]
                    if y_axis_settings.get("grid", "none") == "dashed"
                    else alt.Undefined,
                    labelOverlap=True,
                    labelColor=axis_col,  # Dark mode
                    gridColor=grid_col,  # Dark mode
                    tickColor=tick_col,  # Dark mode
                ),
                scale=alt.Scale(
                    domainMax=int(y_axis_settings["max"])
                    if "max" in y_axis_settings
                    else alt.Undefined,
                    domainMin=int(y_axis_settings["min"])
                    if "min" in y_axis_settings
                    else alt.Undefined,
                    type=y_axis_settings.get("scale", alt.Undefined),
                ),
            ),
            y2=alt.datum(0),
            tooltip=tooltip,
            opacity=alt.value(1),
            color=alt.value(_get_color(0)),
        )
    )

    outer_layer = (
        alt.layer(base)
        .properties(
            description="outer data layer",
        )
        .resolve_scale(color="independent", y="shared")
    )

    chart = alt.layer(outer_layer)

    x_lines = create_line(df, "x", style)
    y_lines = create_line(df, "y", style)

    # Create the layer and add to inner layers
    chart = (
        alt.layer(chart, *x_lines, *y_lines)
        .properties(
            width="container",
            height="container",
            usermeta={
                "chartType": "histogram",
                "histogramSettings": usermeta,
                "styleSettings": style,
                "subplots": subplots,
            },
        )
        .configure(background=bg_col)
    )

    vega_spec = chart.to_json(format="vega")
    return (vega_spec, "SUCCESS")


def create_tooltip(
    axis1_field,
    axis1_type,
    temporal_format,
    axis2_field,
    axis2_type,
    aggregate,
    color_by_field=None,
    color_by_type=None,
    color_by_aggregate=None,
    tooltip_show=True,
    axis1_title=None,
    axis2_title=None,
    axis1_format="none",
    axis2_format="none",
):
    if not tooltip_show:
        return alt.Undefined

    tooltips = [
        alt.Tooltip(
            field=axis1_field,
            type=axis1_type,
            title=axis1_title,
            timeUnit=temporal_format if temporal_format else alt.Undefined,
            format=axis1_format if axis1_format != "none" else alt.Undefined,
        ),
        alt.Tooltip(
            field=axis2_field,
            type=axis2_type,
            title=axis2_title
            if aggregate == "none"
            else f"{aggregate} of {axis2_field}".title(),
            aggregate=aggregate if aggregate != "none" else alt.Undefined,
            format=axis2_format if axis2_format != "none" else ",.1f",
        ),
    ]
    if color_by_field:
        tooltips.append(
            alt.Tooltip(
                field=color_by_field,
                type=color_by_type,
                title=color_by_field,
                aggregate=color_by_aggregate
                if color_by_aggregate != "none"
                else alt.Undefined,
            )
        )

    return tooltips


def main_chart(
    df: pl.DataFrame,
    settings: LivedocsChartSpec,
    schema: dict,
    style_settings: StyleSettings,
    subplots: SubplotSettings,
) -> tuple[str, str]:
    """
    Generates the Vega spec from a chart configuration for:
    - Line charts
    - Area, stacked area charts
    - Column charts (grouped, stacked, and full stacked)
    - Scatter charts
    """
    usermeta = settings

    legend_show = style_settings.get("legend", {}).get("show", True)
    legend_position = style_settings.get("legend", {}).get("position", "right")
    legend_title = style_settings.get("legend", {}).get("title", alt.Undefined)
    legend_font_size = style_settings.get("fontSize", 10)
    legend_font_color = "#93715A"  # dark mode

    tooltip_show = style_settings.get("tooltip", True)

    if "xAxis" not in settings or not settings["xAxis"].get("field"):
        # Set the right inferred type for the color by field
        if "yAxis" in settings and settings["yAxis"].get("primary"):
            for index, y_series in enumerate(settings["yAxis"]["primary"]):
                if y_series.get("color_by") and y_series["color_by"].get("field"):
                    usermeta["yAxis"]["primary"][index]["color_by"]["type"] = (
                        map_datatype_to_scale_type(
                            schema[y_series["color_by"]["field"]]
                        )
                    )

        empty_chart = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "usermeta": {
                "chartSettings": usermeta,
                "styleSettings": style_settings,
                "chartType": "main",
            },
        }
        return (json.dumps(empty_chart), "EMPTY")

    if (
        "yAxis" not in settings
        or not settings["yAxis"].get("primary")
        or all(not series.get("field") for series in settings["yAxis"]["primary"])
    ):
        default_y_field, default_y_type = get_first_field_by_preference(schema)
        settings["yAxis"] = {
            "primary": [
                {
                    "field": default_y_field,
                    "name": "line layer 1",
                    "aggregate": "none",
                    "mark": "grouped_column",
                    "type": default_y_type,
                    "temporalFormat": None,
                    "color_by": None,
                }
            ]
        }
        usermeta["yAxis"] = settings["yAxis"]

    x_field = settings["xAxis"]["field"]
    x_type = settings["xAxis"].get("type", map_datatype_to_scale_type(schema[x_field]))
    x_sort = settings["xAxis"].get("sort", "ascending")
    x_temporal_format = settings["xAxis"].get("temporalFormat")
    if x_type == "temporal" and x_temporal_format is None:
        x_temporal_format = "yearmonthdate"

    color_groups = {}

    usermeta["xAxis"] = {
        "field": x_field,
        "type": x_type,
        "sort": x_sort,
        "temporalFormat": x_temporal_format,
    }

    # Add transformation for temporal fields
    transform = []
    if x_type == "temporal":
        transform.append({"calculate": f"toDate(datum['{x_field}'])", "as": x_field})

    for y_series in settings["yAxis"]["primary"]:
        y_field = y_series["field"]
        y_type = y_series.get("type") or map_datatype_to_scale_type(schema[y_field])
        if y_type == "temporal":
            transform.append(
                {"calculate": f"toDate(datum['{y_field}'])", "as": y_field}
            )

    transform.append({"filter": f"isValid(datum['{x_field}'])"})

    inner_layers = []
    color_groups = {}

    for index, y_series in enumerate(settings["yAxis"]["primary"]):
        color_index = index
        series_color_name = y_series.get("name", "no-name-found-livedocs")
        custom_key = (
            f"{y_series['mark']} layer {index + 1}"
            if not series_color_name
            or series_color_name == "no-name-found-livedocs"
            or series_color_name == ""
            else series_color_name
        )

        y_field = y_series["field"]
        y_type = y_series.get("type") or map_datatype_to_scale_type(schema[y_field])
        y_aggregate = y_series.get("aggregate", "sum")
        mark_type = y_series.get("mark", "grouped_column")
        y_temporal_format = y_series.get("temporalFormat")

        y_encoding = create_y_encoding(
            y_field, y_type, y_aggregate, y_temporal_format, style_settings
        )
        x_encoding = create_x_encoding(
            x_field, x_type, x_sort, x_temporal_format, style_settings
        )
        color_by_field = None
        color_by_type = None
        color_by_encoding = None
        color_by_aggregate = None
        color_by_sort = None

        if y_series.get("color_by") and y_series["color_by"].get("field") != "none":
            color_by_field = y_series["color_by"].get("field", "none")
            unique_values = (
                df.select(pl.col(color_by_field).unique()).to_series().to_list()
            )
            color_groups[custom_key] = {
                _get_color_group_key(value): _get_user_defined_color(
                    custom_key,
                    _get_color_group_key(value),
                    style_settings,
                    color_index + i,
                )
                for i, value in enumerate(unique_values)
            }

            color_by_sort = y_series["color_by"].get("sort", "ascending")
            color_by_aggregate = y_series["color_by"].get("aggregate", "none")

            color_by_type = (
                map_datatype_to_scale_type(
                    y_series["color_by"].get("type") or schema[color_by_field]
                )
                if color_by_aggregate == "none"
                else "quantitative"
            )

            usermeta["yAxis"]["primary"][index]["color_by"]["type"] = (
                color_by_type if color_by_aggregate == "none" else "quantitative"
            )

            legend = None
            if legend_show:
                legend = alt.Legend(
                    labelFontSize=legend_font_size,
                    labelColor=legend_font_color,
                    titleFontSize=legend_font_size,
                    title=legend_title
                    if legend_show and legend_title
                    else (color_by_field if legend_show else alt.Undefined),
                    orient=legend_position,
                )

            color_by_encoding = alt.Color(
                field=color_by_field,
                type=color_by_type if color_by_aggregate == "none" else "quantitative",
                sort=color_by_sort,
                aggregate=color_by_aggregate
                if color_by_aggregate != "none"
                else alt.Undefined,
                title=legend_title
                if legend_show and legend_title
                else (color_by_field if legend_show else alt.Undefined),
                scale=alt.Scale(
                    domain=[_get_color_group_key(value) for value in unique_values],
                    range=[
                        _get_user_defined_color(
                            custom_key,
                            _get_color_group_key(value),
                            style_settings,
                            color_index + i,
                        )
                        for i, value in enumerate(unique_values)
                    ],
                ),
                legend=legend,
            )
        else:
            default_color = (
                style_settings.get("markSettings", {})
                .get(custom_key, {})
                .get("color", {})
                .get("hex", {})
                .get("default", _get_color(color_index))
            )
            color_by_encoding = alt.value(default_color)
            color_groups[custom_key] = default_color

        opacity_encoding = _get_user_defined_opacity(
            custom_key, style_settings, get_first_field_by_preference(schema)
        )

        if (
            not usermeta["yAxis"]["primary"][index].get("name")
            or usermeta["yAxis"]["primary"][index]["name"] == ""
        ):
            usermeta["yAxis"]["primary"][index]["name"] = (
                f"{y_series.get('mark', 'grouped_column')} layer {index + 1}"
            )

        # Create selectors
        brush = alt.selection_interval(encodings=["x"], name="brush")
        select = alt.selection_point(name="select", on="click")
        highlight = alt.selection_point(name="highlight", on="pointerover", empty=False)

        conditional_stroke = {
            "condition": [
                {"param": "select", "empty": False, "value": 2},
                {"param": "highlight", "empty": False, "value": 1},
            ],
            "value": 0,
        }

        # Create the tooltip
        tooltip = create_tooltip(
            axis1_field=x_field,
            axis1_type=x_type,
            axis2_field=y_field,
            axis2_type=y_type,
            aggregate=y_aggregate,
            temporal_format=x_temporal_format,
            color_by_field=color_by_field,
            color_by_type=color_by_type,
            color_by_aggregate=color_by_aggregate,
            tooltip_show=tooltip_show,
            axis1_title=x_field,
            axis2_title=y_field,
        )

        # MarkDataLabelsSettings
        layer_name = next(
            (
                k
                for k in style_settings.get("markSettings", {})
                if k.endswith("layer 1")
            ),
            {},
        )
        label_settings = {}
        if layer_name:
            label_settings = (
                style_settings.get("markSettings", {})
                .get(layer_name, {})
                .get("dataLabels", {})
            )

        labels_show = label_settings.get("show", False)
        labels_color = label_settings.get("color", "black")
        labels_angle = label_settings.get("angle", 0)
        labels_fontsize = label_settings.get("fontSize", 10)
        labels_position_input = label_settings.get("position", "outside-top")
        labels_mode = label_settings.get("mode", "per_color")  #  'per_color' or 'total'

        # For dark mode
        color_mode = style_settings.get("mode", "light")
        bg_col = "white"

        if color_mode == "dark":
            bg_col = _get_darkmode_color("background")

        # Labels position mapping
        label_position_map = {
            "inside-top": "top",
            "outside-top": "bottom",
            "center": "middle",
        }

        labels_position = label_position_map.get(labels_position_input, "bottom")

        # Black as default color
        if labels_color == "auto":
            labels_color = "black"

        # Add text encoding for data labels
        text = None

        text_encoding = alt.Text(
            field=y_field,
            aggregate=y_aggregate if y_aggregate != "none" else alt.Undefined,
            format=",.1f",
        )

        y_label_encoding = y_encoding
        if mark_type == "full_stacked_column":
            y_label_encoding = y_encoding.stack("normalize")
        elif labels_mode == "per_color" and mark_type.endswith("column"):
            y_label_encoding = y_encoding.stack("zero")
        elif mark_type == "line" and labels_mode == "per_color":
            y_label_encoding = y_encoding
        # else:
        elif (
            mark_type == "grouped_column"
            or mark_type == "line"
            or (mark_type == "point" and labels_mode == "total")
        ):
            y_label_encoding = alt.value(100)

        text = (
            alt.Chart(df)
            .mark_text(
                align="left" if x_temporal_format else "center",
                baseline=labels_position,
            )
            .encode(
                x=x_encoding,
                y=y_label_encoding,
                text=text_encoding,
                yOffset=alt.value(0),
                xOffset=color_by_field
                if (
                    mark_type == "grouped_column"
                    and color_by_field
                    and labels_mode == "per_color"
                )
                else alt.Undefined,
                detail=color_by_field
                if (color_by_field and labels_mode == "per_color")
                else alt.Undefined,
                color=alt.value(labels_color),
                angle=alt.value(labels_angle),
                size=alt.value(labels_fontsize),
                order=alt.Order(sort=color_by_sort) if color_by_sort else alt.Undefined,
            )
        )

        # Place text in center of bar
        if (
            labels_position == "middle"
            and labels_mode == "per_color"
            and mark_type == "stacked_column"
        ):
            text = text.encode(y=y_encoding.bandPosition(0.5).stack("zero"))
        elif (
            labels_position == "middle"
            and labels_mode == "total"
            and (mark_type == "stacked_column" or mark_type == "grouped_column")
        ):
            text = text.encode(
                y=alt.Y(
                    field="__label_position",
                    type="quantitative",
                    aggregate=y_aggregate if y_aggregate != "none" else alt.Undefined,
                )
            ).transform_calculate(
                calculate=f"datum['{y_field}'] / 2", as_="__label_position"
            )

        # Subplots
        h_subplot_settings = subplots.get("horizontal", {})
        h_subplot_field = h_subplot_settings.get("field", "none")
        h_subplot_wrap = h_subplot_settings.get("wrap", False)
        h_subplot_cols = h_subplot_settings.get("columns", 3)
        h_subplot_sort = h_subplot_settings.get("sort", "ascending")
        h_subplot_bin_bool = h_subplot_settings.get("bin", False)
        h_subplot_bin_count = h_subplot_settings.get("bin_count", 5)

        v_subplot_settings = subplots.get("vertical", {})
        v_subplot_field = v_subplot_settings.get("field", "none")
        v_subplot_linkYAxis = v_subplot_settings.get("linkYAxis", True)
        v_subplot_bin_bool = v_subplot_settings.get("bin", False)
        v_subplot_bin_count = v_subplot_settings.get("bin_count", 5)

        # Make facet plot
        facet = None
        h_facet_encoding = "none"
        v_facet_encoding = "none"

        if h_subplot_field != "none":
            h_facet_encoding = alt.Facet(
                field=h_subplot_field,
                sort=h_subplot_sort,
                bin=alt.Bin(maxbins=h_subplot_bin_count)
                if h_subplot_bin_bool
                else alt.Undefined,
            )
            facet = True

        if v_subplot_field != "none":
            v_facet_encoding = alt.Facet(
                field=v_subplot_field,
                bin=alt.Bin(maxbins=v_subplot_bin_count)
                if v_subplot_bin_bool
                else alt.Undefined,
            )
            facet = True

        # Create the appropriate mark type
        if mark_type == "grouped_column":
            if color_by_field:
                base_layer = (
                    alt.Chart(df)
                    .mark_bar(clip=True, stroke="black")
                    .encode(
                        x=x_encoding,
                        y=y_encoding,
                        xOffset=alt.XOffset(field=color_by_field, sort=color_by_sort),
                        color=alt.condition(
                            brush, color_by_encoding, alt.value("lightgray")
                        ),
                        order=alt.Order(color_by_field, sort=color_by_sort),
                        opacity=opacity_encoding,
                        fillOpacity=alt.condition(
                            select, opacity_encoding, alt.value(0.3)
                        ),
                        strokeWidth=conditional_stroke,
                        tooltip=tooltip,
                    )
                    .add_params(select, highlight, brush)
                )
            else:
                base_layer = (
                    alt.Chart(df)
                    .mark_bar(clip=True, stroke="black")
                    .encode(
                        x=x_encoding,
                        y=y_encoding,
                        opacity=opacity_encoding,
                        fillOpacity=alt.condition(
                            select, opacity_encoding, alt.value(0.3)
                        ),
                        strokeWidth=conditional_stroke,
                        color=alt.condition(
                            brush, color_by_encoding, alt.value("lightgray")
                        ),
                        tooltip=tooltip,
                    )
                    .add_params(select, highlight, brush)
                )

        elif mark_type == "stacked_column":
            if color_by_aggregate:
                base_layer = (
                    alt.Chart(df)
                    .mark_bar(clip=True, stroke="black")
                    .encode(
                        x=x_encoding,
                        y=y_encoding,
                        color=alt.condition(
                            brush, color_by_encoding, alt.value("lightgray")
                        ),
                        opacity=opacity_encoding,
                        fillOpacity=alt.condition(
                            select, opacity_encoding, alt.value(0.3)
                        ),
                        strokeWidth=conditional_stroke,
                        order=alt.Order(color_by_field, sort=color_by_sort),
                        tooltip=tooltip,
                    )
                    .add_params(select, highlight, brush)
                )
            else:
                base_layer = (
                    alt.Chart(df)
                    .mark_bar(clip=True, stroke="black")
                    .encode(
                        x=x_encoding,
                        y=y_encoding,
                        opacity=opacity_encoding,
                        fillOpacity=alt.condition(
                            select, opacity_encoding, alt.value(0.3)
                        ),
                        strokeWidth=conditional_stroke,
                        color=alt.condition(
                            brush, color_by_encoding, alt.value("lightgray")
                        ),
                        tooltip=tooltip,
                    )
                    .add_params(select, highlight, brush)
                )

        elif mark_type == "full_stacked_column":
            if color_by_aggregate:
                base_layer = (
                    alt.Chart(df)
                    .mark_bar(clip=True, stroke="black")
                    .encode(
                        x=x_encoding,
                        y=y_encoding.stack("normalize"),
                        color=alt.condition(
                            brush, color_by_encoding, alt.value("lightgray")
                        ),
                        opacity=opacity_encoding,
                        fillOpacity=alt.condition(
                            select, opacity_encoding, alt.value(0.3)
                        ),
                        strokeWidth=conditional_stroke,
                        order=alt.Order(color_by_field, sort=color_by_sort),
                        tooltip=tooltip,
                    )
                    .add_params(select, highlight, brush)
                )
            else:
                y_encoding["scale"]["domain"] = [0, 1]
                y_encoding["axis"]["format"] = "%"

                base_layer = (
                    alt.Chart(df)
                    .mark_bar(clip=True, stroke="black")
                    .encode(
                        x=x_encoding,
                        y=y_encoding.stack("normalize"),
                        opacity=opacity_encoding,
                        fillOpacity=alt.condition(
                            select, opacity_encoding, alt.value(0.3)
                        ),
                        strokeWidth=conditional_stroke,
                        color=alt.condition(
                            brush, color_by_encoding, alt.value("lightgray")
                        ),
                        tooltip=tooltip,
                    )
                    .add_params(select, highlight, brush)
                )

        elif mark_type == "line":
            ## Selectors and layers for line chart
            nearest = alt.selection_point(
                nearest=True,
                on="pointerover",
                empty=False,
                encodings=["x"],
                fields=[x_field]
                if x_temporal_format is None
                else [f"{x_temporal_format}({(x_field)})"],
            )

            if color_by_aggregate:
                select = alt.selection_point(fields=[color_by_field], bind="legend")

                lines = (
                    alt.Chart(df)
                    .mark_line(
                        clip=True,
                        strokeCap="square",
                        strokeJoin="round",
                    )
                    .encode(
                        x=x_encoding,
                        y=y_encoding,
                        color=alt.condition(
                            select, color_by_encoding, alt.value("lightgray")
                        ),
                    )
                ).add_params(select)

                points = lines.mark_point().transform_filter(nearest)

                ## Making tooltip for multiple categories
                if y_aggregate != "none":
                    tooltip_list = [
                        f"{y_aggregate}({col}):Q"
                        for col in df[f"{color_by_field}"].unique().to_list()
                    ]
                else:
                    tooltip_list = [
                        f"{col}:Q" for col in df[f"{color_by_field}"].unique().to_list()
                    ]

                ## Tooltip for x-field
                if x_temporal_format:
                    tooltip_list.append(f"{x_temporal_format}({x_field})")
                else:
                    tooltip_list.append(x_field)

                rules = (
                    alt.Chart(df)
                    .transform_pivot(color_by_field, value=y_field, groupby=[x_field])
                    .mark_rule(color="gray")
                    .encode(
                        x=x_encoding,
                        tooltip=tooltip_list if tooltip_show else alt.Undefined,
                        opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
                    )
                    .add_params(nearest)
                )

                base_layer = alt.layer(lines, points, rules)

            else:
                lines = (
                    alt.Chart(df)
                    .mark_line(
                        clip=True,
                        strokeCap="square",
                        strokeJoin="round",
                    )
                    .encode(
                        x=x_encoding,
                        y=y_encoding,
                        color=color_by_encoding,
                    )
                )

                points = lines.mark_point().transform_filter(nearest)

                rules = (
                    alt.Chart(df)
                    .mark_rule(color="gray")
                    .encode(
                        x=x_encoding,
                        tooltip=[
                            alt.Tooltip(
                                field=x_field,
                                type=x_type,
                                title=x_field,
                                timeUnit=x_temporal_format
                                if x_temporal_format
                                else alt.Undefined,
                            ),
                            alt.Tooltip(
                                field=y_field,
                                type=y_type,
                                title=y_field
                                if y_aggregate == "none"
                                else f"{y_aggregate} of {y_field}",
                                aggregate=y_aggregate
                                if y_aggregate != "none"
                                else alt.Undefined,
                                format=",.2f",
                            ),
                        ]
                        if tooltip_show
                        else alt.Undefined,
                        opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
                    )
                    .add_params(nearest)
                )

                series_list = settings.get("yAxis", {}).get("primary", [])
                if len(series_list) == 1:
                    base_layer = alt.layer(lines, points, rules)
                else:
                    base_layer = alt.layer(lines)

        elif mark_type == "point":
            brush = alt.selection_interval(encodings=["x"], name="brush")
            if color_by_aggregate:
                base_layer = (
                    alt.Chart(df)
                    .mark_circle(stroke="black", size=30)
                    .encode(
                        x=x_encoding,
                        y=y_encoding,
                        color=alt.condition(
                            brush, color_by_encoding, alt.value("lightgray")
                        ),
                        opacity=opacity_encoding,
                        fillOpacity=alt.condition(
                            select, opacity_encoding, alt.value(0.3)
                        ),
                        strokeWidth=conditional_stroke,
                        tooltip=tooltip,
                    )
                    .add_params(select, highlight, brush)
                )

            else:
                base_layer = (
                    alt.Chart(df)
                    .mark_circle(stroke="black", size=30)
                    .encode(
                        x=x_encoding,
                        y=y_encoding,
                        opacity=opacity_encoding,
                        fillOpacity=alt.condition(
                            select, opacity_encoding, alt.value(0.3)
                        ),
                        strokeWidth=conditional_stroke,
                        color=alt.condition(
                            brush, color_by_encoding, alt.value("lightgray")
                        ),
                        tooltip=tooltip,
                    )
                    .add_params(select, highlight, brush)
                )

        elif mark_type == "stacked_area":
            if color_by_aggregate:
                base_layer = (
                    alt.Chart(df)
                    .mark_area(
                        clip=True,
                        strokeJoin="round",
                    )
                    .encode(
                        x=x_encoding,
                        y=y_encoding,
                        color=color_by_encoding,
                        opacity=opacity_encoding,
                        order=alt.Order(color_by_field, sort=color_by_sort),
                        tooltip=tooltip,
                    )
                )

            else:
                base_layer = (
                    alt.Chart(df)
                    .mark_area(
                        clip=True,
                        line=True,
                        strokeJoin="round",
                    )
                    .encode(
                        x=x_encoding,
                        y=y_encoding,
                        color=color_by_encoding,
                        opacity=opacity_encoding,
                        tooltip=tooltip,
                    )
                )

        elif mark_type == "full_stacked_area":
            if color_by_aggregate:
                base_layer = (
                    alt.Chart(df)
                    .mark_area(
                        clip=True,
                        strokeJoin="round",
                    )
                    .encode(
                        x=x_encoding,
                        y=y_encoding.stack("normalize"),
                        color=color_by_encoding,
                        opacity=opacity_encoding,
                        order=alt.Order(color_by_field, sort=color_by_sort),
                        tooltip=tooltip,
                    )
                )

            else:
                y_encoding["scale"]["domain"] = [0, 1]
                y_encoding["axis"]["format"] = "%"

                base_layer = (
                    alt.Chart(df)
                    .mark_area(
                        clip=True,
                        line=True,
                        strokeJoin="round",
                    )
                    .encode(
                        x=x_encoding,
                        y=y_encoding.stack("normalize"),
                        color=color_by_encoding,
                        opacity=opacity_encoding,
                        tooltip=tooltip,
                    )
                )

        x_lines = create_line(df, "x", style_settings)
        y_lines = create_line(df, "y", style_settings)

        # Create the layer and add to inner layers
        inner_layers.append(base_layer)
        if labels_show is True:
            inner_layers.append(text)
        chart = alt.layer(*inner_layers)

    ## For tooltips on charts with added series
    series_list = settings.get("yAxis", {}).get("primary", [])
    if len(series_list) > 1:
        nearest2 = alt.selection_point(
            nearest=True,
            on="pointerover",
            empty=False,
            encodings=["x"],
            fields=[x_field]
            if x_temporal_format is None
            else [f"{x_temporal_format}({(x_field)})"],
        )

        tooltip_list = []
        for col in series_list:
            if col["field"] == "none":
                tooltip = alt.Tooltip(alt.Undefined)
            else:
                tooltip = alt.Tooltip(
                    field=col["field"],
                    type=col["type"],
                    title=col["field"]
                    if col["aggregate"] == "none"
                    else f"{col['aggregate']} of {col['field']}".title(),
                    aggregate=col["aggregate"]
                    if col["aggregate"] != "none"
                    else alt.Undefined,
                )
            tooltip_list.append(tooltip)

        tooltip_list.insert(
            0,
            alt.Tooltip(
                field=x_field,
                type=x_type,
                title=x_field,
                timeUnit=x_temporal_format if x_temporal_format else alt.Undefined,
                format=alt.Undefined,
            ),
        )

        final_rule = (
            alt.Chart(df)
            .mark_rule()
            .encode(
                x=x_encoding,
                tooltip=tooltip_list,
                opacity=alt.condition(nearest2, alt.value(1), alt.value(0)),
            )
            .add_params(nearest2)
        )

        chart = alt.layer(*inner_layers, *x_lines, *y_lines, final_rule)

    else:
        chart = alt.layer(*inner_layers, *x_lines, *y_lines)

    for t in transform:
        if "calculate" in t:
            chart = chart.transform_calculate(**t)
        if "filter" in t:
            chart = chart.transform_filter(t["filter"])

    # Add scale resolution
    chart = chart.resolve_scale(color="independent", y="shared")

    if facet:
        chart = chart.properties(
            width=200,
            height=200,
        )
    else:
        chart = chart.properties(
            width="container",
            height="container",
            usermeta={
                "chartType": "main",
                "chartSettings": usermeta,
                "styleSettings": style_settings,
                "colorGroups": color_groups,
                "subplots": subplots,
            },
        ).configure(background=bg_col)

    # Facet if required
    if facet:
        chart = chart.facet(
            column=h_facet_encoding if h_facet_encoding != "none" else alt.Undefined,
            columns=h_subplot_cols if h_subplot_wrap else alt.Undefined,
            row=v_facet_encoding if v_facet_encoding != "none" else alt.Undefined,
            usermeta={
                "chartType": "main",
                "chartSettings": usermeta,
                "styleSettings": style_settings,
                "colorGroups": color_groups,
                "subplots": subplots,
            },
        )

    vega_spec = chart.to_json(format="vega")
    return (vega_spec, "SUCCESS")


def swapped_main_chart(
    df: pl.DataFrame,
    settings: LivedocsSwappedChartSpec,
    schema: dict,
    style_settings: StyleSettings,
    subplots: SubplotSettings,
) -> tuple[str, str]:
    """
    Generates the Vega spec from a chart configuration where the UI indicates
    that the Axes of the chart have been swapped. Eg: Horizontal bar chart,
    grouped, stacked, and full stacked.
    """
    usermeta = settings

    legend_show = style_settings.get("legend", {}).get("show", True)
    legend_position = style_settings.get("legend", {}).get("position", "right")
    legend_title = style_settings.get("legend", {}).get("title", alt.Undefined)
    legend_font_size = style_settings.get("fontSize", 10)

    tooltip_show = style_settings.get("tooltip", True)

    if "yAxis" not in settings or not settings["yAxis"].get("field"):
        empty_chart = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "usermeta": {
                "swappedChartSettings": usermeta,
                "styleSettings": style_settings,
                "chartType": "swapped_main",
            },
        }
        return (json.dumps(empty_chart), "EMPTY")

    if "xAxis" not in settings or not settings["xAxis"].get("field"):
        default_x_field, default_x_type = get_first_field_by_preference(schema)
        settings["xAxis"] = {
            "field": default_x_field,
            "name": "bar layer 1",
            "aggregate": "none",
            "mark": "bar",
            "type": default_x_type,
            "temporalFormat": None,
            "color_by": None,
        }
        usermeta["yAxis"] = settings["yAxis"]

    x_field = settings["xAxis"]["field"]
    x_type = settings["xAxis"].get("type", map_datatype_to_scale_type(schema[x_field]))
    x_sort = settings["xAxis"].get("sort", "ascending")
    x_temporal_format = settings["xAxis"].get("temporalFormat")
    if x_type == "temporal" and x_temporal_format is None:
        x_temporal_format = "yearmonthdate"

    x_aggregate = settings["xAxis"].get("aggregate", "sum")
    x_color_by = settings["xAxis"].get("color_by")

    y_field = settings["yAxis"]["field"]
    y_type = settings["yAxis"].get("type", map_datatype_to_scale_type(schema[y_field]))
    y_temporal_format = settings["yAxis"].get("temporalFormat")
    mark_type = settings["yAxis"].get("mark", "grouped_bar")
    y_aggregate = settings["yAxis"].get("aggregate", "none")

    transform = []
    if y_type == "temporal":
        transform.append({"calculate": f"toDate(datum['{y_field}'])", "as": y_field})

    if x_type == "temporal":
        transform.append({"calculate": f"toDate(datum['{x_field}'])", "as": x_field})

    transform.append({"filter": f"isValid(datum['{y_field}'])"})

    color_index = 0
    custom_key = f"{mark_type} layer 1"

    color_by_field = None
    color_by_type = None
    color_by_encoding = None
    color_by_aggregate = None
    color_by_sort = None

    if x_color_by:
        color_by_field = x_color_by["field"]
        unique_values = df.select(pl.col(color_by_field).unique()).to_series().to_list()
        color_by_type = map_datatype_to_scale_type(
            x_color_by.get("type") or schema[color_by_field]
        )
        color_by_sort = x_color_by.get("sort", "ascending")
        color_by_aggregate = x_color_by.get("aggregate", "none")

        usermeta["xAxis"]["color_by"]["type"] = color_by_type

        legend = None
        if legend_show:
            legend = alt.Legend(
                labelFontSize=legend_font_size,
                titleFontSize=legend_font_size,
                title=legend_title
                if legend_show and legend_title
                else (color_by_field if legend_show else alt.Undefined),
                orient=legend_position,
            )

        color_by_encoding = alt.Color(
            field=color_by_field,
            type=color_by_type,
            sort=color_by_sort,
            aggregate=color_by_aggregate
            if color_by_aggregate != "none"
            else alt.Undefined,
            title=legend_title
            if legend_show and legend_title
            else (color_by_field if legend_show else alt.Undefined),
            scale=alt.Scale(
                domain=[_get_color_group_key(value) for value in unique_values],
                range=[
                    _get_user_defined_color(
                        custom_key,
                        _get_color_group_key(value),
                        style_settings,
                        color_index + i,
                    )
                    for i, value in enumerate(unique_values)
                ],
            ),
            legend=legend,
        )
    else:
        default_color = (
            style_settings.get("markSettings", {})
            .get(custom_key, {})
            .get("color", {})
            .get("hex", {})
            .get("default", _get_color(color_index))
        )
        color_by_encoding = alt.value(default_color)

    opacity_encoding = _get_user_defined_opacity(
        custom_key, style_settings, get_first_field_by_preference(schema)
    )

    y_encoding = create_y_encoding(
        y_field, y_type, y_aggregate, y_temporal_format, style_settings
    )
    x_encoding = create_x_encoding(
        x_field,
        x_type,
        x_sort,
        x_temporal_format,
        style_settings,
        x_aggregate,
        horizontal=True,
    )

    # Create selectors
    brush = alt.selection_interval(encodings=["y"], name="brush")
    select = alt.selection_point(name="select", on="click")
    highlight = alt.selection_point(name="highlight", on="pointerover", empty=False)

    conditional_stroke = {
        "condition": [
            {"param": "select", "empty": False, "value": 2},
            {"param": "highlight", "empty": False, "value": 1},
        ],
        "value": 0,
    }

    # Create the tooltip
    tooltip = create_tooltip(
        axis1_field=y_field,
        axis1_type=y_type,
        axis2_field=x_field,
        axis2_type=x_type,
        aggregate=x_aggregate,
        temporal_format=y_temporal_format,
        color_by_field=color_by_field,
        color_by_type=color_by_type,
        color_by_aggregate=color_by_aggregate,
        tooltip_show=tooltip_show,
    )

    if mark_type == "grouped_bar":
        if color_by_field:
            base_layer = (
                alt.Chart(df)
                .mark_bar(clip=True, filled=True, stroke="black")
                .encode(
                    x=x_encoding,
                    y=y_encoding,
                    color=alt.condition(
                        brush, color_by_encoding, alt.value("lightgray")
                    ),
                    opacity=opacity_encoding,
                    fillOpacity=alt.condition(select, opacity_encoding, alt.value(0.3)),
                    strokeWidth=conditional_stroke,
                    yOffset=alt.YOffset(field=color_by_field, sort=color_by_sort)
                    if color_by_encoding
                    else None,
                    tooltip=tooltip,
                )
                .add_params(select, highlight, brush)
            )

        else:
            base_layer = (
                alt.Chart(df)
                .mark_bar(clip=True, filled=True, stroke="black")
                .encode(
                    x=x_encoding,
                    y=y_encoding,
                    opacity=opacity_encoding,
                    fillOpacity=alt.condition(select, opacity_encoding, alt.value(0.3)),
                    strokeWidth=conditional_stroke,
                    color=alt.condition(
                        brush, color_by_encoding, alt.value("lightgray")
                    ),
                    tooltip=tooltip,
                )
                .add_params(select, highlight, brush)
            )

    elif mark_type == "stacked_bar":
        if color_by_field:
            base_layer = (
                alt.Chart(df)
                .mark_bar(clip=True, filled=True, stroke="black")
                .encode(
                    x=x_encoding,
                    y=y_encoding,
                    color=alt.condition(
                        brush, color_by_encoding, alt.value("lightgray")
                    ),
                    opacity=opacity_encoding,
                    fillOpacity=alt.condition(select, opacity_encoding, alt.value(0.3)),
                    strokeWidth=conditional_stroke,
                    order=alt.Order(color_by_field, sort=color_by_sort),
                    tooltip=tooltip,
                )
                .add_params(select, highlight, brush)
            )

        else:
            base_layer = (
                alt.Chart(df)
                .mark_bar(clip=True, filled=True, stroke="black")
                .encode(
                    x=x_encoding,
                    y=y_encoding,
                    opacity=opacity_encoding,
                    fillOpacity=alt.condition(select, opacity_encoding, alt.value(0.3)),
                    strokeWidth=conditional_stroke,
                    color=alt.condition(
                        brush, color_by_encoding, alt.value("lightgray")
                    ),
                    tooltip=tooltip,
                )
                .add_params(select, highlight, brush)
            )

    elif mark_type == "full_stacked_bar":
        if color_by_field:
            base_layer = (
                alt.Chart(df)
                .mark_bar(clip=True, filled=True, stroke="black")
                .encode(
                    x=x_encoding.stack("normalize"),
                    y=y_encoding,
                    color=alt.condition(
                        brush, color_by_encoding, alt.value("lightgray")
                    ),
                    opacity=opacity_encoding,
                    fillOpacity=alt.condition(select, opacity_encoding, alt.value(0.3)),
                    strokeWidth=conditional_stroke,
                    order=alt.Order(color_by_field, sort=color_by_sort),
                    tooltip=tooltip,
                )
                .add_params(select, highlight, brush)
            )

        else:
            x_encoding["scale"] = alt.Scale(domain=[0, 1])
            x_encoding["axis"]["format"] = "%"

            base_layer = (
                alt.Chart(df)
                .mark_bar(clip=True, filled=True, stroke="black")
                .encode(
                    x=x_encoding.stack("normalize"),
                    y=y_encoding,
                    opacity=opacity_encoding,
                    fillOpacity=alt.condition(select, opacity_encoding, alt.value(0.3)),
                    strokeWidth=conditional_stroke,
                    color=alt.condition(
                        brush, color_by_encoding, alt.value("lightgray")
                    ),
                    tooltip=tooltip,
                )
                .add_params(select, highlight, brush)
            )

    chart = alt.layer(base_layer)

    x_lines = create_line(df, "x", style_settings)
    y_lines = create_line(df, "y", style_settings)

    chart = alt.layer(chart, *x_lines, *y_lines)

    for t in transform:
        if "calculate" in t:
            chart = chart.transform_calculate(**t)
        if "filter" in t:
            chart = chart.transform_filter(t["filter"])

    chart = chart.resolve_scale(color="independent", y="shared")

    # Set background based on style settings (light/dark)
    color_mode = style_settings.get("mode", "light")
    bg_col = "white"
    if color_mode == "dark":
        bg_col = _get_darkmode_color("background")

    chart = chart.properties(
        width="container",
        height="container",
        usermeta={
            "chartType": "swapped_main",
            "swappedChartSettings": usermeta,
            "styleSettings": style_settings,
            "subplots": subplots,
        },
    ).configure(background=bg_col)

    vega_spec = chart.to_json(format="vega")
    return (vega_spec, "SUCCESS")


def create_x_encoding(
    field: str,
    type: str,
    sort: str,
    temporal_format: str,
    style: StyleSettings,
    x_aggregate: str = "none",
    horizontal: bool = False,
):
    axis_settings = style.get("xAxis", {})

    # For dark mode
    color_mode = style.get("mode", "light")
    bg_col = "white"
    grid_col = "lightgray"
    axis_col = "black"
    tick_col = "black"

    if color_mode == "dark":
        bg_col = _get_darkmode_color("background")
        grid_col = _get_darkmode_color("grid lines")
        axis_col = _get_darkmode_color("axis labels")
        tick_col = _get_darkmode_color("tick labels")

    if type == "temporal" and temporal_format and temporal_format != "none":
        return alt.X(
            f"{x_aggregate}({field}):T" if x_aggregate != "none" else f"{field}:T",
            timeUnit=temporal_format,
            axis=alt.Axis(
                title=field
                if "xAxis" not in style
                else style["xAxis"].get("title", field),
                format=get_axis_format(temporal_format),
                titleFontSize=style.get("fontSize", 10),
                labelFontSize=style.get("fontSize", 10),
                labelAngle=axis_settings.get("labelAngle", alt.Undefined),
                tickCount=axis_settings.get("ticks", alt.Undefined),
                grid=True if axis_settings.get("grid", "none") != "none" else True,
                gridDash=[4, 4]
                if axis_settings.get("grid", "none") == "dashed"
                else alt.Undefined,
                labelOverlap=True,
                labelColor=axis_col,  # Dark mode
                gridColor=grid_col,  # Dark mode
                tickColor=tick_col,  # Dark mode
            ),
            scale=alt.Scale(
                domainMax=iso_to_alt_datetime(axis_settings["max"])
                if "max" in axis_settings
                else alt.Undefined,
                domainMin=iso_to_alt_datetime(axis_settings["min"])
                if "min" in axis_settings
                else alt.Undefined,
                type=axis_settings.get("scale", alt.Undefined),
            ),
        )
    else:
        return alt.X(
            f"{x_aggregate}({field}):{type}"
            if x_aggregate != "none"
            else f"{field}:{type}",
            axis=alt.Axis(
                title=field
                if "xAxis" not in style
                else style["xAxis"].get("title", field),
                titleFontSize=style.get("fontSize", 10),
                labelFontSize=style.get("fontSize", 10),
                labelAngle=axis_settings.get("labelAngle", alt.Undefined),
                tickCount=axis_settings.get("ticks", alt.Undefined),
                grid=True if axis_settings.get("grid", "none") != "none" else True,
                format=axis_settings.get("format", alt.Undefined),
                gridDash=[4, 4]
                if axis_settings.get("grid", "none") == "dashed"
                else alt.Undefined,
                labelOverlap=True,
                labelColor=axis_col,  # Dark mode
                gridColor=grid_col,  # Dark mode
                tickColor=tick_col,  # Dark mode
            ),
            scale=alt.Scale(
                domainMax=int(axis_settings["max"])
                if "max" in axis_settings
                else alt.Undefined,
                domainMin=int(axis_settings["min"])
                if "min" in axis_settings
                else alt.Undefined,
                type=axis_settings.get("scale", alt.Undefined),
            ),
        )


def create_y_encoding(
    field: str, type: str, aggregate: str, temporal_format: str, style: StyleSettings
):
    axis_settings = style.get("yAxis", {})

    color_mode = style.get("mode", "light")
    bg_col = "white"
    grid_col = "lightgray"
    axis_col = "black"
    tick_col = "black"

    if color_mode == "dark":
        bg_col = _get_darkmode_color("background")
        grid_col = _get_darkmode_color("grid lines")
        axis_col = _get_darkmode_color("axis labels")
        tick_col = _get_darkmode_color("tick labels")

    if type == "temporal" and temporal_format and temporal_format != "none":
        return alt.Y(
            f"{aggregate}({field}):T" if aggregate != "none" else f"{field}:T",
            timeUnit=temporal_format,
            axis=alt.Axis(
                title=field
                if "yAxis" not in style
                else style["yAxis"].get("title", field),
                format=get_axis_format(temporal_format),
                titleFontSize=style.get("fontSize", 10),
                labelFontSize=style.get("fontSize", 10),
                labelAngle=axis_settings.get("labelAngle", alt.Undefined),
                tickCount=axis_settings.get("ticks", alt.Undefined),
                grid=True if axis_settings.get("grid", "none") != "none" else True,
                gridDash=[4, 4]
                if axis_settings.get("grid", "none") == "dashed"
                else alt.Undefined,
                labelOverlap=True,
                labelColor=axis_col,  # Dark mode
                gridColor=grid_col,  # Dark mode
                tickColor=tick_col,  # Dark mode
            ),
            scale=alt.Scale(
                domainMax=iso_to_alt_datetime(axis_settings["max"])
                if "max" in axis_settings
                else alt.Undefined,
                domainMin=iso_to_alt_datetime(axis_settings["min"])
                if "min" in axis_settings
                else alt.Undefined,
                type=axis_settings.get("scale", alt.Undefined),
            ),
        )
    else:
        return alt.Y(
            f"{aggregate}({field}):{type}"
            if aggregate != "none"
            else f"{field}:{type}",
            axis=alt.Axis(
                title=field
                if "yAxis" not in style
                else style["yAxis"].get("title", field),
                titleFontSize=style.get("fontSize", 10),
                labelFontSize=style.get("fontSize", 10),
                labelAngle=axis_settings.get("labelAngle", alt.Undefined),
                tickCount=axis_settings.get("ticks", alt.Undefined),
                grid=True if axis_settings.get("grid", "none") != "none" else True,
                format=axis_settings.get("format", alt.Undefined),
                gridDash=[4, 4]
                if axis_settings.get("grid", "none") == "dashed"
                else alt.Undefined,
                labelOverlap=True,
                labelColor=axis_col,  # Dark mode
                gridColor=grid_col,  # Dark mode
                tickColor=tick_col,  # Dark mode
            ),
            scale=alt.Scale(
                domainMax=int(axis_settings["max"])
                if "max" in axis_settings
                else alt.Undefined,
                domainMin=int(axis_settings["min"])
                if "min" in axis_settings
                else alt.Undefined,
                type=axis_settings.get("scale", alt.Undefined),
            ),
        )


def map_datatype_to_scale_type(type: str) -> str:
    """
    Maps the Livedocs primitive type to it's respective Vega field type
    """
    type_mapping = {"STRING": "nominal", "NUMBER": "quantitative", "DATE": "temporal"}
    return type_mapping.get(type, "nominal")


def get_first_field_by_preference(schema: dict) -> tuple[str, str]:
    """
    Picks a random field from a given schema to be used in a secondary axis
    """
    type_preference = {
        "NUMBER": "quantitative",
        "STRING": "nominal",
        "DATE": "temporal",
    }

    # Find non-ID fields first
    for preferred_type in ["NUMBER", "STRING", "DATE"]:
        for col, col_type in schema.items():
            if (
                col_type == preferred_type
                and col.lower() != "id"
                and not col.lower().endswith("id")
            ):
                return col, type_preference[col_type]

    # Fall back to any field
    for preferred_type in ["NUMBER", "STRING", "DATE"]:
        for col, col_type in schema.items():
            if col_type == preferred_type:
                return col, type_preference[col_type]

    raise ValueError("No suitable field found in the schema")
