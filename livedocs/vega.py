import json
import uuid

import altair as alt
import polars as pl

from livedocs.types import (
    ElementDataSource,
    HistogramSpec,
    LivedocsChartSpec,
    LivedocsSwappedChartSpec,
    PieChartSpec,
    Spec,
    StyleSettings,
    VegaSpec,
)
from livedocs.utils.common import (
    _get_color,
    _get_color_group_key,
    _get_user_defined_color,
    _get_user_defined_opacity,
)

"""
Helper function, never used in production environments. 
It removes the data key from the vega spec dict for clearer logging. 
"""


def clean_spec_for_logging(spec):
    """Remove the 'data' part from the spec for cleaner logging."""
    spec_dict = json.loads(spec)
    if "data" in spec_dict:
        spec_dict["data"] = {"values": "[data removed for logging]"}
    if "datasets" in spec_dict:
        spec_dict["datasets"] = {"values": "[data removed for logging]"}

    return json.dumps(spec_dict, indent=2)


"""
Helper function, never used in a production environment. 
It returns a string that contains information about a polars dataframe 
in the absence of a Polars info() method like Pandas. 
"""


def dataframe_info(df: pl.DataFrame):
    info = [
        f"Polars DataFrame with {df.shape[0]} rows and {df.shape[1]} columns.",
        "Columns:",
    ]
    for col in df.columns:
        dtype = str(df[col].dtype)
        non_null = df.shape[0] - df[col].null_count()

        if isinstance(df[col].dtype, pl.datatypes.String):
            empty_string_count = df[col].is_null().sum() + (df[col] == "").sum()
            info.append(
                f"  {col}: {dtype} ({non_null} non-null, {empty_string_count} empty/null)"
            )
        else:
            info.append(f"  {col}: {dtype} ({non_null} non-null)")

    return "\n".join(info)


"""
Adds a UUIDV4 prefix to layer names
"""


def generate_unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


"""
Prepares the DuckDB query for each datasource
"""


def _get_altair_datasource_query(datasource: ElementDataSource):
    match datasource["source_type"]:
        case "file":
            return f"SELECT * FROM {datasource['file_info']['file_name']}"
        case "dataframe":
            return f"SELECT * FROM {datasource['dataframe_info']['df_name']}"
        case "database_table":
            return f"SELECT * FROM {datasource['database_info']['database_name']}.{datasource['database_table_info']['schema_name']}.{datasource['database_table_info']['table_name']}"
        case _:
            return "unknown datasource"


"""
Maps the Livedocs primitive type to it's respective Vega field type
"""


def map_datatype_to_scale_type(type: str) -> str:
    type_mapping = {"STRING": "nominal", "NUMBER": "quantitative", "DATE": "temporal"}
    return type_mapping.get(type, "nominal")


def convert_datetime_to_iso(df):
    for column in df.select_dtypes(include=["datetime64"]).columns:
        df[column] = df[column].dt.strftime("%Y-%m-%dT%H:%M:%S")
    return df


"""
Returns a Vega spec for a given Livedocs chart configuration and a dataframe
retrieved using _get_altair_datasource_query method. In production, the vegafusion
data transformer should be enabled, although that obscures the spec. 

This method only delegates the spec generation to other more specific functions
based on the chartType parameter of the Livedocs chart config. 
"""


def create_vega_spec(df: pl.DataFrame, spec: Spec, schema: dict):
    alt.data_transformers.enable("vegafusion")

    style_settings = spec.get("styleSettings", {})

    if df.height > 10000:
        empty_chart = {
            "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
            "usermeta": {
                "styleSettings": style_settings,
                "chartType": "main",
            },
        }
        validated_spec = VegaSpec(
            **{
                "spec": json.dumps(empty_chart),
                "schema": schema,
                "status": "OVERLOADED",
            }
        )
        return validated_spec.model_dump_json()
    else:
        if spec.get("chartType"):
            if spec["chartType"] == "main":
                (vega_spec, status) = main_chart(
                    df, spec["chartSettings"], schema, style_settings
                )
            if spec["chartType"] == "swapped_main":
                (vega_spec, status) = swapped_main_chart(
                    df, spec["swappedChartSettings"], schema, style_settings
                )
            elif spec["chartType"] == "histogram":
                (vega_spec, status) = histogram(
                    df, spec["histogramSettings"], schema, style_settings
                )
            elif spec["chartType"] == "pie":
                (vega_spec, status) = pie(
                    df, spec["pieSettings"], schema, style_settings
                )

            validated_spec = VegaSpec(
                **{"spec": vega_spec, "schema": schema, "status": status}
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
                **{"spec": json.dumps(empty_chart), "schema": schema, "status": "EMPTY"}
            )
            return validated_spec.model_dump_json()


"""
Generates the Vega spec from a pie chart configuration. 
"""


def pie(
    df: pl.DataFrame,
    settings: PieChartSpec,
    schema: dict,
    style_settings: StyleSettings,
) -> tuple[str, str]:
    usermeta = settings
    if "color_by" in settings:
        color_by_field = settings["color_by"].get("field", "")
        color_by_type = settings["color_by"].get("type", "")
        usermeta["color_by"] = {"field": color_by_field, "type": color_by_type}
        tooltip1 = alt.Tooltip(
            field=color_by_field,
            type=map_datatype_to_scale_type(settings["color_by"]["type"]),
            title=color_by_field,
        )
    if "size_by" in settings:
        size_by_field = settings["size_by"].get("field", "")
        size_by_type = settings["size_by"].get("type", "")
        size_by_aggregate = settings["size_by"].get("aggregate", "none")
        usermeta["size_by"] = {
            "field": size_by_field,
            "type": size_by_type,
            "aggregate": size_by_aggregate,
        }
        tooltip2 = alt.Tooltip(
            field=size_by_field,
            type="quantitative",
            aggregate=size_by_aggregate
            if size_by_aggregate != "none"
            else alt.Undefined,
            title="Count of Records" if size_by_aggregate == "count" else size_by_field,
            format=",.2f",
        )

    format_type = settings.get("format", "")
    show_as = settings.get("show_as", "value")

    usermeta["format"] = format_type
    usermeta["show_as"] = show_as

    legend_show = style_settings.get("legend", {}).get("show", True)
    legend_position = style_settings.get("legend", {}).get("position", "right")
    legend_title = style_settings.get("legend", {}).get("title", alt.Undefined)
    legend_font_size = style_settings.get("fontSize", 10)

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

    tooltip_show = style_settings.get("tooltip", True)

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


    # Determine the theta encoding based on the aggregation type
    theta_encoding = alt.Theta(field=size_by_field, type="quantitative", stack=True)
    if size_by_aggregate != "none":
        theta_encoding = alt.Theta(
            field=size_by_field,
            type="quantitative",
            aggregate=size_by_aggregate,
            stack=True,
        )

    base=alt.Chart(df)

    if show_as == "percentage":
        base = alt.Chart(df).transform_joinaggregate(
            joinaggregate=[{"op":"sum", "field": size_by_field, "as": "__totalCount"}],
            groupby=[]
        ).transform_calculate(
            calculate=f"datum.{size_by_field}/datum.__totalCount",
            as_="__percentOfTotal"
        )

    print(show_as)
    print(format_type)

    # Generate the pie chart using Altair
    chart = (
        base
        .mark_arc(cursor="pointer")
        .encode(
            theta=theta_encoding,
            color=alt.Color(
                legend=legend,
                field=color_by_field,
                type=map_datatype_to_scale_type(settings["color_by"]["type"]),
                scale=alt.Scale(
                    range=[
                        "#4C78A8",
                        "#F58518",
                        "#E45756",
                        "#72B7B2",
                        "#54A24B",
                        "#EECA3B",
                        "#B279A2",
                        "#FF9DA6",
                        "#9D755D",
                        "#BAB0AC",
                    ]
                ),
            ),
            tooltip=[tooltip1, tooltip2] if tooltip_show else alt.Undefined,
            opacity=alt.value(1),
        )
    )

    text=(base
        .mark_text(radius=150)
        .encode(
            text=alt.Text(
                field=size_by_field
                if show_as!="percentage"
                else "__percentOfTotal",
                format=format_type,
                aggregate=size_by_aggregate
                if size_by_aggregate and size_by_aggregate!="none"
                else alt.Undefined),
            color=alt.value("black"),
            detail=color_by_field,
            theta=theta_encoding
        )
    )

    # Nest the chart within a layer
    outer_layer = alt.layer(chart, text).properties(
        description="outer data layer",
    )

    final_chart = alt.layer(outer_layer).properties(
        width="container",
        height="container",
        usermeta={
            "chartType": "pie",
            "pieSettings": usermeta,
            "styleSettings": style_settings,
        },
    )

    # Convert the chart to Vega JSON spec
    vega_spec = final_chart.to_json(format="vega")
    return (vega_spec, "SUCCESS")


"""
Generates the Vega spec from a histogram chart configuration. 
"""


def histogram(
    df: pl.DataFrame,
    settings: HistogramSpec,
    schema: dict,
    style: StyleSettings,
) -> tuple[str, str]:
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

    tooltip1 = alt.Tooltip(
        field="__count" if format_type == "count" else "__PercentOfTotal",
        title="Count of Records" if format_type == "count" else "Percentage of Records",
        type="quantitative",
    )

    tooltip2 = alt.Tooltip("__bin_range", title=field, type="nominal")

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
                    if format_type=="count"
                    else "%",
                    gridDash=[4, 4]
                    if y_axis_settings.get("grid", "none") == "dashed"
                    else alt.Undefined,
                    labelOverlap=True,
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
            tooltip=[tooltip1, tooltip2] if tooltip_show else alt.Undefined,
            opacity=alt.value(1),
            color=alt.value("#4C78A8"),
        )
    )

    outer_layer = (
        alt.layer(base)
        .properties(
            description="outer data layer",
        )
        .resolve_scale(color="independent", y="shared")
    )

    chart = alt.layer(outer_layer).properties(
        width="container",
        height="container",
        usermeta={
            "chartType": "histogram",
            "histogramSettings": usermeta,
            "styleSettings": style,
        },
    )

    vega_spec = chart.to_json(format="vega")
    return (vega_spec, "SUCCESS")

###
def create_tooltip(x_field, 
                   x_type, 
                   x_temporal_format,
                   y_field, 
                   y_type, 
                   y_aggregate,
                   color_by_field=None,
                   color_by_type=None,
                   color_by_aggregate=None,
                   tooltip_show=True):
    if not tooltip_show:
        return alt.Undefined

    tooltips=[
        alt.Tooltip(
            field=x_field,
            type=x_type,
            title=x_field,
            timeUnit=x_temporal_format if x_temporal_format else alt.Undefined,
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
        )
    ]
    if color_by_field:
        # if not color_by_type:
        #     color_by_type="nominal"

        tooltips.append(
            alt.Tooltip(field=color_by_field, 
                        type=color_by_type,
                        title=color_by_field,
                        aggregate=color_by_aggregate
                        if color_by_aggregate!="none"
                        else alt.Undefined)
        )
    
    return tooltips

    


"""
Generates the Vega spec from a chart configuration for:
- Line charts
- Area, stacked area charts
- Column charts (grouped, stacked, and full stacked)
- Scatter charts
"""
def main_chart(
    df: pl.DataFrame,
    settings: LivedocsChartSpec,
    schema: dict,
    style_settings: StyleSettings,
) -> tuple[str, str]:
    usermeta = settings

    legend_show = style_settings.get("legend", {}).get("show", True)
    legend_position = style_settings.get("legend", {}).get("position", "right")
    legend_title = style_settings.get("legend", {}).get("title", alt.Undefined)
    legend_font_size = style_settings.get("fontSize", 10)

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
    if (x_type == "temporal" and x_temporal_format is None):
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
        color_by_field=None
        color_by_type=None
        color_by_encoding=None
        color_by_aggregate=None

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
                f"{y_series.get('mark', "grouped_column")} layer {index + 1}"
            )

        # Create selectors
        brush = alt.selection_interval(encodings=["x"])
        select = alt.selection_point(name="select", on="click")
        highlight = alt.selection_point(name="highlight", on="pointerover", empty=False)

        conditional_stroke = {
            "condition": [
                {"param": "select", "empty": False, "value": 2},
                {"param": "highlight", "empty": False, "value": 1},
            ],
            "value": 0,
        }

        print(color_by_type)

        tooltip=create_tooltip(
            x_field=x_field,
            x_type=x_type,
            y_field=y_field,
            y_type=y_type,
            y_aggregate=y_aggregate,
            x_temporal_format=x_temporal_format,
            color_by_field=color_by_field,
            color_by_type=color_by_type,
            color_by_aggregate=color_by_aggregate
        )

        # Create the appropriate mark type
        if mark_type == "grouped_column":
            if color_by_field:
                base_layer = (
                    alt.Chart(df)
                    .mark_bar(clip=True, stroke="black")
                    .encode(
                        x=x_encoding,
                        y=y_encoding,
                        xOffset=alt.XOffset(field=color_by_field
                        #  sort=color_by_sort),
                        ),
                        color=alt.condition(
                            brush, color_by_encoding, alt.value("lightgray")
                        ),
                        order=alt.Order(color_by_field, sort=color_by_sort),
                        opacity=opacity_encoding,
                        fillOpacity=alt.condition(
                            select, opacity_encoding, alt.value(0.3)
                        ),
                        strokeWidth=conditional_stroke,

                        tooltip=tooltip
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
                        tooltip=tooltip
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
                            alt.Tooltip(
                                field=color_by_field,
                                type=color_by_type,
                                title=color_by_field,
                                aggregate=color_by_aggregate
                                if color_by_aggregate != "none"
                                else alt.Undefined,
                            ),
                        ]
                        if tooltip_show
                        else alt.Undefined,
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
                    )
                    .add_params(select, highlight, brush)
                )

        elif mark_type == "full_stacked_column":
            # Generate the base layer chart
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
                            alt.Tooltip(
                                field=color_by_field,
                                type=color_by_type,
                                title=color_by_field,
                                aggregate=color_by_aggregate
                                if color_by_aggregate != "none"
                                else alt.Undefined,
                            ),
                        ]
                        if tooltip_show
                        else alt.Undefined,
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

                base_layer = alt.layer(lines, points, rules)

        elif mark_type == "point":
            brush = alt.selection_interval()
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
                            alt.Tooltip(
                                field=color_by_field,
                                type=color_by_type,
                                title=color_by_field,
                                aggregate=color_by_aggregate
                                if color_by_aggregate != "none"
                                else alt.Undefined,
                            ),
                        ]
                        if tooltip_show
                        else alt.Undefined,
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
                            alt.Tooltip(
                                field=color_by_field,
                                type=color_by_type,
                                title=color_by_field,
                                aggregate=color_by_aggregate
                                if color_by_aggregate != "none"
                                else alt.Undefined,
                            ),
                        ]
                        if tooltip_show
                        else alt.Undefined,
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
                            alt.Tooltip(
                                field=color_by_field,
                                type=color_by_type,
                                title=color_by_field,
                                aggregate=color_by_aggregate
                                if color_by_aggregate != "none"
                                else alt.Undefined,
                            ),
                        ]
                        if tooltip_show
                        else alt.Undefined,
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
                    )
                )

        # Create the layer and add to inner layers
        inner_layers.append(base_layer)
        chart = alt.layer(*inner_layers)

    for t in transform:
        if "calculate" in t:
            chart = chart.transform_calculate(**t)
        if "filter" in t:
            chart = chart.transform_filter(t["filter"])

    # Add scale resolution
    chart = chart.resolve_scale(color="independent", y="shared")

    chart = chart.properties(
        width="container",
        height="container",
        usermeta={
            "chartType": "main",
            "chartSettings": usermeta,
            "styleSettings": style_settings,
            "colorGroups": color_groups,
        },
    )

    vega_spec = chart.to_json(format="vega")
    return (vega_spec, "SUCCESS")


"""
Generates the Vega spec from a chart configuration where the UI indicates
that the Axes of the chart have been swapped. Eg: Horizontal bar chart, 
grouped, stacked, and full stacked.  
"""


def swapped_main_chart(
    df: pl.DataFrame,
    settings: LivedocsSwappedChartSpec,
    schema: dict,
    style_settings: StyleSettings,
) -> tuple[str, str]:
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

    x_aggregate = settings["xAxis"].get("aggregate", "sum")
    x_color_by = settings["xAxis"].get("color_by")

    y_field = settings["yAxis"]["field"]
    y_type = settings["yAxis"].get("type", map_datatype_to_scale_type(schema[y_field]))
    # y_sort = settings["yAxis"].get("sort", "ascending")
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

    color_by_encoding = None
    color_by_field = None

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
    brush = alt.selection_interval(encodings=["y"])
    select = alt.selection_point(name="select", on="click")
    highlight = alt.selection_point(name="highlight", on="pointerover", empty=False)

    conditional_stroke = {
        "condition": [
            {"param": "select", "empty": False, "value": 2},
            {"param": "highlight", "empty": False, "value": 1},
        ],
        "value": 0,
    }

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
                    tooltip=[
                        alt.Tooltip(
                            field=y_field,
                            type=y_type,
                            title=y_field,
                            timeUnit=y_temporal_format
                            if y_temporal_format
                            else alt.Undefined,
                        ),
                        alt.Tooltip(
                            field=x_field,
                            type=x_type,
                            title=x_field
                            if x_aggregate == "none"
                            else f"{x_aggregate} of  {x_field}",
                            aggregate=x_aggregate
                            if x_aggregate != "none"
                            else alt.Undefined,
                            format=",.2f",
                        ),
                        alt.Tooltip(
                            field=color_by_field,
                            type=color_by_type,
                            title=color_by_field,
                            aggregate=color_by_aggregate
                            if color_by_aggregate != "none"
                            else alt.Undefined,
                        ),
                    ]
                    if tooltip_show
                    else alt.Undefined,
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
                    tooltip=[
                        alt.Tooltip(
                            field=y_field,
                            type=y_type,
                            title=y_field,
                            timeUnit=y_temporal_format
                            if y_temporal_format
                            else alt.Undefined,
                        ),
                        alt.Tooltip(
                            field=x_field,
                            type=x_type,
                            title=x_field,
                            aggregate=x_aggregate
                            if x_aggregate != "none"
                            else alt.Undefined,
                            format=",.2f",
                        ),
                    ]
                    if tooltip_show
                    else alt.Undefined,
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
                    tooltip=[
                        alt.Tooltip(
                            field=y_field,
                            type=y_type,
                            title=y_field,
                            timeUnit=y_temporal_format
                            if y_temporal_format
                            else alt.Undefined,
                        ),
                        alt.Tooltip(
                            field=x_field,
                            type=x_type,
                            title=x_field
                            if x_aggregate == "none"
                            else f"{x_aggregate} of  {x_field}",
                            aggregate=x_aggregate
                            if x_aggregate != "none"
                            else alt.Undefined,
                            format=",.2f",
                        ),
                        alt.Tooltip(
                            field=color_by_field,
                            type=color_by_type,
                            title=color_by_field,
                            aggregate=color_by_aggregate
                            if color_by_aggregate != "none"
                            else alt.Undefined,
                        ),
                    ]
                    if tooltip_show
                    else alt.Undefined,
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
                    tooltip=[
                        alt.Tooltip(
                            field=y_field,
                            type=y_type,
                            title=y_field,
                            timeUnit=y_temporal_format
                            if y_temporal_format
                            else alt.Undefined,
                        ),
                        alt.Tooltip(
                            field=x_field,
                            type=x_type,
                            title=x_field,
                            aggregate=x_aggregate
                            if x_aggregate != "none"
                            else alt.Undefined,
                            format=",.2f",
                        ),
                    ]
                    if tooltip_show
                    else alt.Undefined,
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
                    tooltip=[
                        alt.Tooltip(
                            field=y_field,
                            type=y_type,
                            title=y_field,
                            timeUnit=y_temporal_format
                            if y_temporal_format
                            else alt.Undefined,
                        ),
                        alt.Tooltip(
                            field=x_field,
                            type=x_type,
                            title=x_field
                            if x_aggregate == "none"
                            else f"{x_aggregate} of  {x_field}",
                            aggregate=x_aggregate
                            if x_aggregate != "none"
                            else alt.Undefined,
                            format=",.2f",
                        ),
                        alt.Tooltip(
                            field=color_by_field,
                            type=color_by_type,
                            title=color_by_field,
                            aggregate=color_by_aggregate
                            if color_by_aggregate != "none"
                            else alt.Undefined,
                        ),
                    ]
                    if tooltip_show
                    else alt.Undefined,
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
                    tooltip=[
                        alt.Tooltip(
                            field=y_field,
                            type=y_type,
                            title=y_field,
                            timeUnit=y_temporal_format
                            if y_temporal_format
                            else alt.Undefined,
                        ),
                        alt.Tooltip(
                            field=x_field,
                            type=x_type,
                            title=x_field,
                            aggregate=x_aggregate
                            if x_aggregate != "none"
                            else alt.Undefined,
                            format=",.2f",
                        ),
                    ]
                    if tooltip_show
                    else alt.Undefined,
                )
                .add_params(select, highlight, brush)
            )

    chart = alt.layer(base_layer)

    for t in transform:
        if "calculate" in t:
            chart = chart.transform_calculate(**t)
        if "filter" in t:
            chart = chart.transform_filter(t["filter"])

    chart = chart.resolve_scale(color="independent", y="shared")

    chart = chart.properties(
        width="container",
        height="container",
        usermeta={
            "chartType": "swapped_main",
            "swappedChartSettings": usermeta,
            "styleSettings": style_settings,
        },
    )

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


"""
Picks a random field from a given schema to be used in a secondary axis
"""


def get_first_field_by_preference(schema: dict) -> tuple[str, str]:
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


__all__ = [
    "_get_altair_datasource_query",
    "create_vega_spec",
]
