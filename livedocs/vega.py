import json
import uuid

import altair as alt
import polars as pl

from livedocs.types import LivedocsChartSpec


def generate_unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _get_altair_datasource_query(datasource):
    match datasource["source_type"]:
        case "file":
            return f"SELECT * FROM {datasource['file_info']['file_name']} limit 5000"
        case "dataframe":
            return f"SELECT * FROM {datasource['dataframe_info']['df_name']} limit 5000"
        case "database_table":
            return f"SELECT * FROM {datasource['database_table_info']['table_name']} limit 5000"
        case _:
            return "unknown datasource"


def map_datatype_to_scale_type(type: str) -> str:
    type_mapping = {"STRING": "nominal", "NUMBER": "quantitative", "DATE": "temporal"}
    return type_mapping.get(type, "nominal")


def clean_spec_for_logging(spec):
    """Remove the 'data' part from the spec for cleaner logging."""
    spec_dict = json.loads(spec)
    if "data" in spec_dict:
        spec_dict["data"] = {"values": "[data removed for logging]"}
    if "datasets" in spec_dict:
        spec_dict["datasets"] = {"values": "[data removed for logging]"}
    return json.dumps(spec_dict, indent=2)


def dataframe_info(df: pl.DataFrame):
    """Return a string with information about the Polars DataFrame."""
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


def create_vega_spec(df: pl.DataFrame, settings: LivedocsChartSpec, schema: dict):
    # alt.data_transformers.enable("vegafusion")

    usermeta = settings

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
                    "name": default_y_field,
                    "aggregate": "none",
                    "mark": "line",
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
    for y_series in settings["yAxis"]["primary"]:
        y_field = y_series["field"]
        y_type = y_series.get("type") or map_datatype_to_scale_type(schema[y_field])
        y_aggregate = y_series.get("aggregate", "sum")
        mark_type = y_series.get("mark", "line")
        y_temporal_format = y_series.get("temporalFormat")

        y_encoding = create_y_encoding(y_field, y_type, y_aggregate, y_temporal_format)
        x_encoding = create_x_encoding(x_field, x_type, x_sort, x_temporal_format)

        color_by_encoding = None
        if y_series.get("color_by"):
            color_by_field = y_series["color_by"]["field"]
            color_by_type = map_datatype_to_scale_type(
                y_series["color_by"].get("type") or schema[color_by_field]
            )
            color_by_sort = y_series["color_by"].get("sort", "ascending")
            color_by_aggregate = y_series["color_by"].get("aggregate", "none")

            if color_by_aggregate != "none":
                color_by_encoding = alt.Color(
                    field=color_by_field,
                    type=color_by_type,
                    sort=color_by_sort,
                    aggregate=color_by_aggregate,
                    title=color_by_field,
                )
            else:
                color_by_encoding = alt.Color(
                    field=color_by_field,
                    type=color_by_type,
                    sort=color_by_sort,
                    title=color_by_field,
                )

        # Create the appropriate mark type
        if mark_type == "bar" or (
            mark_type == "grouped_column" and not color_by_encoding
        ):
            base_layer = (
                alt.Chart(df)
                .mark_bar(clip=True)
                .encode(
                    x=x_encoding,
                    y=y_encoding,
                    color=color_by_encoding or alt.value("#4C78A8"),
                    opacity=alt.value(1),
                )
            )
        elif mark_type == "grouped_column" and color_by_aggregate:
            base_layer = (
                alt.Chart(df)
                .mark_bar(clip=True)
                .encode(
                    x=x_encoding,
                    y=y_encoding,
                    color=color_by_encoding or alt.value("#4C78A8"),
                    opacity=alt.value(1),
                    xOffset=alt.XOffset(field=color_by_field)
                    if color_by_encoding
                    else None,
                )
            )
        elif mark_type == "stacked_column":
            norm_start = generate_unique_name("norm_start")
            norm_end = generate_unique_name("norm_end")

            if color_by_encoding:
                base_layer = (
                    alt.Chart(df)
                    .mark_bar(clip=True)
                    .encode(
                        x=x_encoding,
                        y=alt.Y(f"{norm_start}:Q", title=y_field),
                        y2=f"{norm_end}:Q",
                        color=color_by_encoding,
                        opacity=alt.value(1),
                    )
                    .transform_calculate(value=f"datum['{y_field}']")
                    .transform_stack(
                        stack="value",
                        groupby=[x_field, color_by_field],
                        offset="normalize",
                        sort=[],
                        as_=[norm_start, norm_end],
                    )
                )
            else:
                base_layer = (
                    alt.Chart(df)
                    .mark_bar(clip=True)
                    .encode(
                        x=x_encoding,
                        y=alt.Y(f"{norm_start}:Q", title=y_field),
                        y2=f"{norm_end}:Q",
                        color=alt.value("#4C78A8"),
                        opacity=alt.value(1),
                    )
                    .transform_calculate(value=f"datum['{y_field}']")
                    .transform_stack(
                        stack="value",
                        groupby=[x_field],
                        offset="normalize",
                        sort=[],
                        as_=[norm_start, norm_end],
                    )
                )
        elif mark_type == "line":
            base_layer = (
                alt.Chart(df)
                .mark_line(
                    clip=True,
                    strokeCap="square",
                    strokeJoin="round",
                    cursor="crosshair",
                )
                .encode(
                    x=x_encoding,
                    y=y_encoding,
                    color=color_by_encoding or alt.value("#4C78A8"),
                    opacity=alt.value(1),
                )
            )
        elif mark_type == "point":
            base_layer = (
                alt.Chart(df)
                .mark_point(clip=True, cursor="crosshair")
                .encode(
                    x=x_encoding,
                    y=y_encoding,
                    color=color_by_encoding or alt.value("#4C78A8"),
                    opacity=alt.value(1),
                )
            )
        elif mark_type == "area":
            base_layer = (
                alt.Chart(df)
                .mark_area(
                    clip=True,
                    point=False,
                    line=True,
                    strokeJoin="round",
                    cursor="crosshair",
                )
                .encode(
                    x=x_encoding,
                    y=y_encoding,
                    color=color_by_encoding or alt.value("#4C78A8"),
                    opacity=alt.value(1),
                )
            )
        else:
            base_layer = (
                alt.Chart(df)
                .mark_line(
                    clip=True,
                    strokeCap="square",
                    strokeJoin="round",
                    cursor="crosshair",
                )
                .encode(
                    x=x_encoding,
                    y=y_encoding,
                    color=color_by_encoding or alt.value("#4C78A8"),
                    opacity=alt.value(1),
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

    chart = chart.properties(width="container", height="container", usermeta=usermeta)

    vega_spec = chart.to_json()

    print(clean_spec_for_logging(vega_spec))
    return vega_spec


def create_x_encoding(field: str, type: str, sort: str, temporal_format: str):
    tick_count_expr = "length(domain('x')) > 0 ? min(ceil(width / 40), ceil((domain('x')[1] - domain('x')[0]) / 7884000000)) : ceil(width / 40)"

    axis_props = alt.Axis(title=field, tickCount=alt.expr(tick_count_expr))

    if type == "temporal" and temporal_format and temporal_format != "none":
        axis_props.format = get_axis_format(temporal_format)
        return alt.X(
            f"{field}:O",
            timeUnit=temporal_format,
            bandPosition=0,
            sort=sort,
            axis=axis_props,
        )
    else:
        return alt.X(f"{field}:{type}", sort=sort, axis=axis_props)


def create_y_encoding(field: str, type: str, aggregate: str, temporal_format: str):
    if type == "temporal" and temporal_format and temporal_format != "none":
        return alt.Y(
            f"{aggregate}({field}):T" if aggregate != "none" else f"{field}:T",
            timeUnit=temporal_format,
            axis=alt.Axis(title=field, format=get_axis_format(temporal_format)),
        )
    else:
        return alt.Y(
            f"{aggregate}({field}):{type}"
            if aggregate != "none"
            else f"{field}:{type}",
            axis=alt.Axis(title=field),
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


def get_first_field_by_preference(schema: dict) -> tuple[str, str]:
    type_preference = {
        "NUMBER": "quantitative",
        "STRING": "nominal",
        "DATE": "temporal",
    }

    for preferred_type in ["NUMBER", "STRING", "DATE"]:
        for col, col_type in schema.items():
            if col_type == preferred_type:
                return col, type_preference[col_type]

    raise ValueError("No suitable field found in the schema")


__all__ = [
    "_get_altair_datasource_query",
    "create_vega_spec",
]
