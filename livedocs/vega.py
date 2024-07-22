import altair as alt
import polars as pl

from livedocs.types import ElementDataSource, ElementDatasourceType, LivedocsChartSpec


def _get_altair_datasource_query(datasource: ElementDataSource) -> str:
    match ElementDatasourceType(datasource["source_type"]):
        case ElementDatasourceType.file:
            return f"SELECT * FROM {datasource['file_info']['file_name']} limit 5000"
        case ElementDatasourceType.dataframe:
            return f"SELECT * FROM {datasource['dataframe_info']['df_name']} limit 5000"
        case ElementDatasourceType.database_table:
            return f"SELECT * FROM {datasource['database_table_info']['table_name']} limit 5000"
        case _:
            return "unknown datasource"


def map_datatype_to_scale_type(type: str) -> str:
    type_mapping = {"STRING": "nominal", "NUMBER": "quantitative", "DATE": "temporal"}
    return type_mapping.get(type, "nominal")


def create_vega_spec(df: pl.DataFrame, settings: LivedocsChartSpec, schema: dict):
    # Enable the VegaFusion data transformer
    alt.data_transformers.enable("vegafusion")

    usermeta = settings
    base = alt.Chart(df).properties(
        width="container",
        height="container",
    )

    x_field = settings["xAxis"]["field"]
    x_type = map_datatype_to_scale_type(schema[x_field])
    x_sort = settings["xAxis"].get("sort", "ascending")

    usermeta["xAxis"] = {"field": x_field, "type": x_type, "sort": x_sort}

    x_encoding = alt.X(f"{x_field}:{x_type}", sort=x_sort)

    # Apply Y-axis settings or create a default one
    if "yAxis" not in settings or not settings["yAxis"].get("primary"):
        # Create a default Y-axis with the first suitable field based on the schema
        y_field, y_type = get_first_field_by_preference(schema)
        y_encoding = alt.Y(f"{y_field}:{y_type}", title="Value")
        color = alt.value("steelblue")

        usermeta["yAxis"] = {
            "primary": [{"field": y_field, "name": y_field, "aggregate": "none"}]
        }

        chart = base.mark_line().encode(x=x_encoding, y=y_encoding, color=color)
    else:
        # TODO: Implement custom Y-axis settings
        # For now, just use the first primary Y-axis series
        y_series = settings["yAxis"]["primary"][0]
        y_field, y_frontend_type = y_series["field"].split("$$::$$")
        y_type = map_datatype_to_scale_type(y_frontend_type)
        y_aggregate = y_series["aggregate"]
        y_encoding = alt.Y(
            f"{y_aggregate}({y_field}):{y_type}", title=y_series.get("name", y_field)
        )
        chart = base.mark_line().encode(x=x_encoding, y=y_encoding)

    chart = chart.properties(usermeta=usermeta)
    spec = chart.to_json(format="vega")
    return spec


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
