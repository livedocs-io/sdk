from livedocs.types import ElementDataSource, ElementDatasourceType
import altair as alt
import polars as pl
import pandas as pd


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


def auto_visualize(df: pl.DataFrame):
    if len(df.columns) < 2:
        raise ValueError("DataFrame must have at least two columns for visualization")

    # Get dtypes of columns
    dtypes = df.dtypes

    # Identify numeric and categorical columns
    numeric_columns = [
        col
        for col, dtype in zip(df.columns, dtypes)
        if dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]
    ]
    categorical_columns = [
        col
        for col, dtype in zip(df.columns, dtypes)
        if dtype in [pl.Utf8, pl.Categorical]
    ]

    # Convert Polars DataFrame to pandas DataFrame, handling potential nested structures
    pandas_df = pd.DataFrame()
    for col in df.columns:
        try:
            pandas_df[col] = df[col].to_pandas()
        except ValueError:
            # If conversion fails, try converting to string
            pandas_df[col] = df[col].cast(pl.Utf8).to_pandas()

    if len(numeric_columns) >= 2:
        # Scatter plot for two numeric columns
        x = numeric_columns[0]
        y = numeric_columns[1]
        chart = (
            alt.Chart(pandas_df)
            .mark_circle()
            .encode(x=x, y=y, tooltip=list(df.columns))
            .interactive()
        )
    elif len(numeric_columns) == 1 and len(categorical_columns) >= 1:
        # Bar chart for one numeric and one categorical column
        x = categorical_columns[0]
        y = numeric_columns[0]
        chart = (
            alt.Chart(pandas_df)
            .mark_bar()
            .encode(x=x, y=y, tooltip=list(df.columns))
            .interactive()
        )
    elif len(categorical_columns) >= 2:
        # Heatmap for two categorical columns
        x = categorical_columns[0]
        y = categorical_columns[1]
        chart = (
            alt.Chart(pandas_df)
            .mark_rect()
            .encode(
                x=x,
                y=y,
                color="count()",
                tooltip=[x, y, alt.Tooltip("count()", title="Count")],
            )
            .interactive()
        )
    else:
        raise ValueError(
            "Unable to determine appropriate visualization for the given DataFrame"
        )

    return chart.properties(
        width=600,
        height=400,
        title=f"Automatic Visualization of {', '.join(df.columns)}",
    ).to_json()


__all__ = [
    "_get_altair_datasource_query",
    "auto_visualize",
]
