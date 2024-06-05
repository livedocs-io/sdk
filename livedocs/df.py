
import pandas as pd
import sqlite3
from jinja2 import Environment, BaseLoader
from misc import pandas_to_polars
import polars as pl

def parse_jinja_expression(expression):
    env = Environment(loader=BaseLoader())
    template = env.from_string(expression)
    variables = template.variable_end_string.split() if template.variable_end_string else []
    return template, variables

def preprocess_dataframe(dataframe):
    for col in dataframe.columns:
        if dataframe[col].apply(lambda x: isinstance(x, (dict, list, set, tuple))).any():
            dataframe[col] = dataframe[col].apply(lambda x: str(x) if isinstance(x, (dict, list, set, tuple)) else x)
    return dataframe

def preprocess_polars_dataframe(dataframe):
    for col in dataframe.columns:
        if dataframe[col].dtype == pl.Object and any(dataframe[col].apply(lambda x: isinstance(x, (dict, list, set, tuple)), return_dtype=pl.Boolean, skip_nulls=False)):
            dataframe = dataframe.with_column(col, dataframe[col].map(lambda x: str(x) if isinstance(x, (dict, list, set, tuple)) else x, return_dtype=pl.UTF8, skip_nulls=False))
    return dataframe


def execute_dataframe_query(query,df_name, dataframe):
    try:
        # Preprocess the DataFrame to handle unsupported data types
        processed_pl_df = preprocess_polars_dataframe(dataframe)

        result = processed_pl_df.sql(query)



        # Print the result
        print(result)
     

        return result
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return None

def run_dataframe_query(query,df_name, dataframe):
    results = execute_dataframe_query(query, df_name, dataframe)
    return results
