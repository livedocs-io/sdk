
import pandas as pd
import sqlite3
from jinja2 import Environment, BaseLoader

def parse_jinja_expression(expression):
    env = Environment(loader=BaseLoader())
    template = env.from_string(expression)
    variables = template.variable_end_string.split() if template.variable_end_string else []
    return template, variables

def execute_dataframe_query(query, dataframe):
    try:
        df = dataframe

        # Convert Polars DataFrame to Pandas DataFrame
        pandas_df = df.to_pandas()

        # Create an in-memory SQLite database and load the Pandas DataFrame into it
        conn = sqlite3.connect(':memory:')
        pandas_df.to_sql('my_table', conn, index=False)

        # Execute SQL query on the DataFrame
        query_result = pd.read_sql_query(query, conn)
        return query_result
    except Exception as e:
        print(f"Error executing Polars query: {e}")
        return None

def run_dataframe_query(query, dataframe):
    template, variables = parse_jinja_expression(query)
    if not variables:
        return query, None  # No variables to substitute, return original query and None as result
    rendered_query = template.render()  # Render the template without context (assuming variables are already set in the query)
    results = execute_dataframe_query(rendered_query, dataframe)
    return rendered_query, results
