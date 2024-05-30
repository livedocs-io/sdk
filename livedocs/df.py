
import pandas as pd
import sqlite3
from jinja2 import Environment, BaseLoader

def parse_jinja_expression(expression):
    env = Environment(loader=BaseLoader())
    template = env.from_string(expression)
    variables = template.variable_end_string.split() if template.variable_end_string else []
    return template, variables

def preprocess_dataframe(dataframe):
    # Convert columns with complex types (e.g., lists, dictionaries) to strings
    for col in dataframe.columns:
        # If the column contains dicts, lists or other complex types, convert them to strings
        if dataframe[col].apply(lambda x: isinstance(x, (dict, list, set, tuple))).any():
            dataframe[col] = dataframe[col].apply(lambda x: str(x) if isinstance(x, (dict, list, set, tuple)) else x)
    return dataframe

def execute_dataframe_query(query,df_name, dataframe):
    try:
        # Preprocess the DataFrame to handle unsupported data types
        dataframe = preprocess_dataframe(dataframe)
        
        # Create an in-memory SQLite database and load the Pandas DataFrame into it
        conn = sqlite3.connect(':memory:')

        dataframe.to_sql(df_name, conn, index=False)

        # Execute SQL query on the DataFrame
        query_result = pd.read_sql_query(query, conn)

        # Close the connection
        conn.close()

        return query_result
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return None

def run_dataframe_query(query,df_name, dataframe):
    results = execute_dataframe_query(query, df_name, dataframe)
    return results
