import psycopg2
from psycopg2 import sql
from jinja2 import Environment, BaseLoader
import pandas as pd
import re
import requests
import json
import os


def parse_jinja_expression(expression):
    env = Environment(loader=BaseLoader())
    template = env.from_string(expression)
    variables = template.variable_end_string.split() if template.variable_end_string else []
    return template, variables

def execute_postgres_query(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    description = cursor.description  # Save the description before closing the cursor
    cursor.close()
    return results, description

def run_postgres_query(query, connection):
    template, variables = parse_jinja_expression(query)
    if not variables:
        return query, pd.DataFrame()  # No variables to substitute, return original query and an empty DataFrame
    rendered_query = template.render()  # Render the template without context (assuming variables are already set in the query)
    results, description = execute_postgres_query(rendered_query, connection)
    # Convert results to DataFrame
    columns = [desc[0] for desc in description]
    results_df = pd.DataFrame(results, columns=columns)
    return rendered_query, results_df

# Connect to PostgreSQL database
def connect_to_postgres(host, port, database, user, password):
    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

def choose_db_creds(query, creds):
    # Regular expression to match the FROM clause and the following table reference
    from_clause_pattern = r'\bFROM\b\s+([`a-zA-Z0-9_\-\.]+)'
    
    # Find the part of the query that matches the FROM clause pattern
    match = re.search(from_clause_pattern, query, re.IGNORECASE)
    
    if match:
        # Extract the current source reference
        current_source = match.group(1)
        db_name = current_source.split(".")[0]
        return creds[db_name]
    else:
        return ""

# Close the connection
def close_postgres_connection(connection):
    connection.close()

def parse_pg_query(query, db_name, pg_creds):
    current_query_creds = pg_creds[db_name]
    # conn = connect_to_postgres(
    #     connect_to_postgres.host,
    #     connect_to_postgres.port,
    #     connect_to_postgres.database,
    #     connect_to_postgres.user,
    #     connect_to_postgres.password,
    # )
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        'query': query,
        'host':current_query_creds.host,
        'port':current_query_creds.port,
        'database':current_query_creds.database,
        'user':current_query_creds.user,
        'password':current_query_creds.password,
    }
    response = requests.post(os.env.get("PG_BASE_URL_STAGING"), headers=headers, data=json.dumps(data))
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.json().get('error')}")
        return None
    

def create_pg_cred_dict(creds_arr):
    creds_dict = {}
    for item in creds_arr:
        creds_dict[item.db_name] = item
    return creds_dict


def query_flask_server(query, livedocs_env):
    headers = {
        'Content-Type': 'application/json',
    }
    data = {
        'query': query,
    }
    response = requests.post(os.env.get("PG_BASE_URL_STAGING"), headers=headers, data=json.dumps(data))
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.json().get('error')}")
        return None
