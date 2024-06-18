from jinja2 import Environment, BaseLoader
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import pandas as pd
import polars as pl
import os
import re
import json

def parse_jinja_expression(expression):
    env = Environment(loader=BaseLoader())
    template = env.from_string(expression)

    variables = re.findall(r'{{\s*(.*?)\s*}}', expression)

    return template, variables


def evaluate_jinja_expression(expression, context):
    template, variables = parse_jinja_expression(expression)
    if not variables:
        return expression 
    rendered_template = template.render(context)
    return rendered_template

def run_bigquery_query(query, bq_client):
    query_job = bq_client.query(query)
    results = query_job.result().to_dataframe()
    return results

def split_and_replace_query(query, wsid):
    # Regular expression to match the FROM clause and the following table reference
    from_clause_pattern = r'\bFROM\b\s+([`a-zA-Z0-9_\-\.]+)'
    
    # Find the part of the query that matches the FROM clause pattern
    match = re.search(from_clause_pattern, query, re.IGNORECASE)
    
    if match:
        # Extract the current source reference
        current_source = match.group(1)
        new_source =""
        
        if current_source.split(".")[0].startswith("livedocs"):
            new_source = current_source
        else:
            converted_wsid = wsid.replace("-", "")

            new_source = f"liveocs-dev.s_{converted_wsid}_{current_source}"
        
        # Split the query at the FROM clause
        before_from = query[:match.start()]
        after_from = query[match.end():]
        
        # Replace the current source with the new source
        new_query = f"{before_from}FROM {new_source} {after_from}"
        print("new_query")
        print(new_query)
        
        return new_query
    print("query")
    print(query)
    return query


def parse_bq_query(query, context,  wsid, client):
    parsed_exp = evaluate_jinja_expression(query, context)
    parsed_query= split_and_replace_query(parsed_exp, wsid)
    print(parse_bq_query)
    data = run_bigquery_query(parsed_query, client)
    return pl.DataFrame(data) 

def create_bigquery_client(bq_creds):
    # Convert the dictionary to a JSON string
    json_str = json.dumps(bq_creds)

    # Create credentials object from the service account info
    credentials = Credentials.from_service_account_info(json.loads(json_str))

   
    # Create a BigQuery client using the credentials
    client = bigquery.Client(credentials=credentials, project=bq_creds["project_id"])


    return client