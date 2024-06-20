from google.cloud import bigquery
import polars as pl
import requests
import json
import os

def get_workspace_connection_details(auth_token , env):
    # URL of the GraphQL endpoint
    url = {
        "dev": 'http://0.0.0.0:4000/api/credentials',
        "staging" : "https://staging.livedocs.com/api/credentials",
        "prod": "https://api.livedocs.com/api/credentials"
    }

  
    headers = {
        'authorization': auth_token ,
        'content-type': 'application/json'
    }


    # Make the request
    response = requests.get(url[env], headers=headers, verify=False)

    # Print the response
    if response.status_code == 200:
        # print(response.json())
        return response.json()['data']
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        print("Response:", response.text)
        return None
    
def setup_secrets(secrets_arr):
    for secret in secrets_arr:
        os.environ[secret['key']] = secret['value']

def create_schema_from_dataframe(dataframe):
    schema = []
    for column_name, dtype in dataframe.dtypes.items():
        # Convert pandas dtype to BigQuery type
        if dtype == 'int64':
            bq_type = 'INTEGER'
        elif dtype == 'float64':
            bq_type = 'FLOAT'
        elif dtype == 'bool':
            bq_type = 'BOOLEAN'
        elif dtype == 'datetime64[ns]':
            bq_type = 'TIMESTAMP'
        else:
            bq_type = 'STRING'
        
        # Add SchemaField to schema list
        schema.append(bigquery.SchemaField(column_name, bq_type))
    
    return schema


def save_dataframe(data, dataset_id, table_id, client):
    schema = create_schema_from_dataframe(data)
    # Load data into BigQuery
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(
        data, f"{dataset_id}.{table_id}", job_config=job_config
    )
    job.result() 

