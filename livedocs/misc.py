from google.cloud import bigquery
import pandas as pd
import polars as pl
import requests
import json

def get_workspace_connection_details(wsid, auth_token, env):
    # URL of the GraphQL endpoint
    url = {
        "dev": 'http://0.0.0.0:4000/',
        "staging" : "https://staging.livedocs.com/",
        "prod": "https://api.livedocs.com/"
    }

    # Headers, including your authorization token
    headers = {
        'authorization': auth_token,
        'content-type': 'application/json'
    }

    # GraphQL query
    query = """
    query GetWorkspaceConnectionDetails($wsid: String!) {
        getWorkspaceConnectionDetails(wsid: $wsid) {
            workspace_secrets {
                id
                key
                value
            }
            databases {
                id
                name
                database_type
                connection_details {
                    password
                    host
                    port
                    database
                    user_name
                    certificate
                    root_certificate
                    order
                    ssl_key
                    ssl_password
                }
            }
            bigquery_creds {
                type
                project_id
                private_key_id
                client_email
                client_id
                auth_uri
                token_uri
                auth_provider_x509_cert_url
                client_x509_cert_url
            }
        }
    }
    """

    # Variables for the query
    variables = {
        'wsid': wsid
    }

    # Payload for the request
    payload = {
        'query': query,
        'variables': variables
    }

    # Make the request
    response = requests.post(url[env], headers=headers, data=json.dumps(payload), verify=False)

    # Print the response
    if response.status_code == 200:
        print(response.json()['data']['getWorkspaceConnectionDetails'])
        return response.json()['data']['getWorkspaceConnectionDetails']
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        print("Response:", response.text)
        return None

def pandas_to_polars(df: pd.DataFrame) -> pl.DataFrame:
    return pl.from_pandas(df)

def polars_to_pandas(df: pl.DataFrame) -> pd.DataFrame:
    return df.to_pandas()

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