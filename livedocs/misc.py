from google.cloud import bigquery

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
    # Define your BigQuery dataset and table ID
    dataset_id = 'your_dataset_id'
    table_id = 'your_table_id'
    schema = create_schema_from_dataframe(data)
    # Load data into BigQuery
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(
        data, f"{dataset_id}.{table_id}", job_config=job_config
    )
    job.result() 