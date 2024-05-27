from bq import  create_bigquery_client, parse_bq_query
from pg import create_pg_cred_dict, parse_pg_query
from df import run_dataframe_query
from misc import save_dataframe
from datetime import datetime
import os


class Lib:
    def __init__(self, bq_creds, pg_creds, secrets_dict, worskpace_id):
        self.bq_creds = bq_creds
        self.pg_creds = create_pg_cred_dict(pg_creds)
        self.secrets_dict = secrets_dict
        self.wsid = worskpace_id
        self.bq_client = create_bigquery_client(bq_creds)
        self.curr_run = datetime.now()
        

    
    def parse_bq(query, context, self):
        return parse_bq_query(query, context, self.wsid, self.bq_client)
    
    def parse_pg(query,db_name, self):

        return parse_pg_query(query, db_name, self.pg_creds)
    
    def parse_df(query, df):
        return run_dataframe_query(query, df)
    
    def save_to_bq(data, dataset_id, table_id, client):
        return save_dataframe(data, dataset_id, table_id, client)