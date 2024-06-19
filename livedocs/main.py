from bq import  create_bigquery_client, parse_bq_query
from pg import create_pg_cred_dict, parse_pg_query
from df import run_dataframe_query
from misc import save_dataframe, get_workspace_connection_details, setup_secrets
from chart import ChartGenerator
from flask import  jsonify
from datetime import datetime
from dotenv import load_dotenv


class Lib:
    def __init__(self,  auth_token, env):
        connection_details = get_workspace_connection_details( auth_token=auth_token, env=env)
        print(connection_details['workspace_id'])
        setup_secrets(connection_details['workspace_secrets'])
        self.env = env
        self.bq_creds = connection_details['bigquery_creds']
        self.pg_creds = create_pg_cred_dict(connection_details['databases'])
        self.secrets_arr = connection_details['workspace_secrets']
        self.wsid = connection_details['workspace_id']
        self.bq_client = create_bigquery_client(connection_details['bigquery_creds'])
        self.curr_run = datetime.now()
        

    
    def parse_bq(self, query, context):
        return parse_bq_query(query, context, self.wsid, self.bq_client)
    

    def parse_chart(self, config, data):
        chart_generator = ChartGenerator().generate_highcharts_config(config=config, data=data)
        return jsonify(chart_generator)
    
    def parse_pg(self, query,db_name):
        load_dotenv()
        return parse_pg_query(query, db_name, self.pg_creds)
    
    def parse_df(self, query,df_name, df):
        return run_dataframe_query(query,df_name, df)
    
    def save_to_bq(self, data, dataset_id, table_id, client):
        return save_dataframe(data, dataset_id, table_id, client)