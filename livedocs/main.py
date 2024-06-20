from bq import  BigQueryExecutor
from pg import PostgresExecutor
from df import DataFrameQueryExecutor
from misc import save_dataframe, get_workspace_connection_details, setup_secrets
from chart import ChartGenerator
from flask import  jsonify
from datetime import datetime
from dotenv import load_dotenv


class Lib:
    def __init__(self,  auth_token, env):
        connection_details = get_workspace_connection_details( auth_token=auth_token, env=env)
        setup_secrets(connection_details['workspace_secrets'])
        self.env = env
        self.dataFrameExecutor = DataFrameQueryExecutor()
        self.chart_generator = ChartGenerator()
        self.postgresParser = PostgresExecutor()
        self.bigqueryParser = BigQueryExecutor(connection_details['bigquery_creds'])
        self.pg_creds = self.postgresParser.create_pg_cred_dict(connection_details['databases'])
        self.secrets_arr = connection_details['workspace_secrets']
        self.wsid = connection_details['workspace_id']
        self.curr_run = datetime.now()
        

    
    def run_bigquery(self, query, context):
        return self.bigqueryParser.parse_bq_query(query, context, self.wsid)
    

    def run_chart(self, config, data):
        chart_config = self.chart_generator.generate_highcharts_config(config=config, data=data)
        return jsonify(chart_config)
    
    def run_postgres(self, query,db_name):
        load_dotenv()
        return self.postgresParser.parse_pg_query(query, db_name, self.pg_creds)
    
    def run_dataframe(self, query,df_name, df):
        return self.dataFrameExecutor.run_dataframe_query(query,df_name, df)
    
    def save_to_bq(self, data, dataset_id, table_id, client):
        return save_dataframe(data, dataset_id, table_id, client)