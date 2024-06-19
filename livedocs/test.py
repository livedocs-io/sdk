# from bq import evaluate_jinja_expression, split_and_replace_query, run_bigquery_query,create_bigquery_client
# from pg import parse_pg_query
# from df import run_dataframe_query
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS
from chart import ChartGenerator
import os
from bq import create_bigquery_client, parse_bq_query
from misc import get_workspace_connection_details
from main import Lib

# cred = {
#         "type": "service_account",
#         "project_id": "livedocs-dev",
#         "private_key_id": "5e601d7eb2fe173de42852053e9e9e5e01201655",
#         "private_key":"",
#         "client_email": "",
#         "client_id": "",
#         "auth_uri": "",
#         "token_uri": "https://oauth2.googleapis.com/token",
#         "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#         "client_x509_cert_url": ""
#       }
# client = create_bigquery_client(cred)

# query = """
# SELECT
#   metric_date,
#   coalesce(
#     COUNT(
#       CASE
#         WHEN resource_id = '401b810e-74ed-42d2-a391-f73d46412e8c' THEN (
#           CASE
#             WHEN denominator > 0 THEN numerator / denominator
#             ELSE 0
#           END
#         )
#         ELSE NULL
#       END
#     ),
#     0
#   ) AS `Newfollowers`,
#   coalesce(
#     COUNT(
#       CASE
#         WHEN resource_id = '74c8fc6d-cddc-4a00-95b9-eca89fea5433' THEN (
#           CASE
#             WHEN denominator > 0 THEN numerator / denominator
#             ELSE 0
#           END
#         )
#         ELSE NULL
#       END
#     ),
#     0
#   ) AS `Engagement`
# FROM
#   linkedin_company_pages.livedocs_linkedin_company_pages
# WHERE
#   resource_id IN (
#     '401b810e-74ed-42d2-a391-f73d46412e8c',
#     '74c8fc6d-cddc-4a00-95b9-eca89fea5433'
#   )
# GROUP BY
#   metric_date
# ORDER BY
#   metric_date ASC
# """

# query2 = "select * from followers_by_geo"
      

# parse_bq_query(query2, {}, "ce6a3eab-b5af-4541-8545-8908baa33c40", client)













wsid = '3cdba016-cd57-45bc-a537-6720bf138d76'
auth_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiZ29vZ2xlLW9hdXRoMnwxMDA2NDkzNTc1MjQ4MzkyMDE1MDUiLCJ3b3Jrc3BhY2VfaWQiOiIzY2RiYTAxNi1jZDU3LTQ1YmMtYTUzNy02NzIwYmYxMzhkNzYiLCJyZXBvcnRfaWQiOiI2ODlmNjc2NC00ZjU5LTQ1Y2EtODA2Yi1jN2E1OWRhODYzYjAiLCJ1c2VyX2ltZyI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FDZzhvY0tNQ3JlaVlEbnpGZmp6WVJrb3dpZE5rV0hYT2FZak44UXZKZUxidzVFYzFMaGk2SnM9czk2LWMiLCJ1c2VyX25hbWUiOiJSYWFodWwgUHJlbSIsImlhdCI6MTcxODgxMjQzNCwiZXhwIjoxNzE4ODQxMjM0fQ.36pFgkw8L2cBPwHYgaaKbeWauJh-hpE-5Q_mHz16Hu4"
# response_data = get_workspace_connection_details(wsid=wsid, auth_token=auth_token, env="dev")


# Initialize the Lib class
lib = Lib( auth_token=auth_token, env="dev")

# print(lib.secrets_arr)
# print(lib.pg_creds)
# Example query and usage
# bq_query = '''SELECT * FROM ingestors.epl LIMIT 1000'''
# context = {}  # Define the context if needed
# bq_result = lib.parse_bq(bq_query, context)
# print(bq_result)

# pg_query = "SELECT * FROM users"
# db_name = "raahulprem"
# pg_result = lib.parse_pg(pg_query, db_name)
# print(pg_result)

# print("\n")
# print("----------------------------------")
# print("\n")

# # Assuming you have a DataFrame `df` and a query to run on it
# df_query = "SELECT * FROM df"
# df = pg_result  # Your Polars DataFrame
# df_result = lib.parse_df(df_query,"df", df)
# print(df_result)

# # Save data to BigQuery
# data_to_save = ...  # Your data to save, e.g., a Pandas or Polars DataFrame
# dataset_id = "your_dataset_id"
# table_id = "your_table_id"
# save_result = lib.save_to_bq(data_to_save, dataset_id, table_id, lib.bq_client)
# print(save_result)(worskpace_id=workspace_id, auth_token=auth_token, env=env)





















# load_dotenv()

# cred= {"db_name": {"host": "localhost",
#     "port": 5432,
#     "database": "New Connection",
#     "user": "raahulprem",
#     "password": "password"}}
# result = parse_pg_query( "SELECT * FROM ELEMENTS", "db_name", cred )


# print(result)

# df_res = run_dataframe_query('SELECT id, title from result' ,"result", result)
# print(df_res)


# app = Flask(__name__)
# CORS(app)


# @app.route('/highcharts', methods=['POST'])
# def highcharts():
#     data = request.get_json()

#     chart_generator = ChartGenerator().generate_highcharts_config(config=data, data=bq_result)
#     return jsonify(chart_generator)
 
# @app.route('/health')
# def health():
#     return "This is the health check."


# if __name__ == '__main__':
#     app.run(debug=True, port=7000)


 
