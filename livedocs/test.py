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
auth_token = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Im5wRy1MUi1nblE0NDZUcS1qajBxdSJ9.eyJnaXZlbl9uYW1lIjoiUmFhaHVsIiwiZmFtaWx5X25hbWUiOiJQcmVtIiwibmlja25hbWUiOiJyYWFodWwiLCJuYW1lIjoiUmFhaHVsIFByZW0iLCJwaWN0dXJlIjoiaHR0cHM6Ly9saDMuZ29vZ2xldXNlcmNvbnRlbnQuY29tL2EvQUNnOG9jS01DcmVpWURuekZmanpZUmtvd2lkTmtXSFhPYVlqTjhRdkplTGJ3NUVjMUxoaTZKcz1zOTYtYyIsInVwZGF0ZWRfYXQiOiIyMDI0LTA2LTE2VDEyOjA0OjU2LjEzMVoiLCJlbWFpbCI6InJhYWh1bEBsaXZlZG9jcy5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6Ly9saXZlZG9jcy1kZXYudXMuYXV0aDAuY29tLyIsImF1ZCI6IndOeGRieWwxS0pzeHZ5NEREQ3ZXaFhsYVFZUDJkY3ptIiwiaWF0IjoxNzE4NTM5NDk4LCJleHAiOjE3MTg3MTIyOTgsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MTAwNjQ5MzU3NTI0ODM5MjAxNTA1Iiwic2lkIjoiaHNhUl9GYUZlRUFmOFNIWW9SaEZLTkZMRTdCMDhCMEQifQ.bI5tH0GKz1qmICnT44u7uziGnYuTS_HyNFQ0dljyvYLWTTNASoBssdd0VFMdWhtGGaJ3zqeuh6E9uiKfLlPOOM-tX9ckDABgagjj6uFkNk31EG2-5ZEZgxC30mEQgiar1_JSFVc2QxSRkMVEHoUeOvxrptRPETI_tYWWjD7_9SZJN9JqkFeZD_4F1LeyvV2M_zIT6HfVArxoJkbSdQNhcf8MiZxlXCCHHxRlTSmbvC6OtTZPQOFVQ5lZ2pnLcedzqwjKjbBYw9ZMfO9wDQCVTvBSIaI5vzak-7qz7TQ4m98iPwE1QQWtGg4knyKQI1jpehNIBFlqOaw9j_1Y-cOWJA'
response_data = get_workspace_connection_details(wsid=wsid, auth_token=auth_token, env="dev")

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

# chart_meta =  {
#     "chart_meta": {
#         "axis_zooming": True,
#         "chart_type": "spline",
#         "x_axis_label": "",
#         "y_axis_label": "",
#         "axis_type": "logarithmic",
#         "pointers": True
#     },
#     "column_meta": {
#         "primaryAxis": {
#             "column_name": "",
#             "column_type": "",
#             "aggregate": "",
#             "color": "",
#             "order": "Ascending",
#             "filter_by": "",
#             "filter_type": "Show All",
#             "label": "",
#             "format": "",
#             "group_by": ""
#         },
#         "secondaryAxis": {
#             "column_name": "",
#             "column_type": "",
#             "aggregate": "average",
#             "color": "",
#             "order": "",
#             "filter_by": "",
#             "filter_type": "Show All",
#             "label": "",
#             "format": "",
#             "group_by": ""
#         }
#     }
# }



# @app.route('/highcharts', methods=['POST'])
# def highcharts():
#     data = request.get_json()

#     chart_generator = ChartGenerator().generate_highcharts_config(data=data)
#     return jsonify(chart_generator)
 
# @app.route('/health')
# def health():
#     return "This is the health check."


# if __name__ == '__main__':
#     app.run(debug=True, port=7000)


 
