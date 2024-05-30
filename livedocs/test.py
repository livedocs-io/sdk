# from bq import evaluate_jinja_expression, split_and_replace_query, run_bigquery_query,create_bigquery_client
from pg import parse_pg_query
from df import run_dataframe_query
from dotenv import load_dotenv
import os

load_dotenv()

cred= {"db_name": {"host": "localhost",
    "port": 5432,
    "database": "New Connection",
    "user": "raahulprem",
    "password": "password"}}
result = parse_pg_query( "SELECT * FROM ELEMENTS", "db_name", cred )


print(result)

df_res = run_dataframe_query('SELECT * from result' ,"result", result)
print(df_res)