# Setup the test file and environment

from datetime import datetime
import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Setup a dummy prelude cell

from livedocs import Livedocs
from livedocs.types import DatabaseType, ElementDatasourceType
import pandas as pd
import polars as pl
import numpy as np
import json

livedocs = Livedocs()
livedocs.initialize(
    "cea74395-5d3f-42cf-97bf-20e471b93834",
    "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiZ29vZ2xlLW9hdXRoMnwxMDg5MDMzNDg1NzY5MDU4MTc5MTciLCJ3b3Jrc3BhY2VfaWQiOiJhZjA2YWYxMi0zY2Y5LTRkYzMtODNmOC1jNmU1M2MzNTI1ZjAiLCJ1c2VyX2ltZyI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FDZzhvY0xXSlVQa3M1bkoxdWg5cG1ia1pSNndSQlp6VnFCaDRzUDluMUhMZkhMcnQ0ZEJjekk9czk2LWMiLCJ1c2VyX25hbWUiOiJBcnNhbGFuIEJhc2hpciIsInJlcG9ydF9pZCI6ImNlYTc0Mzk1LTVkM2YtNDJjZi05N2JmLTIwZTQ3MWI5MzgzNCIsInJlcG9ydF9wZXJtaXNzaW9uX2xldmVsIjozLCJ3c19wZXJtaXNzaW9uX2xldmVsIjozLCJtZW1iZXJfaWQiOiIzZjU4YmY0Zi03MmMzLTRiZmQtOWEzOS05YjdkZTY0MWI3ZjMiLCJleHAiOjE3NDgwMDE2NTl9.rhDenfupBzIubUepaMtydto7_Ulfcooq0uXEYr1FZQw"
)
# User code (i.e, the test)

# Postgres datasource

# pg_datasource = {
#     "source_type": "database",
#     "database_info": {
#         "database_connector_id": "7ff3aee7-59c5-4934-89c1-4a77715774d3",
#         "database_name": "appstore",
#         "database_type": "Postgres",
#     },
# }


snowflake_datasource = {
    "source_type": "database_table",
    "database_info": {
        "database_connector_id": "426339e1-4bd5-42f8-9fb2-b68fbeb466b0",
        "database_name": "snowflake",
        "database_type": "snowflake",
    },
}

snowflake_table_source = {
    "source_type": "database_table",
    "database_info": {
        "database_type": "snowflake",
        "database_connector_id": "426339e1-4bd5-42f8-9fb2-b68fbeb466b0",
        "database_name": "snowflake"
    },
    "database_table_info": {
        "instance_id": "426339e1-4bd5-42f8-9fb2-b68fbeb466b0",
        "schema_name": "ARSLNB_LOAD_SAMPLE_DATA_FROM_S3",
        "table_name": "MENU"
    }
}

# pg_result = livedocs.query("select * from Appstore.public.users limit 10", json.dumps(pg_datasource), {})

# snowflake_result = livedocs.query("SELECT * FROM snowflake_learning_db.arslnb_load_sample_data_from_s3.menu LIMIT 1", json.dumps(snowflake_datasource), {})


snowflake_table = livedocs._get_table_response(json.dumps(snowflake_table_source), limit=10)
print(snowflake_table)
# print(pg_result)

# File datasources

# csv_datasource = {
#     "source_type": ElementDatasourceType.file,
#     "file_info": {
#         "file_id": "e6699ed1-3b74-4ddc-b836-802fff4404df",
#         "file_name": "data.csv",
#         "file_type": "csv",
#         "file_has_layers": False,
#     },
# }

# xlsx_datasource = {
#     "source_type": ElementDatasourceType.file,
#     "file_info": {
#         "file_id": "c6f8d2ab-4516-4164-b8d3-e9f853188c44",
#         "file_name": "Untitled spreadsheet.xlsx",
#         "file_type": "xlsx",
#         "file_has_layers": True,
#         "layer_name": "Sheet1",
#     },
# }

# xls_datasource = {
#     "source_type": ElementDatasourceType.file,
#     "file_info": {
#         "file_id": "fe408b07-42e6-4c6a-bedf-24a5b39ddec0",
#         "file_name": "xls.xls",
#         "file_type": "xls",
#         "file_has_layers": True,
#         "layer_name": "Sheet1",
#     },
# }

# csv_result = livedocs.query("select * from data.csv limit 100", csv_datasource)
# xlsx_result = livedocs.query(
#     "select * from Untitled spreadsheet.xlsx limit 10", xlsx_datasource
# )
# xls_result = livedocs.query("select * from xls.xls limit 10", xls_datasource)

# print(csv_result)
# print(xlsx_result)
# print(xls_result)


# Dataframe datasources

# polars_datasource = {
#     "source_type": "dataframe",
#     "dataframe_info": {
#         "df_name": "df_polars",
#         "df_element_id": "IRRELEVANT",
#     },
# }

# pandas_datasource = {
#     "source_type": "dataframe",
#     "dataframe_info": {
#         "df_name": "df_pandas",
#         "df_element_id": "IRRELEVANT",
#     },
# }

# # Create a 20x20 array of random numbers
# data_df = np.random.rand(20, 20)

# # Create headers
# headers = [f"col{i}" for i in range(20)]

# df_polars = pl.DataFrame(data_df, schema=headers)
# df_pandas = pd.DataFrame(data_df, columns=headers)

# df = pl.DataFrame(
#     data=[
#         {
#             "id": "f68ad0ab-1f0c-4ad4-98da-3fc2eafdb9ce",
#             "email": "test2@example.com",
#             "workspace_id": "1d63562c-0602-42a9-b6c7-229b154c9f43",
#             "ws_permission_level": "admin",
#             "deleted_at": None
#         }
#     ],
#     schema={
#         "id": pl.Utf8,
#         "email": pl.Utf8,
#         "workspace_id": pl.Utf8,
#         "ws_permission_level": pl.Utf8,
#         "updated_at": pl.Datetime,
#         "created_at": pl.Datetime,
#         "deleted_at": pl.Datetime
#     }
# )

# save_config = {
#   "dataframe_name": "df",
#   "dataframe_element_id": "IRRELEVANT",
#   "database_name": "appstoreb",
#   "database_id": "d97d43ef-66c4-477c-9b27-a1211002aea9",
#   "database_type": "postgres",
#   "schema_name": "public",
#   "table_name": "invites",
#   "table_is_new": False,
#   "write_mode": "overwrite",
#   "run_settings": [
#     "edit_mode",
#     "view_mode",
#     "scheduled_runs",
#     "webhook_runs"
#   ]
# }

# _new_save_config = {
#   "dataframe_name": "df",
#   "dataframe_element_id": "IRRELEVANT",
#   "database_name": "appstoreb",
#   "database_id": "d97d43ef-66c4-477c-9b27-a1211002aea9",
#   "database_type": "postgres",
#   "schema_name": "public",
#   "table_name": "newtable",
#   "table_is_new": True,
#   "write_mode": "append",
#   "run_settings": [
#     "edit_mode",
#     "view_mode",
#     "scheduled_runs",
#     "webhook_runs"
#   ]
# }

# str_save_config = json.dumps(save_config)
# str_save_config_new = json.dumps(_new_save_config)

# x = livedocs.save_to_database(df, str_save_config)
# a = livedocs.save_to_database(df, str_save_config_new)

# print(x)
# print(a)
# livedocs._get_dataframe_schema(df_polars)
# livedocs._get_dataframe_schema(df_pandas)


# polars_result = livedocs.query("select * from df_polars limit 10", json.dumps(polars_datasource), {}, df_polars)
# pandas_result = livedocs.query("select * from df_pandas limit 10", json.dumps(pandas_datasource), {}, df_pandas)


# print(pandas_result)
# print(polars_result)
# print(pandas_result)

# print(livedocs.secrets('CLIENT_ID', 'not the actual value'))
# print(livedocs.secrets('UNKNOWN', 'not the actual value'))
# print(livedocs.secrets('UNKNOWN'))