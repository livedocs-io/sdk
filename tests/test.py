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
    "1f836c4b-442f-4144-a02f-5b70a4a78581",
    "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiZ29vZ2xlLW9hdXRoMnwxMDg5MDMzNDg1NzY5MDU4MTc5MTciLCJ3b3Jrc3BhY2VfaWQiOiIxZDYzNTYyYy0wNjAyLTQyYTktYjZjNy0yMjliMTU0YzlmNDMiLCJyZXBvcnRfaWQiOiIxZjgzNmM0Yi00NDJmLTQxNDQtYTAyZi01YjcwYTRhNzg1ODEiLCJ1c2VyX2ltZyI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FDZzhvY0xXSlVQa3M1bkoxdWg5cG1ia1pSNndSQlp6VnFCaDRzUDluMUhMZkhMcnQ0ZEJjekk9czk2LWMiLCJ1c2VyX25hbWUiOiJBcnNhbGFuIEJhc2hpciIsImV4cCI6MTczNjQ4NDg0NH0.42Z_GI4S_wfU4YnX-MYrKHE3CnJT7xqtlBHzgIkmVso"
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

# pg_result = livedocs.query("select * from Appstore.public.users limit 10", json.dumps(pg_datasource), {})

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

polars_datasource = {
    "source_type": "dataframe",
    "dataframe_info": {
        "df_name": "df_polars",
        "df_element_id": "IRRELEVANT",
    },
}

pandas_datasource = {
    "source_type": "dataframe",
    "dataframe_info": {
        "df_name": "df_pandas",
        "df_element_id": "IRRELEVANT",
    },
}

# Create a 20x20 array of random numbers
data_df = np.random.rand(20, 20)

# Create headers
headers = [f"col{i}" for i in range(20)]

df_polars = pl.DataFrame(data_df, schema=headers)
df_pandas = pd.DataFrame(data_df, columns=headers)

df = pl.DataFrame(
    data=[
        {
            "id": "f68ad0ab-1f0c-4ad4-98da-3fc2eafdb9ce",
            "email": "test2@example.com",
            "workspace_id": "1d63562c-0602-42a9-b6c7-229b154c9f43",
            "ws_permission_level": "admin",
            "deleted_at": None
        }
    ],
    schema={
        "id": pl.Utf8,
        "email": pl.Utf8,
        "workspace_id": pl.Utf8,
        "ws_permission_level": pl.Utf8,
        "updated_at": pl.Datetime,
        "created_at": pl.Datetime,
        "deleted_at": pl.Datetime
    }
)

save_config = {
  "dataframe_name": "df",
  "dataframe_element_id": "IRRELEVANT",
  "database_name": "appstoreb",
  "database_id": "d97d43ef-66c4-477c-9b27-a1211002aea9",
  "database_type": "postgres",
  "schema_name": "public",
  "table_name": "invites",
  "table_is_new": False,
  "write_mode": "overwrite",
  "run_settings": [
    "edit_mode",
    "view_mode",
    "scheduled_runs",
    "webhook_runs"
  ]
}

_new_save_config = {
  "dataframe_name": "df",
  "dataframe_element_id": "IRRELEVANT",
  "database_name": "appstoreb",
  "database_id": "d97d43ef-66c4-477c-9b27-a1211002aea9",
  "database_type": "postgres",
  "schema_name": "public",
  "table_name": "newtable",
  "table_is_new": True,
  "write_mode": "append",
  "run_settings": [
    "edit_mode",
    "view_mode",
    "scheduled_runs",
    "webhook_runs"
  ]
}

str_save_config = json.dumps(save_config)
str_save_config_new = json.dumps(_new_save_config)

livedocs.save_to_database(df, str_save_config)
livedocs.save_to_database(df, str_save_config_new)

print("done")
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