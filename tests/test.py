# Setup the test file and environment

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
    "219141ef-b707-4d24-ac0f-f6fd52aeca34",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiZ29vZ2xlLW9hdXRoMnwxMDg5MDMzNDg1NzY5MDU4MTc5MTciLCJ3b3Jrc3BhY2VfaWQiOiJiZTZmMjJlNi01NDJiLTQ2MzgtYWEzZC1mMWQxYmVmODJmNzYiLCJyZXBvcnRfaWQiOiIyMTkxNDFlZi1iNzA3LTRkMjQtYWMwZi1mNmZkNTJhZWNhMzQiLCJ1c2VyX2ltZyI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FDZzhvY0xXSlVQa3M1bkoxdWg5cG1ia1pSNndSQlp6VnFCaDRzUDluMUhMZkhMcnQ0ZEJjekk9czk2LWMiLCJ1c2VyX25hbWUiOiJBcnNhbGFuIEJhc2hpciIsImlhdCI6MTcyNTMwMzg3NywiZXhwIjoxNzI1MzMyNjc3fQ.1cc-TZ_UU74ts8F7TF5TZo7XdyYwpNp-emw1UjgUNbo"
)

# User code (i.e, the test)

# Postgres datasource

pg_datasource = {
    "source_type": "database",
    "database_info": {
        "database_connector_id": "7ff3aee7-59c5-4934-89c1-4a77715774d3",
        "database_name": "appstore",
        "database_type": "Postgres",
    },
}

pg_result = livedocs.query("select * from Appstore.public.users limit 10", json.dumps(pg_datasource), {})

print(pg_result)

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

# Create a 20x20 array of random numbers
# data_df = np.random.rand(20, 20)

# Create headers
# headers = [f"col{i}" for i in range(20)]

# df_polars = pl.DataFrame(data_df, schema=headers)
# df_pandas = pd.DataFrame(data_df, columns=headers)

# polars_datasource = {
#     "source_type": "dataframe",
#     "dataframe_info": {
#         "df_name": "df_polars",
#         "df_element_id": "IRRELEVANT",
#     },
# }

# pandas_datasource = {
#     "source_type": ElementDatasourceType.dataframe,
#     "dataframe_info": {
#         "df_name": "df_pandas",
#         "df_element_id": "IRRELEVANT",
#     },
# }


# polars_result = livedocs.query("select * from df_polars limit 10", json.dumps(polars_datasource), {})
# pandas_result = livedocs.query("select * from df_pandas limit 10", pandas_datasource)

# print(polars_result)
# print(pandas_result)

# print(livedocs.secrets('CLIENT_ID', 'not the actual value'))
# print(livedocs.secrets('UNKNOWN', 'not the actual value'))
# print(livedocs.secrets('UNKNOWN'))