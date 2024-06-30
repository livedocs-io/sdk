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

livedocs = Livedocs(
    "156144d8-9e59-4815-b11a-fd8952b83369",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiZ29vZ2xlLW9hdXRoMnwxMDg5MDMzNDg1NzY5MDU4MTc5MTciLCJ3b3Jrc3BhY2VfaWQiOiIxYWFhMGJmZC01MWEzLTRlZDktOWYwMy1iNzVmMzZhMjdkNDEiLCJyZXBvcnRfaWQiOiIxNTYxNDRkOC05ZTU5LTQ4MTUtYjExYS1mZDg5NTJiODMzNjkiLCJ1c2VyX2ltZyI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FDZzhvY0xXSlVQa3M1bkoxdWg5cG1ia1pSNndSQlp6VnFCaDRzUDluMUhMZkhMcnQ0ZEJjekk9czk2LWMiLCJ1c2VyX25hbWUiOiJBcnNhbGFuIEJhc2hpciIsImlhdCI6MTcxOTc0Mjk3NiwiZXhwIjoxNzE5NzcxNzc2fQ.cvwxkg4oX8RsJnnxcVUIL01O-L5AUB_Sxr04T2iccLA",
)

# User code (i.e, the test)

# Postgres datasource

pg_datasource = {
    "sourceType": ElementDatasourceType.database,
    "databaseInfo": {
        "database_connector_id": "22c5d054-eb49-415f-af1d-183b834f8fc1",
        "database_name": "appstore",
        "database_type": DatabaseType.Postgres,
    },
}

pg_result = livedocs.query("select * from users limit 10", pg_datasource)

print(pg_result)

# File datasources

csv_datasource = {
    "sourceType": ElementDatasourceType.file,
    "fileInfo": {
        "file_id": "59158863-440b-4faf-ab25-456a0d748920",
        "file_name": "Random_Numbers_DataFrame.csv",
        "file_type": "csv",
        "file_has_layers": False,
    },
}

xlsx_datasource = {
    "sourceType": ElementDatasourceType.file,
    "fileInfo": {
        "file_id": "c6f8d2ab-4516-4164-b8d3-e9f853188c44",
        "file_name": "Untitled spreadsheet.xlsx",
        "file_type": "xlsx",
        "file_has_layers": True,
        "layer_name": "Sheet1",
    },
}

# xls_datasource = {
#     "sourceType": ElementDatasourceType.file,
#     "fileInfo": {
#         "file_id": "fe408b07-42e6-4c6a-bedf-24a5b39ddec0",
#         "file_name": "xls.xls",
#         "file_type": "xls",
#         "file_has_layers": True,
#         "layer_name": "Sheet1",
#     },
# }

csv_result = livedocs.query(
    "select * from Random_Numbers_DataFrame.csv limit 10", csv_datasource
)
xlsx_result = livedocs.query(
    "select * from Untitled spreadsheet.xlsx limit 10", xlsx_datasource
)
# xls_result = livedocs.query("select * from xls.xls limit 10", xls_datasource)

print(csv_result)
print(xlsx_result)
# print(xls_result)


# Dataframe datasources

# Create a 20x20 array of random numbers
data_df = np.random.rand(20, 20)

# Create headers
headers = [f"col{i}" for i in range(20)]

df_polars = pl.DataFrame(data_df, schema=headers)
df_pandas = pd.DataFrame(data_df, columns=headers)

polars_datasource = {
    "sourceType": ElementDatasourceType.dataframe,
    "dataframeInfo": {
        "df_name": "df_polars",
        "df_element_id": "IRRELEVANT",
    },
}

pandas_datasource = {
    "sourceType": ElementDatasourceType.dataframe,
    "dataframeInfo": {
        "df_name": "df_pandas",
        "df_element_id": "IRRELEVANT",
    },
}


polars_result = livedocs.query("select * from df_polars limit 10", polars_datasource)
pandas_result = livedocs.query("select * from df_pandas limit 10", pandas_datasource)

print(polars_result)
print(pandas_result)
