# Setup the test file and environment

import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Setup a dummy prelude cell

from livedocs import Livedocs
from livedocs.types import ElementDatasourceType
import pandas as pd
import polars as pl
import numpy as np

livedocs = Livedocs()
livedocs.initialize("report_id", "token")

# User code (i.e, the test)

# Postgres datasource

# pg_datasource = {
#     "source_type": ElementDatasourceType.database,
#     "database_info": {
#         "database_connector_id": "22c5d054-eb49-415f-af1d-183b834f8fc1",
#         "database_name": "appstore",
#         "database_type": DatabaseType.Postgres,
#     },
# }

# pg_result = livedocs.query("select * from users limit 10", pg_datasource)

# print(pg_result)

# File datasources

csv_datasource = {
    "source_type": ElementDatasourceType.file,
    "file_info": {
        "file_id": "59158863-440b-4faf-ab25-456a0d748920",
        "file_name": "Random_Numbers_DataFrame.csv",
        "file_type": "csv",
        "file_has_layers": False,
    },
}

xlsx_datasource = {
    "source_type": ElementDatasourceType.file,
    "file_info": {
        "file_id": "c6f8d2ab-4516-4164-b8d3-e9f853188c44",
        "file_name": "Untitled spreadsheet.xlsx",
        "file_type": "xlsx",
        "file_has_layers": True,
        "layer_name": "Sheet1",
    },
}

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
    "source_type": ElementDatasourceType.dataframe,
    "dataframe_info": {
        "df_name": "df_polars",
        "df_element_id": "IRRELEVANT",
    },
}

pandas_datasource = {
    "source_type": ElementDatasourceType.dataframe,
    "dataframe_info": {
        "df_name": "df_pandas",
        "df_element_id": "IRRELEVANT",
    },
}


# polars_result = livedocs.query("select * from df_polars limit 10", polars_datasource)
# pandas_result = livedocs.query("select * from df_pandas limit 10", pandas_datasource)

# print(polars_result)
# print(pandas_result)
