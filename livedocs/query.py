from livedocs.types import DatabaseType, ElementDataSource, ElementDatasourceType
import pandas as pd

# Using the defined type:
element_data_source: ElementDataSource = {
    "__typename": "ElementDataSource",
    "databaseInfo": {
        "__typename": "DatabaseInfo",
        "database_connector_id": "conn123",
        "database_name": "mydb",
        "database_type": DatabaseType.Postgres,
    },
    "databaseTableInfo": None,
    "dataframeInfo": None,
    "fileInfo": None,
    "sourceType": ElementDatasourceType.database,
}


def query(query: str, datasource: str) -> pd.DataFrame:
    return "ok"
