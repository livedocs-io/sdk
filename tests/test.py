# Setup the test file and environment

import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Setup a dummy prelude cell

from livedocs import Livedocs
from livedocs.types import DatabaseType, ElementDatasourceType

livedocs = Livedocs(
    "156144d8-9e59-4815-b11a-fd8952b83369",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiZ29vZ2xlLW9hdXRoMnwxMDg5MDMzNDg1NzY5MDU4MTc5MTciLCJ3b3Jrc3BhY2VfaWQiOiIxYWFhMGJmZC01MWEzLTRlZDktOWYwMy1iNzVmMzZhMjdkNDEiLCJyZXBvcnRfaWQiOiIxNTYxNDRkOC05ZTU5LTQ4MTUtYjExYS1mZDg5NTJiODMzNjkiLCJ1c2VyX2ltZyI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FDZzhvY0xXSlVQa3M1bkoxdWg5cG1ia1pSNndSQlp6VnFCaDRzUDluMUhMZkhMcnQ0ZEJjekk9czk2LWMiLCJ1c2VyX25hbWUiOiJBcnNhbGFuIEJhc2hpciIsImlhdCI6MTcxOTc0Mjk3NiwiZXhwIjoxNzE5NzcxNzc2fQ.cvwxkg4oX8RsJnnxcVUIL01O-L5AUB_Sxr04T2iccLA",
)


# User code (i.e, the test)

pg_datasource = {
    "sourceType": ElementDatasourceType.database,
    "databaseInfo": {
        "database_connector_id": "22c5d054-eb49-415f-af1d-183b834f8fc1",
        "database_name": "appstore",
        "database_type": DatabaseType.Postgres,
    },
}


df = livedocs.query("select * from users limit 10", pg_datasource)
