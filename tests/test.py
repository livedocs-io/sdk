# Setup the test file and environment

import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Setup a dummy prelude cell

from livedocs import Livedocs
from livedocs.types import DatabaseType, ElementDatasourceType

livedocs = Livedocs(
    "6b151f06-d0b6-4b97-acad-a010589a69ae",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoiZ29vZ2xlLW9hdXRoMnwxMDg5MDMzNDg1NzY5MDU4MTc5MTciLCJ3b3Jrc3BhY2VfaWQiOiJmZTlkZjhlOS1hZWNiLTQxOTAtOTBkOS00NDQ3OWJiMDU2NGIiLCJyZXBvcnRfaWQiOiI2YjE1MWYwNi1kMGI2LTRiOTctYWNhZC1hMDEwNTg5YTY5YWUiLCJ1c2VyX2ltZyI6Imh0dHBzOi8vbGgzLmdvb2dsZXVzZXJjb250ZW50LmNvbS9hL0FDZzhvY0xXSlVQa3M1bkoxdWg5cG1ia1pSNndSQlp6VnFCaDRzUDluMUhMZkhMcnQ0ZEJjekk9czk2LWMiLCJ1c2VyX25hbWUiOiJBcnNhbGFuIEJhc2hpciIsImlhdCI6MTcxOTY4ODg0NiwiZXhwIjoxNzE5NzE3NjQ2fQ.-U6M-VXpRzWmN5gNa8WjqFnJcxrsrrOl7i1NuUpP4PI",
)

# User code (i.e, the test)

pg_datasource = {
    "sourceType": ElementDatasourceType.database,
    "databaseInfo": {
        "database_connector_id": "conn123",
        "database_name": "mydb",
        "database_type": DatabaseType.Postgres,
    },
}


df = livedocs.query("select * from livedocs.livedocs.test_table limit 10", "df")
