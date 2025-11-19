import json
import os
import shutil
import tempfile
import unittest
import uuid

import polars as pl
import psycopg
from pydantic import SecretStr

from livedocs import Livedocs, LivedocsConfig
from livedocs.utils.lib.cache import QueryCache
from livedocs.manager.credentials import StaticCredentialStore
from livedocs.types import Credentials, DatabaseConnection

LIVEDOCS_TEST_POSTGRES_URL_ENV = ""


@unittest.skipUnless(
    os.getenv(LIVEDOCS_TEST_POSTGRES_URL_ENV),
    f"Set {LIVEDOCS_TEST_POSTGRES_URL_ENV} to run PostgreSQL integration tests.",
)
class TestLivedocsPostgresIntegration(unittest.TestCase):
    def setUp(self):
        self.connection_url = os.environ[LIVEDOCS_TEST_POSTGRES_URL_ENV]

        self._prev_files_path = os.getenv("LIVEDOCS_FILES_PATH")
        self.temp_dir = tempfile.mkdtemp()
        os.environ["LIVEDOCS_FILES_PATH"] = self.temp_dir

        self._prev_run_context = os.getenv("LIVEDOCS_RUN_CONTEXT")
        os.environ["LIVEDOCS_RUN_CONTEXT"] = "logic"

        os.environ.pop("LIVEDOCS_PY_SDK_SENTRY_DSN", None)

        self.connector_id = f"pg-{uuid.uuid4().hex[:8]}"

        connection_details = SecretStr(
            json.dumps(
                {
                    "connect_using": "url",
                    "connection_url": self.connection_url,
                }
            )
        )

        db_connection = DatabaseConnection(
            db_connector_id=self.connector_id,
            db_name="integration",
            connection_details=connection_details,
        )

        credentials_bundle = Credentials(
            workspace_id="ws",
            workspace_secrets={},
            databases={self.connector_id: db_connection},
            built_in_vars={},
        )

        config = LivedocsConfig(
            credential_store_factory=lambda *_: StaticCredentialStore(
                credentials_bundle
            ),
            query_cache_factory=lambda report_id, token: QueryCache(report_id, token),
        )

        self.livedocs = Livedocs(config=config)
        self.livedocs.initialize("report-id", "token")

        self.datasource = {
            "source_type": "database",
            "database_info": {
                "database_connector_id": self.connector_id,
                "database_name": "integration",
                "database_type": "postgres",
            },
        }

        self.schema = "public"
        self.read_table = f"vm_read_{uuid.uuid4().hex[:8]}"
        self.write_table = f"vm_write_{uuid.uuid4().hex[:8]}"

        self._prepare_table(self.read_table)
        self._prepare_table(self.write_table)

    def tearDown(self):
        for table_name in [self.read_table, self.write_table]:
            try:
                self._execute_sql(f"DROP TABLE IF EXISTS {self.schema}.{table_name}")
            except Exception:
                pass

        if self._prev_files_path is None:
            os.environ.pop("LIVEDOCS_FILES_PATH", None)
        else:
            os.environ["LIVEDOCS_FILES_PATH"] = self._prev_files_path

        if self._prev_run_context is None:
            os.environ.pop("RUN_CONTEXT", None)
        else:
            os.environ["RUN_CONTEXT"] = self._prev_run_context

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _execute_sql(self, statement: str, params=None):
        with psycopg.connect(self.connection_url) as conn:
            with conn.cursor() as cursor:
                cursor.execute(statement, params or ())
                if cursor.description:
                    return cursor.fetchall()
        return None

    def _prepare_table(self, table_name: str):
        self._execute_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} (
                id INTEGER PRIMARY KEY,
                label TEXT,
                amount INTEGER
            )
            """
        )
        self._execute_sql(f"TRUNCATE {self.schema}.{table_name}")

    def test_query_postgres_reads_data(self):
        rows = [
            (1, "alpha", 10),
            (2, "beta", 20),
            (3, "gamma", 30),
        ]
        insert_sql = f"INSERT INTO {self.schema}.{self.read_table} (id, label, amount) VALUES (%s, %s, %s)"
        with psycopg.connect(self.connection_url) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_sql, rows)

        query = (
            f"SELECT id, label, amount FROM {self.schema}.{self.read_table} ORDER BY id"
        )
        df, payload = self.livedocs.query(
            query,
            json.dumps(self.datasource),
            context={},
            limit=10,
            offset=0,
            use_cache=False,
        )

        self.assertEqual(df.height, len(rows))
        self.assertListEqual(df["label"].to_list(), [row[1] for row in rows])
        self.assertEqual(payload.result.metadata["total_rows"], len(rows))

    def test_save_to_postgres_overwrites_table(self):
        seed_rows = [
            (10, "seed", 0),
        ]
        seed_insert = f"INSERT INTO {self.schema}.{self.write_table} (id, label, amount) VALUES (%s, %s, %s)"
        with psycopg.connect(self.connection_url) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(seed_insert, seed_rows)

        df_to_write = pl.DataFrame(
            {
                "id": [101, 102],
                "label": ["delta", "epsilon"],
                "amount": [400, 500],
            }
        )

        save_config = json.dumps(
            {
                "dataframe_name": "df",
                "dataframe_element_id": "df-element",
                "database_name": "integration",
                "database_id": self.connector_id,
                "database_type": "postgres",
                "schema_name": self.schema,
                "table_name": self.write_table,
                "table_is_new": False,
                "write_mode": "overwrite",
                "run_settings": ["edit_mode"],
            }
        )

        result = self.livedocs.save_to_database(df_to_write, save_config)
        self.assertIsNotNone(result)

        stored_rows = self._execute_sql(
            f"SELECT id, label, amount FROM {self.schema}.{self.write_table} ORDER BY id"
        )

        self.assertEqual(
            stored_rows,
            [(101, "delta", 400), (102, "epsilon", 500)],
        )


if __name__ == "__main__":
    unittest.main()
