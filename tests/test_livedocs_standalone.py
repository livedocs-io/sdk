import json
import os
import shutil
import tempfile
import unittest

import polars as pl

from livedocs import Livedocs, LivedocsConfig
from livedocs.utils.lib.cache import QueryCache
from livedocs.manager.credentials import StaticCredentialStore
from livedocs.types import CacheStatus, Credentials


class TestLivedocsStandalone(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        os.environ["LIVEDOCS_FILES_PATH"] = self.temp_dir

        self.credentials_bundle = Credentials(
            workspace_id="ws-id",
            workspace_secrets={},
            databases={},
            built_in_vars={},
        )

        self.config = LivedocsConfig(
            credential_store_factory=lambda *_: StaticCredentialStore(
                self.credentials_bundle
            ),
            query_cache_factory=lambda report_id, token: QueryCache(report_id, token),
        )

        self.livedocs = Livedocs(config=self.config)
        self.livedocs.initialize("report-id", "session-token")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _build_dataframe(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "id": [1, 2, 3],
                "value": [10.0, 20.5, 30.25],
                "label": ["a", "b", "c"],
            }
        )

    def test_dataframe_query_roundtrip_with_cache(self):
        dataframe = self._build_dataframe()
        datasource = {
            "source_type": "dataframe",
            "dataframe_info": {
                "df_name": "test_df",
                "df_element_id": "dummy",
            },
        }
        query = "SELECT * FROM test_df"

        df_result, payload = self.livedocs.query(
            query,
            json.dumps(datasource),
            context={},
            dataframe=dataframe,
            limit=10,
            offset=0,
            use_cache=True,
        )

        self.assertEqual(df_result.height, dataframe.height)
        cache_info = payload.result.metadata["cache_info"]
        self.assertEqual(cache_info["status"], CacheStatus.MISS)

        cached_df, cached_payload = self.livedocs.query(
            query,
            json.dumps(datasource),
            context={},
            dataframe=None,
            limit=10,
            offset=0,
            use_cache=True,
        )

        self.assertEqual(cached_df.height, dataframe.height)
        cached_info = cached_payload.result.metadata["cache_info"]
        self.assertEqual(cached_info["status"], CacheStatus.HIT)

    def test_add_jinja_vars_reuses_compiled_templates(self):
        template = "SELECT * FROM foo WHERE id = {{ value }}"

        rendered_1 = self.livedocs.add_jinja_vars(template, {"value": 1})
        self.assertIn("1", rendered_1)
        cache_info = self.livedocs._template_factory.cache_info()
        self.assertEqual(cache_info.misses, 1)

        rendered_2 = self.livedocs.add_jinja_vars(template, {"value": 2})
        self.assertIn("2", rendered_2)
        cache_info = self.livedocs._template_factory.cache_info()
        self.assertGreaterEqual(cache_info.hits, 1)


if __name__ == "__main__":
    unittest.main()
