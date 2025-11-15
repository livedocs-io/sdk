import concurrent.futures
import hashlib
import io
import json
import logging
import threading
import time

import polars as pl
import requests

from livedocs.types import ElementDataSource, GCSBucketType
from livedocs.utils.common import _fetch_file_manifest


class QueryCache:
    """
    A cache class for storing query results with a time-to-live (TTL) expiration mechanism.
    """

    def __init__(
        self, report_id: str, token: str, ttl: int = 3600, max_workers: int = 2
    ):
        """
        Initializes the QueryCache instance with TTL and maximum worker threads.

        Args:
            ttl (int): Time-to-live for cached entries, in seconds. Defaults to 3600.
            max_workers (int): Maximum number of threads for parallel operations. Defaults to 2.
        """
        self.cache = {}
        self.ttl: int = ttl
        self.executor: concurrent.futures.ThreadPoolExecutor = (
            concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        )
        self.lock: threading.Lock = threading.Lock()
        self.report_id: str = report_id
        self.token: str = token

    def generate_cache_id(self, query: str, datasource: ElementDataSource) -> str:
        """
        Generates a SHA-256 hash key based on the query and datasource.

        Args:
            query (str): The query string to be cached.
            datasource (ElementDataSource): The query data source.

        Returns:
            str: The generated hash key.
        """
        try:
            datasource_json = json.dumps(
                datasource,
                separators=(",", ":"),
            )
        except TypeError as e:
            logging.error(f"Failed to generate cache_id: {e}")
            return query

        hash_input = query + datasource_json
        return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()

    def _is_expired(self, timestamp: float) -> bool:
        """
        Checks if a cache entry is expired based on its timestamp.

        Args:
            timestamp (float): The timestamp of the cache entry.

        Returns:
            bool: True if the entry is expired, False otherwise.
        """
        return time.time() - timestamp > self.ttl

    def get(
        self, query: str, datasource: ElementDataSource
    ) -> tuple[pl.DataFrame, dict] | None:
        """
        Retrieves a cached entry for a given query and datasource if available and not expired.

        Args:
            query (str): The query string used as part of the cache key.
            datasource (ElementDataSource): The data source identifier used as part of the cache key.

        Returns:
            tuple[pl.DataFrame, dict] or None: Cached data and metadata if found and valid, otherwise None.
        """
        key = self.generate_cache_id(query, datasource)
        entry = self.cache.get(key)
        if entry and not self._is_expired(entry["timestamp"]):
            return entry["data"]
        elif entry:
            del self.cache[key]
        return None

    def set(self, query: str, datasource: str, result: tuple[pl.DataFrame, dict]):
        """
        Caches the result of a query for a given query string and datasource.

        Args:
            query (str): The query string used to generate the cache key.
            datasource (str): The data source identifier used to generate the cache key.
            result (tuple[pl.DataFrame, dict]): The result data and metadata to cache.
        """
        key = self.generate_cache_id(query, datasource)
        self.cache[key] = {"data": result, "timestamp": time.time()}

    def bust(self):
        """
        Clears the cache by removing all entries.
        """
        self.cache.clear()

    def pop(self, key: str) -> bool:
        """
        Removes a cache entry by key if it exists.

        Args:
            key (str): The cache key to remove

        Returns:
            bool: True if the key was found and removed, False otherwise
        """
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                return True
            return False

    def _write_to_parquet(self, key: str, df: pl.DataFrame):
        """
        Writes a DataFrame to GCS as Parquet file.

        Args:
            key (str): The cache key used as file name
            df (pl.DataFrame): The DataFrame to cache
        """

        with self.lock:
            # Create buffer to store the data in memory
            buffer = io.BytesIO()

            # Write DataFrame to the buffer in Parquet format
            df.write_parquet(buffer)

            # Retrieve the byte data from the buffer
            parquet_bytes = buffer.getvalue()

            if len(parquet_bytes) > 32 * 1024 * 1024:
                raise ValueError("Parquet file exceeds 32MB limit")

            # Get signed URL for upload
            upload_url = _fetch_file_manifest(
                self.report_id,
                self.token,
                "write",
                GCSBucketType.CACHE_ARTIFACTS,
                file_id=f"{key}.parquet",
            )["signed_url"]

            # Upload the Parquet file to GCS
            response = requests.put(
                upload_url,
                data=parquet_bytes,
                headers={"Content-Type": "application/octet-stream"},
            )
            response.raise_for_status()

    def upload_artifacts(self):
        """
        Uploads the cached artifacts to GCS.
        """
        for key in self.cache.keys():
            # Make a cheap copy of the DataFrame to avoid concurrency issues
            df_copy = self.cache.get(key)["data"][0].clone()
            self.executor.submit(self._write_to_parquet, key, df_copy)
