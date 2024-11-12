import concurrent.futures
import hashlib
import io
import logging
import threading
import time

import polars as pl
import requests
import json
from IPython.display import display

from livedocs.utils.common import _fetch_file_manifest
from livedocs.types import ElementDataSource


class QueryCache:
    """
    A cache class for storing query results with a time-to-live (TTL) expiration mechanism.

    Attributes:
        ttl (int): Time-to-live for cache entries in seconds.
        cache (dict): Dictionary for storing cached data and timestamps.
        executor (concurrent.futures.ThreadPoolExecutor): Thread pool for asynchronous tasks.
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
        self.ttl = ttl
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.lock = threading.Lock()
        self.report_id = report_id
        self.token = token

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
    ) -> tuple[pl.DataFrame, dict]:
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
            logging.info(f"Cache hit for key: {key}")
            display(f"Cache entry for key: {key} is {entry}")
            return entry["data"]
        elif entry:
            logging.info(f"Cache expired for key: {key}")
            display("CACHE EXPIRED")
            del self.cache[key]
        logging.info(f"Cache miss for key: {key}")
        display("CACHE MISS")
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
        # Make a copy of the DataFrame to avoid concurrent access issues
        df_copy = result[0].clone()
        # Asynchronously write the DataFrame to a Parquet file
        self.executor.submit(self._write_to_parquet, key, df_copy)
        logging.info(f"Successfully cached key: {key}")
        display(f"Successfully cached key: {key}")

    def _write_to_parquet(self, key: str, df: pl.DataFrame):
        """
        Writes a DataFrame to GCS as parquet using streaming.

        Args:
            key (str): The cache key used as filename
            df (pl.DataFrame): The DataFrame to upload
        """
        try:
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
                    f"cache/{key}.parquet", self.report_id, self.token, "write"
                )["signed_url"]

                # Upload the Parquet file to GCS
                response = requests.put(
                    upload_url,
                    data=parquet_bytes,
                    headers={"Content-Type": "application/octet-stream"},
                )
                response.raise_for_status()

                display(f"Uploaded parquet to GCS for key: {key}")
                logging.info(f"Uploaded parquet to GCS for key: {key}")

        except Exception as e:
            display(f"Failed to upload parquet for key: {key}, error: {e}")
            logging.error(f"Failed to upload parquet for key: {key}, error: {e}")
