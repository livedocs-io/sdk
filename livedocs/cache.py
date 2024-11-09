import hashlib
import polars as pl
import concurrent.futures
import time
import logging
from IPython.display import display


class QueryCache:
    """
    A cache class for storing query results with a time-to-live (TTL) expiration mechanism.

    Attributes:
        ttl (int): Time-to-live for cache entries in seconds.
        cache (dict): Dictionary for storing cached data and timestamps.
        executor (concurrent.futures.ThreadPoolExecutor): Thread pool for asynchronous tasks.
    """

    def __init__(self, ttl: int = 3600, max_workers: int = 2):
        """
        Initializes the QueryCache instance with TTL and maximum worker threads.

        Args:
            ttl (int): Time-to-live for cached entries, in seconds. Defaults to 3600.
            max_workers (int): Maximum number of threads for parallel operations. Defaults to 4.
        """
        self.cache = {}
        self.ttl = ttl
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    def _generate_hash(self, query: str, datasource: str) -> str:
        """
        Generates a SHA-256 hash key based on the query and datasource.

        Args:
            query (str): The query string to be cached.
            datasource (str): The identifier for the data source.

        Returns:
            str: The generated hash key.
        """
        hash_input = query + datasource
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

    def get(self, query: str, datasource: str) -> tuple[pl.DataFrame, dict]:
        """
        Retrieves a cached entry for a given query and datasource if available and not expired.

        Args:
            query (str): The query string used as part of the cache key.
            datasource (str): The data source identifier used as part of the cache key.

        Returns:
            tuple[pl.DataFrame, dict] or None: Cached data and metadata if found and valid, otherwise None.
        """
        key = self._generate_hash(query, datasource)
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
        key = self._generate_hash(query, datasource)
        self.cache[key] = {"data": result, "timestamp": time.time()}
        # Optionally submit the data to be written to a Parquet file asynchronously.
        # self.executor.submit(self._write_to_parquet, key, result[0])
        logging.info(f"Successfully cached key: {key}")
        display(f"Successfully cached key: {key}")

    def _write_to_parquet(self, key: str, df: pl.DataFrame):
        """
        Writes a DataFrame to a Parquet file using the generated cache key as filename.

        Args:
            key (str): The cache key used to name the Parquet file.
            df (pl.DataFrame): The DataFrame to be written to Parquet.
        """
        try:
            df.write_parquet(f"{key}.parquet")
            logging.info(f"Successfully wrote Parquet file for key: {key}")
        except Exception as e:
            logging.error(f"Failed to write Parquet file for key: {key}, error: {e}")
