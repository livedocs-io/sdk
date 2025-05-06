from typing import List

import duckdb


class DuckDBSingleton:
    _instance = None

    def __new__(cls, file_search_path: List[str] = None):
        if cls._instance is None:
            cls._instance = super(DuckDBSingleton, cls).__new__(cls)
            # Initialize connection
            cls._instance.conn = duckdb.connect(":memory:")

            # Set configuration options
            if file_search_path:
                cls._instance.conn.execute(
                    f"SET file_search_path = '{','.join(file_search_path)}';"
                )

            # cls._instance.conn.execute("SET enable_http_logging=true;")
            # cls._instance.conn.execute("SET enable_profiling='json';")
            # cls._instance.conn.execute("SET profiling_output='./profile.json';")
            # cls._instance.conn.execute("SET profiling_mode='detailed';")

            # Install and load extensions
            cls._instance.conn.install_extension("postgres")
            cls._instance.conn.load_extension("postgres")

            # cls._instance.conn.install_extension("spatial")
            # cls._instance.conn.load_extension("spatial")

            cls._instance.conn.install_extension("excel")
            cls._instance.conn.load_extension("excel")

            # Initialize tracking for attached databases
            cls._instance.postgres_connections = {}
        return cls._instance

    def attach_postgres(self, connection_string: str, alias: str):
        if alias not in self.postgres_connections:
            self.conn.execute(
                f"ATTACH '{connection_string}' AS {alias} (TYPE postgres)"
            )
            self.postgres_connections[alias] = connection_string

    def detach_postgres(self, alias: str):
        if alias in self.postgres_connections:
            self.conn.execute(f"DETACH {alias}")
            del self.postgres_connections[alias]
