import duckdb


class DuckDBSingleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DuckDBSingleton, cls).__new__(cls)
            cls._instance.conn = duckdb.connect(":memory:")

            # cls._instance.conn.execute("SET enable_http_logging=true;")
            # cls._instance.conn.execute("SET enable_profiling=JSON;")

            cls._instance.conn.install_extension("postgres")
            cls._instance.conn.load_extension("postgres")

            cls._instance.conn.install_extension("httpfs")
            cls._instance.conn.load_extension("httpfs")

            cls._instance.conn.install_extension("spatial")
            cls._instance.conn.load_extension("spatial")

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
