import duckdb


class DuckDBSingleton:
    _instance = None

    def __new__(cls, file_search_path: list[str] | None = None):
        if cls._instance is None:
            cls._instance = super(DuckDBSingleton, cls).__new__(cls)
            # Initialize connection
            cls._instance.conn = duckdb.connect(":memory:")

            # Set configuration options
            if file_search_path:
                # Escape single quotes by doubling them to prevent SQL injection
                escaped_paths = [path.replace("'", "''") for path in file_search_path]
                _ = cls._instance.conn.execute(
                    f"SET file_search_path = '{','.join(escaped_paths)}';"
                )

            # cls._instance.conn.execute("SET enable_http_logging=true;")
            # cls._instance.conn.execute("SET enable_profiling='json';")
            # cls._instance.conn.execute("SET profiling_output='./profile.json';")
            # cls._instance.conn.execute("SET profiling_mode='detailed';")
            # cls._instance.conn.install_extension("spatial")
            # cls._instance.conn.load_extension("spatial")

            cls._instance.conn.install_extension("excel")
            cls._instance.conn.load_extension("excel")

        return cls._instance
