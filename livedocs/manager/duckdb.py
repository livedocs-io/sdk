import duckdb

_conn: duckdb.DuckDBPyConnection | None = None


def get_duckdb_connection(
    file_search_path: list[str] | None = None,
) -> duckdb.DuckDBPyConnection:
    """
    Get or create the global DuckDB connection.

    Args:
        file_search_path: List of file paths to set for DuckDB file search.
                         Only used on first call; ignored on subsequent calls.

    Returns:
        The shared DuckDB connection instance.
    """
    global _conn
    if _conn is None:
        _conn = duckdb.connect(":memory:")

        # Set configuration options
        if file_search_path:
            # Escape single quotes by doubling them to prevent SQL injection
            escaped_paths = [path.replace("'", "''") for path in file_search_path]
            _ = _conn.execute(f"SET file_search_path = '{','.join(escaped_paths)}';")

        _conn.install_extension("excel")
        _conn.load_extension("excel")
        # _conn.execute("SET enable_http_logging=true;")
        # _conn.execute("SET enable_profiling='json';")
        # _conn.execute("SET profiling_output='./profile.json';")
        # _conn.execute("SET profiling_mode='detailed';")
        # _conn.install_extension("spatial")
        # _conn.load_extension("spatial")

    return _conn
