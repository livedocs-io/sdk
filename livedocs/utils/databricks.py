from __future__ import annotations

import traceback
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Iterable, Sequence

import polars as pl
from typing_extensions import Literal


def process_databricks_schema(
    schema: Sequence[tuple[str, str]] | None,
) -> dict[str, str]:
    """
    Processes Databricks schema information returned from the query endpoint and
    normalizes it to the Livedocs schema format.

    Parameters
    ----------
    schema:
        Sequence of `(column_name, type_text)` tuples (may be `None` when the query
        returned no manifest information).

    Returns
    -------
    dict[str, str]
        Mapping of column name to one of the Livedocs logical types:
        `NUMBER`, `DATE`, or `STRING`.
    """
    if not schema:
        return {}

    processed_schema: dict[str, str] = {}
    for column_name, type_hint in schema:
        normalized_type = _normalize_databricks_type(type_hint)
        processed_schema[column_name] = normalized_type

    return processed_schema


def write_df_to_databricks(
    df: pl.DataFrame,
    statement_runner,
    table_identifier: str,
    *,
    create_table: bool = False,
    write_mode: Literal["append", "overwrite"] = "append",
    chunk_size: int = 100,
) -> dict:
    """
    Write a Polars DataFrame to a Databricks SQL warehouse using INSERT statements.

    Parameters
    ----------
    df:
        The Polars DataFrame to write.
    statement_runner:
        Callable accepting a SQL statement string and executing it using the Databricks
        Statement Execution API. It must raise an exception when the statement fails.
    table_identifier:
        Fully-qualified Databricks table identifier, e.g. `catalog.schema.table`.
    create_table:
        Whether the target table should be created if it does not exist.
    write_mode:
        Either `append` or `overwrite`.
    chunk_size:
        Number of rows to include per INSERT statement batch.

    Returns
    -------
    dict
        Structure compatible with the other datasource write helpers used in vm-lib.
    """
    output = {
        "result": pl.DataFrame(),
        "error": {},
        "rows_written": 0,
        "run_date": datetime.now(timezone.utc),
    }

    try:
        if write_mode not in ("append", "overwrite"):
            raise ValueError('write_mode must be either "append" or "overwrite"')

        if df.is_empty():
            return output

        quoted_columns = [_quote_identifier(col) for col in df.columns]
        databricks_types = [
            _map_polars_to_databricks_type(dtype) for dtype in df.dtypes
        ]

        if create_table:
            columns_with_types = [
                f"{name} {dtype}"
                for name, dtype in zip(quoted_columns, databricks_types)
            ]
            create_sql = (
                f"CREATE TABLE IF NOT EXISTS {table_identifier} "
                f"({', '.join(columns_with_types)})"
            )
            statement_runner(create_sql)

        if write_mode == "overwrite":
            truncate_sql = f"TRUNCATE TABLE {table_identifier}"
            statement_runner(truncate_sql)

        rows = df.to_dicts()
        for chunk in _chunked(rows, chunk_size):
            values_clause = ", ".join(
                "(" + ", ".join(_format_literal(row[col]) for col in df.columns) + ")"
                for row in chunk
            )
            insert_sql = (
                f"INSERT INTO {table_identifier} "
                f"({', '.join(quoted_columns)}) VALUES {values_clause}"
            )
            statement_runner(insert_sql)

        types_row = {
            column: databricks_type
            for column, databricks_type in zip(df.columns, databricks_types)
        }
        types_df = pl.DataFrame([types_row])
        preview_df = df.head(19).select(
            [pl.col(column).cast(pl.Utf8) for column in df.columns]
        )

        output["result"] = pl.concat([types_df, preview_df], how="vertical")
        output["rows_written"] = df.height
        return output
    except Exception as error:
        output["error"] = {
            "message": str(error),
            "stacktrace": traceback.format_exc(),
        }
        return output


def _normalize_databricks_type(raw_type: str | None) -> str:
    if not raw_type:
        return "STRING"

    type_upper = raw_type.upper()

    if any(
        token in type_upper
        for token in (
            "INT",
            "LONG",
            "SHORT",
            "BYTE",
            "DOUBLE",
            "FLOAT",
            "DECIMAL",
            "NUMERIC",
        )
    ):
        return "NUMBER"
    if any(token in type_upper for token in ("DATE", "TIMESTAMP", "TIME")):
        return "DATE"
    if "BOOLEAN" in type_upper:
        return "NUMBER"
    return "STRING"


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def _chunked(iterable: Iterable[dict], size: int) -> Iterable[list[dict]]:
    chunk: list[dict] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _map_polars_to_databricks_type(polars_type: pl.DataType) -> str:
    type_str = str(polars_type).lower()

    if "int" in type_str:
        return "BIGINT"
    if "float" in type_str:
        return "DOUBLE"
    if "decimal" in type_str:
        return "DECIMAL(38, 18)"
    if "date" in type_str and "time" not in type_str:
        return "DATE"
    if "datetime" in type_str or "time" in type_str:
        return "TIMESTAMP"
    if "bool" in type_str:
        return "BOOLEAN"
    if "binary" in type_str:
        return "BINARY"
    return "STRING"


def _format_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        if isinstance(value, float) and (value != value):  # NaN check
            return "NULL"
        if isinstance(value, float) and value in (float("inf"), float("-inf")):
            return "NULL"
        return str(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='microseconds')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, time):
        return f"TIME '{value.isoformat(timespec='microseconds')}'"
    if isinstance(value, bytes):
        hex_str = value.hex()
        return f"X'{hex_str}'"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"
