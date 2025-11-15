import traceback
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any

import polars as pl
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from typing_extensions import Literal


def _create_postgres_connection_url(details: dict[str, str]) -> str:
    user = details.get("user_name", "")
    password = details.get("password", "")
    host = details.get("host", "")
    port = details.get("port", 5432)
    database = details.get("database", "")

    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    else:
        return f"postgresql://{user}@{host}:{port}/{database}"


def _map_postgres_type(column_type: str) -> str:
    """
    Map common Postgres data types to the categories expected by Livedocs.
    """
    if not column_type:
        return "STRING"

    normalized = column_type.upper()

    if any(
        keyword in normalized
        for keyword in ["INT", "DECIMAL", "NUMERIC", "DOUBLE", "REAL", "FLOAT"]
    ):
        return "NUMBER"

    if any(keyword in normalized for keyword in ["DATE", "TIME", "TIMESTAMP"]):
        return "DATE"

    return "STRING"


def _process_postgres_schema(schema_data: pl.DataFrame) -> dict:
    if schema_data.is_empty():
        return {}

    type_column = None
    for candidate in ("data_type", "column_type"):
        if candidate in schema_data.columns:
            type_column = candidate
            break

    if type_column is None:
        raise ValueError(
            "Schema DataFrame must contain a 'data_type' or 'column_type' column"
        )

    processed_schema: dict[str, str] = {}
    for row in schema_data.iter_rows(named=True):
        processed_schema[row["column_name"]] = _map_postgres_type(row[type_column])

    return processed_schema


def describe_postgres_query(connection_string: str, query: str) -> pl.DataFrame:
    normalized_query = query.strip()
    if not normalized_query:
        return pl.DataFrame({"column_name": [], "data_type": []})

    normalized_query = normalized_query.rstrip(";")
    wrapped_query = (
        f"SELECT * FROM ({normalized_query}) AS livedocs_schema_sample LIMIT 0"
    )

    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute(wrapped_query)
            description = cursor.description or ()

        schema_rows = _schema_from_description(conn, description)

    if not schema_rows:
        return pl.DataFrame({"column_name": [], "data_type": []})

    return pl.DataFrame(schema_rows)


def _write_df_to_postgres(
    df: pl.DataFrame,
    connection_string: str,
    table_name: str,
    create_table: bool = False,
    write_mode: Literal["append", "overwrite"] = "append",
):
    """
    Aligns a Polars DataFrame to match a Postgres table schema and writes the DataFrame to the table.

    Args:
        df: Polars DataFrame to align
        connection_string: psycopg connection string
        table_name: Fully qualified table name (database optional)
        create_table: If True, create the table before writing
        write_mode: Either "append" or "overwrite"

    Returns:
        dict with keys:
            result: DataFrame with types as first row + first 19 rows of data
            error: Dict with error message and stack trace, or empty dict on success
            rows_written: Number of rows written (0 if failed)
            run_date: UTC timestamp of operation
    """
    output = {
        "result": pl.DataFrame(),
        "error": {},
        "rows_written": 0,
        "run_date": datetime.now(timezone.utc),
    }

    try:
        if write_mode not in ["append", "overwrite"]:
            raise ValueError('write_mode must be either "append" or "overwrite"')

        schema_name, table_identifier = _parse_table_identifier(table_name)
        aligned_df = df.clone()
        table_schema: list[dict[str, Any]] = []
        rows_to_insert: list[tuple[Any, ...]] = []

        with psycopg.connect(connection_string) as conn:
            with conn.cursor(row_factory=dict_row) as cursor:
                if create_table:
                    _ensure_schema_exists(cursor, schema_name)
                    if not _table_exists(cursor, schema_name, table_identifier):
                        _create_table_from_df(cursor, schema_name, table_identifier, df)

                table_schema = _fetch_postgres_schema(
                    cursor, schema_name, table_identifier
                )

                if not table_schema:
                    raise ValueError(
                        f"Table {schema_name}.{table_identifier} does not exist and create_table is False"
                    )

                _validate_required_columns(df, table_schema)

                aligned_df = _align_dataframe_to_schema(df, table_schema)

                if write_mode == "overwrite":
                    _truncate_table(cursor, schema_name, table_identifier)

                rows_to_insert = _prepare_rows_for_insert(aligned_df, table_schema)

                if rows_to_insert:
                    insert_query = _build_insert_query(
                        schema_name, table_identifier, table_schema
                    )
                    cursor.executemany(insert_query, rows_to_insert)

        output["result"], output["rows_written"] = _prepare_result_payload(
            aligned_df, table_schema, len(rows_to_insert)
        )

        return output

    except Exception as e:
        output["error"] = {"message": str(e), "stacktrace": traceback.format_exc()}
        return output


def _schema_from_description(
    conn: psycopg.Connection, description: Sequence[Any]
) -> list[dict[str, Any]]:
    if not description:
        return []

    type_oids = [
        type_code
        for type_code in {getattr(desc, "type_code", None) for desc in description}
        if type_code is not None
    ]

    type_map: dict[Any, dict[str, Any]] = {}

    if type_oids:
        with conn.cursor(row_factory=dict_row) as meta_cursor:
            meta_cursor.execute(
                """
                SELECT
                    t.oid,
                    pg_catalog.format_type(t.oid, NULL) AS data_type,
                    t.typcategory,
                    t.typname,
                    n.nspname AS type_schema
                FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
                WHERE t.oid = ANY(%s)
                """,
                (type_oids,),
            )

            for row in meta_cursor.fetchall() or []:
                type_map[row["oid"]] = row

    schema_rows: list[dict[str, Any]] = []
    for desc in description:
        type_info = type_map.get(getattr(desc, "type_code", None), {})
        data_type = type_info.get("data_type", "TEXT")

        schema_rows.append(
            {
                "column_name": getattr(desc, "name", ""),
                "data_type": data_type,
            }
        )

    return schema_rows


def _parse_table_identifier(table_name: str) -> tuple[str, str]:
    parts = [part.strip() for part in table_name.split(".") if part.strip()]
    if not parts:
        raise ValueError("table_name cannot be empty")

    if len(parts) == 1:
        return "public", _strip_identifier(parts[0])

    schema_name = _strip_identifier(parts[-2])
    table_identifier = _strip_identifier(parts[-1])

    if not schema_name:
        schema_name = "public"

    return schema_name, table_identifier


def _strip_identifier(identifier: str) -> str:
    identifier = identifier.strip()
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier[1:-1]
    return identifier


def _ensure_schema_exists(cursor, schema_name: str) -> None:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.schemata
        WHERE schema_name = %s
        """,
        (schema_name,),
    )
    exists = cursor.fetchone()
    if not exists:
        cursor.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(schema_name)
            )
        )


def _table_exists(cursor, schema_name: str, table_identifier: str) -> bool:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        ) AS exists
        """,
        (schema_name, table_identifier),
    )
    result = cursor.fetchone() or {}
    return bool(result.get("exists"))


def _create_table_from_df(
    cursor,
    schema_name: str,
    table_identifier: str,
    df: pl.DataFrame,
) -> None:
    if not df.columns:
        raise ValueError("Cannot create a table without columns")

    columns_sql = []
    for name, dtype in df.schema.items():
        postgres_type = _polars_to_postgres_type(dtype)
        columns_sql.append(
            sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(postgres_type))
        )

    create_stmt = sql.SQL("CREATE TABLE {}.{} ({})").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_identifier),
        sql.SQL(", ").join(columns_sql),
    )

    cursor.execute(create_stmt)


def _polars_to_postgres_type(dtype: pl.DataType) -> str:
    type_str = str(dtype)

    if type_str.startswith("Decimal"):
        inside = type_str[type_str.find("(") + 1 : type_str.rfind(")")]
        precision = None
        scale = None
        for part in inside.replace(" ", "").split(","):
            if part.startswith("precision="):
                precision = part.split("=", 1)[1]
            elif part.startswith("scale="):
                scale = part.split("=", 1)[1]
            elif not precision:
                precision = part
            elif not scale:
                scale = part
        if precision and scale:
            return f"DECIMAL({precision},{scale})"
        return "NUMERIC"

    lower = type_str.lower()

    if lower in {"int8", "uint8", "int16", "uint16"}:
        return "SMALLINT"
    if lower in {"int32", "uint32"}:
        return "INTEGER"
    if lower in {"int64", "uint64"}:
        return "BIGINT"
    if lower == "float32":
        return "REAL"
    if lower == "float64":
        return "DOUBLE PRECISION"
    if lower == "boolean":
        return "BOOLEAN"
    if lower.startswith("datetime"):
        return "TIMESTAMP WITH TIME ZONE"
    if lower == "date":
        return "DATE"
    if lower == "time":
        return "TIME"
    if lower == "binary":
        return "BYTEA"
    if (
        lower.startswith("list")
        or lower.startswith("struct")
        or lower.startswith("object")
    ):
        return "JSONB"

    return "TEXT"


def _fetch_postgres_schema(
    cursor, schema_name: str, table_identifier: str
) -> list[dict[str, Any]]:
    schema_query = """
        SELECT
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            NOT a.attnotnull AS is_nullable,
            t.typcategory,
            t.typname,
            tn.nspname AS type_schema
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
        JOIN pg_catalog.pg_namespace tn ON t.typnamespace = tn.oid
        WHERE c.relname = %s
          AND n.nspname = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum;
    """

    cursor.execute(schema_query, (table_identifier, schema_name))
    rows = cursor.fetchall() or []

    schema: list[dict[str, Any]] = []
    for row in rows:
        enum_values = None
        if row.get("typcategory") == "E":
            enum_values = _fetch_enum_values(
                cursor, row.get("typname"), row.get("type_schema")
            )

        schema.append(
            {
                "column_name": row.get("column_name"),
                "data_type": row.get("data_type"),
                "nullable": bool(row.get("is_nullable")),
                "enum_values": enum_values,
            }
        )

    return schema


def _fetch_enum_values(cursor, enum_type: str, enum_schema: str | None) -> list[str]:
    if not enum_type:
        return []

    cursor.execute(
        """
        SELECT enumlabel
        FROM pg_catalog.pg_enum e
        JOIN pg_catalog.pg_type t ON e.enumtypid = t.oid
        JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
        WHERE t.typname = %s AND n.nspname = COALESCE(%s, n.nspname)
        ORDER BY e.enumsortorder
        """,
        (enum_type, enum_schema),
    )

    return [row["enumlabel"] for row in cursor.fetchall() or []]


def _validate_required_columns(df: pl.DataFrame, schema: list[dict[str, Any]]) -> None:
    missing_required = [
        column["column_name"]
        for column in schema
        if not column["nullable"] and column["column_name"] not in df.columns
    ]

    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")


def _postgres_to_polars_type(
    type_name: str,
) -> (
    type[pl.Int64]
    | type[pl.Float64]
    | type[pl.Boolean]
    | type[pl.Datetime]
    | type[pl.Date]
    | type[pl.Time]
    | type[pl.Utf8]
    | type[pl.Binary]
):
    type_upper = type_name.upper()

    if any(keyword in type_upper for keyword in ["BIGINT", "INT", "SMALLINT"]):
        return pl.Int64
    if any(
        keyword in type_upper
        for keyword in ["DOUBLE", "NUMERIC", "DECIMAL", "REAL", "FLOAT"]
    ):
        return pl.Float64
    if "BOOL" in type_upper:
        return pl.Boolean
    if "TIMESTAMP" in type_upper:
        return pl.Datetime
    if "DATE" in type_upper:
        return pl.Date
    if "TIME" in type_upper:
        return pl.Time
    if "UUID" in type_upper:
        return pl.Utf8
    if "BYTEA" in type_upper or "BLOB" in type_upper:
        return pl.Binary

    return pl.Utf8


def _align_dataframe_to_schema(
    df: pl.DataFrame, schema: Iterable[dict[str, Any]]
) -> pl.DataFrame:
    if df.is_empty():
        base = {column["column_name"]: [] for column in schema}
        return pl.DataFrame(base)

    expressions = []
    for column in schema:
        column_name = column["column_name"]
        data_type = column["data_type"]
        nullable = column["nullable"]
        enum_values = column.get("enum_values") or []
        target_type = _postgres_to_polars_type(data_type)

        if column_name in df.columns:
            expr = pl.col(column_name)

            if enum_values:
                if nullable:
                    expr = expr.cast(pl.Utf8, strict=False).map_elements(
                        lambda value: value if value in enum_values else None
                    )
                else:
                    fallback = enum_values[0]
                    expr = expr.cast(pl.Utf8, strict=False).map_elements(
                        lambda value, fallback=fallback: value
                        if value in enum_values
                        else fallback
                    )
            elif "UUID" in data_type.upper():
                expr = expr.cast(pl.Utf8, strict=False)
            elif "TIMESTAMP" in data_type.upper():
                expr = expr.cast(pl.Datetime, strict=False).dt.cast_time_unit("us")
            elif "DATE" in data_type.upper() and "TIMESTAMP" not in data_type.upper():
                expr = expr.cast(pl.Date, strict=False)
            elif "TIME" in data_type.upper() and "TIMESTAMP" not in data_type.upper():
                expr = expr.cast(pl.Time, strict=False)
            else:
                expr = expr.cast(target_type, strict=False)

            if not nullable:
                default_value = _get_default_value_for_postgres_type(
                    data_type, enum_values
                )
                expr = expr.fill_null(default_value)

            expressions.append(expr.alias(column_name))
        else:
            if nullable:
                expressions.append(pl.lit(None).cast(target_type).alias(column_name))
            else:
                default_value = _get_default_value_for_postgres_type(
                    data_type, enum_values
                )
                expressions.append(
                    pl.lit(default_value).cast(target_type).alias(column_name)
                )

    return df.select(expressions)


def _get_default_value_for_postgres_type(
    type_name: str, enum_values: Iterable[str] | None
) -> Any:
    if enum_values:
        return next(iter(enum_values), None)

    type_upper = type_name.upper()

    if "INT" in type_upper:
        return 0
    if any(
        keyword in type_upper
        for keyword in ["DOUBLE", "NUMERIC", "DECIMAL", "REAL", "FLOAT"]
    ):
        return 0.0
    if "BOOL" in type_upper:
        return False
    if "TIMESTAMP" in type_upper:
        return datetime.now(timezone.utc)
    if "DATE" in type_upper and "TIMESTAMP" not in type_upper:
        return datetime.now(timezone.utc).date()
    if "TIME" in type_upper:
        return datetime.now(timezone.utc).time()
    if "UUID" in type_upper:
        return str(uuid.uuid4())
    if "BYTEA" in type_upper or "BLOB" in type_upper:
        return b""

    return ""


def _truncate_table(cursor, schema_name: str, table_identifier: str) -> None:
    cursor.execute(
        sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier(schema_name), sql.Identifier(table_identifier)
        )
    )


def _prepare_rows_for_insert(
    aligned_df: pl.DataFrame, schema: Iterable[dict[str, Any]]
) -> list[tuple[Any, ...]]:
    if aligned_df.is_empty():
        return []

    columns = [column["column_name"] for column in schema]
    rows: list[tuple[Any, ...]] = []

    for row in aligned_df.iter_rows(named=True):
        rows.append(tuple(row[column] for column in columns))

    return rows


def _build_insert_query(
    schema_name: str, table_identifier: str, schema: Iterable[dict[str, Any]]
):
    columns_sql = [sql.Identifier(column["column_name"]) for column in schema]
    placeholders = [sql.Placeholder() for _ in schema]

    return sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_identifier),
        sql.SQL(", ").join(columns_sql),
        sql.SQL(", ").join(placeholders),
    )


def _prepare_result_payload(
    aligned_df: pl.DataFrame,
    schema: Iterable[dict[str, Any]],
    rows_written: int,
) -> tuple[pl.DataFrame, int]:
    ordered_schema = OrderedDict(
        (column["column_name"], column["data_type"]) for column in schema
    )

    types_df = pl.DataFrame([ordered_schema])

    preview_df = _cast_dataframe_for_preview(aligned_df, ordered_schema.keys())

    result_df = types_df.vstack(preview_df)

    return result_df, rows_written


def _cast_dataframe_for_preview(
    df: pl.DataFrame, column_order: Iterable[str]
) -> pl.DataFrame:
    if df.is_empty():
        return pl.DataFrame({column: [] for column in column_order})

    return df.head(19).select(
        [
            pl.col(column).cast(pl.Utf8, strict=False).alias(column)
            for column in column_order
        ]
    )
