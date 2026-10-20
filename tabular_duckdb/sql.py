"""Read-only SQL endpoint: validation and bounded execution of user-provided queries.

Defence in depth, in order:

1. `json_serialize_sql()` only accepts SELECT statements: anything else (PRAGMA, CALL, COPY,
   ATTACH, SET, INSTALL, CREATE, INSERT, DELETE...) is rejected before any execution.
2. exactly one statement, so nothing can hide behind a `;`.
3. no table function at all in the tree, which rules out `read_parquet`, `read_csv`, `read_text`,
   `glob`, `duckdb_secrets()`... hence any read of an arbitrary path with our S3 credentials.
4. the only base table allowed is the resource view (a bare s3:// path in a FROM clause is parsed
   as a base table named after the path, so it is rejected here too). CTEs are allowed.
5. what gets executed is not the user string but the SQL rebuilt by DuckDB from the validated
   tree, so comments and anything trailing cannot survive the validation.
6. execution happens on a sandboxed connection (`enable_external_access=false` with only the
   parquet prefix in `allowed_directories`, configuration locked) holding a temporary view on
   the resource only.
7. the query is wrapped in a `LIMIT`, and interrupted after a timeout.
"""

import asyncio
import json
from typing import Any

import duckdb

from tabular_duckdb.config import settings
from tabular_duckdb.engine import engine, quote_literal
from tabular_duckdb.errors import QueryException
from tabular_duckdb.query import ID_COLUMN
from tabular_duckdb.resources import Resource

# the resource is always exposed under this table name
RESOURCE_TABLE = "resource"

# table functions that cannot reach the filesystem nor the catalog
ALLOWED_TABLE_FUNCTIONS = frozenset({"generate_series", "range", "unnest", "repeat"})

# scalar functions that could leak configuration or read files
DENIED_FUNCTIONS = frozenset({"current_setting", "getvariable", "read_text", "read_blob"})

ALLOWED_SCHEMAS = frozenset({"", "main", "temp"})


def invalid_query(detail: str) -> QueryException:
    return QueryException(400, None, "Invalid SQL query", detail)


def _collect(node: Any, base_tables: set, table_functions: set, functions: set, ctes: set) -> None:
    """Walk the serialized query tree and collect what we need to check."""
    if isinstance(node, dict):
        node_type = node.get("type")
        if node_type == "BASE_TABLE":
            base_tables.add(
                (
                    node.get("catalog_name") or "",
                    node.get("schema_name") or "",
                    node.get("table_name") or "",
                )
            )
        elif node_type == "TABLE_FUNCTION":
            function = node.get("function") or {}
            table_functions.add((function.get("function_name") or "").lower())
        elif node_type == "FUNCTION":
            functions.add((node.get("function_name") or "").lower())
        cte_map = node.get("cte_map")
        if isinstance(cte_map, dict):
            for entry in cte_map.get("map") or []:
                if isinstance(entry, dict) and entry.get("key"):
                    ctes.add(str(entry["key"]).lower())
        for value in node.values():
            _collect(value, base_tables, table_functions, functions, ctes)
    elif isinstance(node, list):
        for value in node:
            _collect(value, base_tables, table_functions, functions, ctes)


def validate_sql(conn: duckdb.DuckDBPyConnection, user_sql: str) -> str:
    """Reject anything that is not a single, self-contained SELECT on the resource.

    Returns the SQL rebuilt by DuckDB from the validated tree, which is what gets executed.
    """
    statement = user_sql.strip()
    if not statement:
        raise invalid_query("The `q` parameter is required and cannot be empty")
    if len(statement) > settings.SQL_MAX_LENGTH:
        raise invalid_query(f"The query exceeds {settings.SQL_MAX_LENGTH} characters")

    try:
        serialized = conn.execute("SELECT json_serialize_sql(?)", [statement]).fetchone()
    except duckdb.Error as exc:
        raise invalid_query(f"Could not parse the query: {exc}") from exc
    tree = json.loads(serialized[0]) if serialized else {}

    if tree.get("error"):
        # non-SELECT statements cannot be serialized, which is exactly what we want to catch
        message = tree.get("error_message") or "only read-only SELECT queries are allowed"
        raise invalid_query(f"Only read-only SELECT queries are allowed ({message.strip()})")

    statements = tree.get("statements") or []
    if len(statements) != 1:
        raise invalid_query("Exactly one SELECT statement is allowed")

    base_tables: set[tuple[str, str, str]] = set()
    table_functions: set[str] = set()
    functions: set[str] = set()
    ctes: set[str] = set()
    _collect(statements[0], base_tables, table_functions, functions, ctes)

    forbidden_table_functions = table_functions - ALLOWED_TABLE_FUNCTIONS
    if forbidden_table_functions:
        raise invalid_query(
            f"Table functions are not allowed: {', '.join(sorted(forbidden_table_functions))}. "
            f"The resource is already available as the `{RESOURCE_TABLE}` table."
        )

    forbidden_functions = functions & DENIED_FUNCTIONS
    if forbidden_functions:
        raise invalid_query(f"Functions not allowed: {', '.join(sorted(forbidden_functions))}")

    for catalog, schema, table in base_tables:
        if table.lower() in ctes:
            continue
        if (
            table != RESOURCE_TABLE
            or schema not in ALLOWED_SCHEMAS
            or catalog not in ("", "memory")
        ):
            raise invalid_query(
                f"Unknown table `{table}`: only the `{RESOURCE_TABLE}` table "
                "(and your own CTEs) can be queried"
            )

    # rebuild the statement from the validated tree: this is what we will run
    try:
        rebuilt = conn.execute(
            "SELECT json_deserialize_sql(?)",
            [json.dumps({"error": False, "statements": [statements[0]]})],
        ).fetchone()
    except duckdb.Error as exc:
        raise invalid_query(f"Could not parse the query: {exc}") from exc
    if not rebuilt or not rebuilt[0]:
        raise invalid_query("Could not parse the query")
    return str(rebuilt[0])


async def run_user_sql(resource: Resource, user_sql: str) -> dict:
    """Validate then run the query, returning the response body of the /sql/ endpoint."""
    row_limit = max(1, settings.SQL_MAX_ROWS)
    view_sql = (
        f"CREATE OR REPLACE TEMP VIEW {RESOURCE_TABLE} AS "
        f"SELECT * EXCLUDE (file_row_number), file_row_number + 1 AS {ID_COLUMN} "
        f"FROM read_parquet({quote_literal(resource.object_url)}, file_row_number = true)"
    )

    async with engine.sql_connection() as conn:
        statement = await asyncio.to_thread(validate_sql, conn, user_sql)
        wrapped_sql = f"SELECT * FROM ({statement}) LIMIT {row_limit + 1}"
        try:
            await asyncio.to_thread(conn.execute, view_sql)
        except duckdb.Error as exc:
            raise QueryException(
                400, None, "Invalid resource file", f"Could not read the parquet file: {exc}"
            ) from exc
        started = asyncio.get_running_loop().time()
        try:
            columns, rows = await asyncio.to_thread(
                engine.run_with_timeout, conn, wrapped_sql, settings.SQL_TIMEOUT
            )
        except duckdb.InterruptException as exc:
            raise QueryException(
                400,
                None,
                "Query timeout",
                f"The query was interrupted after {settings.SQL_TIMEOUT} seconds",
            ) from exc
        except duckdb.Error as exc:
            raise invalid_query(_safe_message(str(exc))) from exc
        execution_ms = round((asyncio.get_running_loop().time() - started) * 1000)

    truncated = len(rows) > row_limit
    return {
        "columns": columns,
        "rows": rows[:row_limit],
        "meta": {
            "rows": min(len(rows), row_limit),
            "truncated": truncated,
            "row_limit": row_limit,
            "execution_ms": execution_ms,
        },
    }


def _safe_message(message: str) -> str:
    """Do not leak the storage location nor credentials in DuckDB error messages."""
    redacted = message.replace(settings.PARQUET_BASE_URL.rstrip("/") + "/", "<resource>/")
    for secret in (settings.S3_SECRET_KEY, settings.S3_ACCESS_KEY):
        if secret:
            redacted = redacted.replace(secret, "***")
    return redacted.strip()
