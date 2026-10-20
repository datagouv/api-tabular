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
from dataclasses import dataclass
from typing import Any

import duckdb

from tabular_duckdb.config import settings
from tabular_duckdb.engine import engine
from tabular_duckdb.errors import QueryException
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


@dataclass(frozen=True)
class TableReference:
    catalog: str
    schema: str
    table: str
    # names of the CTEs in scope where the table is referenced
    visible_ctes: frozenset[str]

    @property
    def is_cte(self) -> bool:
        # a CTE can only be referenced unqualified, and only within its own query
        return not self.catalog and not self.schema and self.table.lower() in self.visible_ctes


def _collect(
    node: Any,
    tables: set[TableReference],
    table_functions: set[str],
    functions: set[str],
    visible_ctes: frozenset[str] = frozenset(),
) -> None:
    """Walk the serialized query tree and collect what we need to check."""
    if isinstance(node, dict):
        cte_map = node.get("cte_map")
        if isinstance(cte_map, dict):
            # the CTEs of a query are visible in the whole query, their own bodies included
            visible_ctes = visible_ctes | {
                str(entry["key"]).lower()
                for entry in cte_map.get("map") or []
                if isinstance(entry, dict) and entry.get("key")
            }
        node_type = node.get("type")
        if node_type == "BASE_TABLE":
            tables.add(
                TableReference(
                    catalog=node.get("catalog_name") or "",
                    schema=node.get("schema_name") or "",
                    table=node.get("table_name") or "",
                    visible_ctes=visible_ctes,
                )
            )
        elif node_type == "TABLE_FUNCTION":
            function = node.get("function") or {}
            table_functions.add((function.get("function_name") or "").lower())
        elif node_type == "FUNCTION":
            functions.add((node.get("function_name") or "").lower())
        for value in node.values():
            _collect(value, tables, table_functions, functions, visible_ctes)
    elif isinstance(node, list):
        for value in node:
            _collect(value, tables, table_functions, functions, visible_ctes)


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

    tables: set[TableReference] = set()
    table_functions: set[str] = set()
    functions: set[str] = set()
    _collect(statements[0], tables, table_functions, functions)

    forbidden_table_functions = table_functions - ALLOWED_TABLE_FUNCTIONS
    if forbidden_table_functions:
        raise invalid_query(
            f"Table functions are not allowed: {', '.join(sorted(forbidden_table_functions))}. "
            f"The resource is already available as the `{RESOURCE_TABLE}` table."
        )

    forbidden_functions = functions & DENIED_FUNCTIONS
    if forbidden_functions:
        raise invalid_query(f"Functions not allowed: {', '.join(sorted(forbidden_functions))}")

    for reference in tables:
        if reference.is_cte:
            continue
        if (
            reference.table != RESOURCE_TABLE
            or reference.schema not in ALLOWED_SCHEMAS
            or reference.catalog not in ("", "memory")
        ):
            raise invalid_query(
                f"Unknown table `{reference.table}`: only the `{RESOURCE_TABLE}` table "
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
        f"CREATE OR REPLACE TEMP VIEW {RESOURCE_TABLE} AS SELECT * FROM {resource.source_sql}"
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
