"""Read-only SQL endpoint: validation and bounded execution of user-provided queries.

Defence in depth, in order:

1. `json_serialize_sql()` only accepts SELECT statements: anything else (PRAGMA, CALL, COPY,
   ATTACH, SET, INSTALL, CREATE, INSERT, DELETE...) is rejected before any execution.
2. exactly one statement, so nothing can hide behind a `;`.
3. no table function at all in the tree, which rules out `read_parquet`, `read_csv`, `read_text`,
   `glob`, `duckdb_secrets()`... hence any read of an arbitrary path with our S3 credentials. The
   built-in macros querying the catalog in their body (`pg_get_viewdef`...) are denied as well.
4. the only base table allowed is the resource view (a bare s3:// path in a FROM clause is parsed
   as a base table named after the path, so it is rejected here too). CTEs are allowed, where
   DuckDB binds them to the CTE and not to a table of the catalog.
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
from tabular_duckdb.engine import engine, fetch, interruptible
from tabular_duckdb.errors import QueryException, query_timeout, safe_message
from tabular_duckdb.resources import Resource, read_error

# the resource is always exposed under this table name
RESOURCE_TABLE = "resource"

# table functions that cannot reach the filesystem nor the catalog
ALLOWED_TABLE_FUNCTIONS = frozenset({"generate_series", "range", "unnest", "repeat"})


def _catalog_macros() -> frozenset[str]:
    """Built-in scalar macros whose body queries a table, e.g. `pg_get_viewdef` on
    `duckdb_views()`: the tree only shows them as a function call, not as a table function."""
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            r"""
            SELECT function_name FROM duckdb_functions()
            WHERE function_type = 'macro' AND regexp_matches(macro_definition, '(?i)\bfrom\b')
            """
        ).fetchall()
    return frozenset(name for (name,) in rows)


# scalar functions that could leak configuration, the catalog, or read files
DENIED_FUNCTIONS = (
    frozenset({"current_setting", "getvariable", "read_text", "read_blob"}) | _catalog_macros()
)

ALLOWED_SCHEMAS = frozenset({"", "main", "temp"})


def invalid_query(detail: str) -> QueryException:
    return QueryException(400, "Invalid SQL query", detail)


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
        entries = cte_map.get("map") or [] if isinstance(cte_map, dict) else []
        for entry in entries:
            if not isinstance(entry, dict) or not entry.get("key"):
                continue
            name = str(entry["key"]).lower()
            body = entry.get("value") or {}
            body_type = (body.get("query_node") or {}).get("type")
            # in the body of a CTE, DuckDB binds the CTEs declared after it, and its own name
            # unless it is recursive, to the catalog: `WITH x AS (SELECT * FROM duckdb_views),
            # duckdb_views AS (...)` reads the system view
            _collect(
                body,
                tables,
                table_functions,
                functions,
                (visible_ctes | {name}) if body_type == "RECURSIVE_CTE_NODE" else visible_ctes,
            )
            visible_ctes = visible_ctes | {name}
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
        for key, value in node.items():
            if key != "cte_map":
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
        # identifiers are case insensitive in DuckDB
        if (
            reference.table.lower() != RESOURCE_TABLE
            or reference.schema.lower() not in ALLOWED_SCHEMAS
            # the resource is a temporary view, it lives in the `temp` catalog
            or reference.catalog.lower() not in ("", "temp")
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


@dataclass(frozen=True)
class SqlResult:
    columns: list[str]
    rows: list[tuple]
    truncated: bool
    row_limit: int
    execution_ms: int


async def run_user_sql(resource: Resource, user_sql: str) -> SqlResult:
    """Validate then run the query, within SQL_TIMEOUT once a connection is borrowed."""
    row_limit = settings.SQL_MAX_ROWS
    view_sql = (
        f"CREATE OR REPLACE TEMP VIEW {RESOURCE_TABLE} AS SELECT * FROM {resource.source_sql}"
    )

    async with engine.sql_connection() as conn:
        statement = await asyncio.to_thread(validate_sql, conn, user_sql)
        wrapped_sql = f"SELECT * FROM ({statement}) LIMIT {row_limit + 1}"
        started = asyncio.get_running_loop().time()
        # the view reads the remote parquet footer: it shares the timeout of the query
        try:
            await asyncio.to_thread(
                interruptible, conn, lambda: conn.execute(view_sql), settings.SQL_TIMEOUT
            )
        except duckdb.InterruptException as exc:
            raise query_timeout(settings.SQL_TIMEOUT) from exc
        except duckdb.Error as exc:
            raise read_error(exc) from exc
        remaining = settings.SQL_TIMEOUT - (asyncio.get_running_loop().time() - started)
        try:
            result = await asyncio.to_thread(
                interruptible, conn, lambda: fetch(conn, wrapped_sql), max(remaining, 0.001)
            )
        except duckdb.InterruptException as exc:
            raise query_timeout(settings.SQL_TIMEOUT) from exc
        except duckdb.Error as exc:
            raise invalid_query(safe_message(str(exc))) from exc
        execution_ms = round((asyncio.get_running_loop().time() - started) * 1000)

    return SqlResult(
        columns=result.columns,
        rows=result.rows[:row_limit],
        truncated=len(result.rows) > row_limit,
        row_limit=row_limit,
        execution_ms=execution_ms,
    )
