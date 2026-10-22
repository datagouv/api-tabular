"""Read-only SQL endpoint: user-provided queries on the resource, exposed as the `resource` table.

The query runs on a connection of its own of the DuckDB instance, which is sandboxed and
read-only (see `DuckDBEngine`): it cannot read outside the parquet prefix, change the
configuration nor anything shared, and whatever it creates disappears with its connection. On
top of that, the query must be a single SELECT calling only ALLOWED_FUNCTIONS, its result is
capped at SQL_MAX_ROWS rows, and it is interrupted after SQL_TIMEOUT seconds.
"""

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import duckdb

from tabular_duckdb.config import settings
from tabular_duckdb.engine import engine, fetch
from tabular_duckdb.errors import QueryException, safe_message
from tabular_duckdb.resources import Resource, is_storage_failure, read_error
from tabular_duckdb.serialization import rows_to_json

# the resource is always exposed under this table name
RESOURCE_TABLE = "resource"

# enough for basic processing of the data (aggregates for a chart, rounding, dates), and nothing
# that fabricates values (`repeat`, `string_agg`, `list`, `||`) nor rows (`range`): an answer
# only holds values of the file, so that its size is bounded by the data and SQL_MAX_ROWS.
# Operators are functions to DuckDB: `~~` is LIKE, `~~*` ILIKE, `!` their negation.
ALLOWED_FUNCTIONS = frozenset(
    {
        # aggregates; count(*) is count_star
        "count",
        "count_star",
        "min",
        "max",
        "sum",
        "avg",
        "median",
        # arithmetic
        "+",
        "-",
        "*",
        "/",
        "//",
        "%",
        "round",
        "abs",
        "floor",
        "ceil",
        "nullif",
        # text
        "~~",
        "!~~",
        "~~*",
        "!~~*",
        "lower",
        "upper",
        "length",
        # dates; EXTRACT is date_part
        "year",
        "month",
        "day",
        "date_trunc",
        "date_part",
    }
)


def invalid_query(detail: str) -> QueryException:
    return QueryException(400, "Invalid SQL query", detail)


def called_functions(node: Any) -> set[str]:
    """Every function of the serialized query tree: scalar, aggregate, window and table ones."""
    if isinstance(node, dict):
        found = {node["function_name"].lower()} if "function_name" in node else set()
        return found.union(*(called_functions(value) for value in node.values()))
    if isinstance(node, list):
        return set().union(*(called_functions(value) for value in node))
    return set()


@dataclass(frozen=True)
class SqlResult:
    # JSON-serializable rows
    rows: list[dict]
    truncated: bool
    row_limit: int
    execution_ms: int


async def run_user_sql(resource: Resource, user_sql: str) -> SqlResult:
    if not user_sql.strip():
        raise invalid_query("The `q` parameter is required and cannot be empty")
    if len(user_sql) > settings.SQL_MAX_LENGTH:
        raise invalid_query(f"The query exceeds {settings.SQL_MAX_LENGTH} characters")
    row_limit = settings.SQL_MAX_ROWS
    view_sql = f"CREATE TEMP VIEW {RESOURCE_TABLE} AS SELECT * FROM {resource.source_sql}"

    async with engine.connection() as cursor:
        try:
            statements = cursor.extract_statements(user_sql)
        except duckdb.Error as exc:
            raise invalid_query(f"Could not parse the query: {safe_message(str(exc))}") from exc
        if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
            raise invalid_query("Only a single SELECT query is allowed")
        tree = json.loads(cursor.execute("SELECT json_serialize_sql(?)", [user_sql]).fetchone()[0])
        forbidden = called_functions(tree) - ALLOWED_FUNCTIONS
        if forbidden:
            raise invalid_query(f"Functions not allowed: {', '.join(sorted(forbidden))}")
        # on its own lines, so that a trailing comment cannot swallow the closing parenthesis
        query = statements[0].query.strip().rstrip(";")
        wrapped_sql = f"SELECT * FROM (\n{query}\n) LIMIT {row_limit + 1}"

        def view_then_query() -> list[dict]:
            try:
                cursor.execute(view_sql)
            except duckdb.InterruptException:
                raise
            except duckdb.Error as exc:
                # the view reads the parquet footer: the file itself is what fails here
                raise read_error(exc, resource.resource_id) from exc
            result = fetch(cursor, wrapped_sql)
            return rows_to_json(result.columns, result.rows)

        started = asyncio.get_running_loop().time()
        try:
            rows = await engine.run(cursor, view_then_query, settings.SQL_TIMEOUT)
        except duckdb.Error as exc:
            if is_storage_failure(exc) and settings.storage_over_http:
                raise read_error(exc, resource.resource_id) from exc
            raise invalid_query(safe_message(str(exc))) from exc
        execution_ms = round((asyncio.get_running_loop().time() - started) * 1000)

    return SqlResult(
        rows=rows[:row_limit],
        truncated=len(rows) > row_limit,
        row_limit=row_limit,
        execution_ms=execution_ms,
    )
