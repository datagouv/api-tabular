"""Read-only SQL endpoint: user-provided queries on the resource, exposed as the `resource` table.

The query runs on a connection of its own of the DuckDB instance, which is sandboxed and
read-only (see `DuckDBEngine`): it cannot read outside the parquet prefix, change the
configuration nor anything shared, and whatever it creates disappears with its connection. On
top of that, the query must be a single SELECT, its result is capped at SQL_MAX_ROWS rows and
SQL_MAX_BYTES bytes, and it is interrupted after SQL_TIMEOUT seconds.
"""

import asyncio
import json
from dataclasses import dataclass

import duckdb

from tabular_duckdb.config import settings
from tabular_duckdb.engine import column_names, engine
from tabular_duckdb.errors import QueryException, safe_message
from tabular_duckdb.resources import Resource, is_storage_failure, read_error
from tabular_duckdb.serialization import rows_to_json

# the resource is always exposed under this table name
RESOURCE_TABLE = "resource"


def invalid_query(detail: str) -> QueryException:
    return QueryException(400, "Invalid SQL query", detail)


# rows fetched at once: the answer can exceed SQL_MAX_BYTES by one such chunk at most
FETCH_CHUNK_ROWS = 100


@dataclass(frozen=True)
class SqlResult:
    # JSON-serializable rows
    rows: list[dict]
    truncated: bool
    row_limit: int
    execution_ms: int


def fetch_bounded(cursor: duckdb.DuckDBPyConnection, row_limit: int) -> tuple[list[dict], bool]:
    """The rows of the result, up to `row_limit` of them and SQL_MAX_BYTES of JSON.

    The caller controls the size of each value (`repeat('x', 100000)`): the row cap alone does
    not bound the answer, and the fetched rows escape DuckDB's memory_limit.
    """
    columns = column_names(cursor)
    rows: list[dict] = []
    size = 0
    while chunk := cursor.fetchmany(FETCH_CHUNK_ROWS):
        for row in rows_to_json(columns, chunk):
            size += len(json.dumps(row, ensure_ascii=False))
            if len(rows) == row_limit or size > settings.SQL_MAX_BYTES:
                return rows, True
            rows.append(row)
    return rows, False


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
        # on its own lines, so that a trailing comment cannot swallow the closing parenthesis
        query = statements[0].query.strip().rstrip(";")
        wrapped_sql = f"SELECT * FROM (\n{query}\n) LIMIT {row_limit + 1}"

        def view_then_query() -> tuple[list[dict], bool]:
            try:
                cursor.execute(view_sql)
            except duckdb.InterruptException:
                raise
            except duckdb.Error as exc:
                # the view reads the parquet footer: the file itself is what fails here
                raise read_error(exc, resource.resource_id) from exc
            cursor.execute(wrapped_sql)
            return fetch_bounded(cursor, row_limit)

        started = asyncio.get_running_loop().time()
        try:
            rows, truncated = await engine.run(cursor, view_then_query, settings.SQL_TIMEOUT)
        except duckdb.Error as exc:
            if is_storage_failure(exc) and settings.storage_over_http:
                raise read_error(exc, resource.resource_id) from exc
            raise invalid_query(safe_message(str(exc))) from exc
        execution_ms = round((asyncio.get_running_loop().time() - started) * 1000)

    return SqlResult(rows=rows, truncated=truncated, row_limit=row_limit, execution_ms=execution_ms)
