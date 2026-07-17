"""Execute DuckDB queries against remote or local Parquet files."""

from __future__ import annotations

import asyncio
import csv
import json
from io import StringIO
from typing import Any

import duckdb
from aiohttp import web
from aiohttp.web import StreamResponse
from aiohttp.web_request import Request

from api_tabular import config
from api_tabular.core.error import QueryException
from api_tabular.tabular.duckdb_query import DuckDBQuery, quote_ident


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    # httpfs is needed for https:// Parquet URLs; harmless for local files
    try:
        con.execute("INSTALL httpfs;")
    except duckdb.Error:
        pass
    try:
        con.execute("LOAD httpfs;")
    except duckdb.Error:
        pass
    return con


def _parquet_source_sql(parquet_path: str, columns: list[str]) -> tuple[str, list]:
    """Build FROM clause ensuring a synthetic __id when missing from the file."""
    if "__id" in columns:
        return "read_parquet(?)", [parquet_path]
    return (
        f"(SELECT file_row_number AS {quote_ident('__id')}, "
        f"* EXCLUDE (file_row_number) FROM read_parquet(?, file_row_number = true))",
        [parquet_path],
    )


def _describe_columns(con: duckdb.DuckDBPyConnection, parquet_path: str) -> list[str]:
    rows = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [parquet_path]).fetchall()
    return [row[0] for row in rows]


def _assemble_sql(
    source_sql: str,
    query: DuckDBQuery,
    *,
    for_count: bool = False,
) -> str:
    if for_count:
        return f"SELECT COUNT(*) FROM {source_sql} AS src {query.where_sql}"

    parts = [
        f"SELECT {query.select_sql} FROM {source_sql} AS src",
        query.where_sql,
        query.group_by_sql,
        query.order_sql,
    ]
    sql = " ".join(p for p in parts if p)
    if query.limit is not None:
        sql += f" LIMIT {int(query.limit)}"
    if query.offset >= 1:
        sql += f" OFFSET {int(query.offset)}"
    return sql


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, dict)):
        return value
    return str(value)


def _rows_to_dicts(con: duckdb.DuckDBPyConnection, result) -> list[dict[str, Any]]:
    columns = [desc[0] for desc in result.description]
    return [{col: _json_safe(val) for col, val in zip(columns, row)} for row in result.fetchall()]


def _execute_sync(
    parquet_path: str,
    query: DuckDBQuery,
) -> tuple[list[dict[str, Any]], int | None]:
    con = _connect()
    try:
        try:
            columns = _describe_columns(con, parquet_path)
        except duckdb.Error as e:
            message = str(e).lower()
            if "404" in message or "not found" in message or "no files found" in message:
                raise web.HTTPNotFound() from e
            raise QueryException(400, None, "Database error", str(e)) from e

        source_sql, source_params = _parquet_source_sql(parquet_path, columns)
        data_sql = _assemble_sql(source_sql, query, for_count=False)
        data_params = source_params + query.where_params

        try:
            result = con.execute(data_sql, data_params)
            rows = _rows_to_dicts(con, result)
        except duckdb.Error as e:
            raise QueryException(400, None, "Database error", str(e)) from e

        total: int | None = None
        if not query.is_aggregation:
            count_sql = _assemble_sql(source_sql, query, for_count=True)
            count_params = source_params + query.where_params
            total = int(con.execute(count_sql, count_params).fetchone()[0])

        return rows, total
    finally:
        con.close()


def _count_sync(parquet_path: str, query: DuckDBQuery) -> int:
    con = _connect()
    try:
        try:
            columns = _describe_columns(con, parquet_path)
        except duckdb.Error as e:
            message = str(e).lower()
            if "404" in message or "not found" in message or "no files found" in message:
                raise web.HTTPNotFound() from e
            raise QueryException(400, None, "Database error", str(e)) from e

        source_sql, source_params = _parquet_source_sql(parquet_path, columns)
        count_sql = _assemble_sql(source_sql, query, for_count=True)
        return int(con.execute(count_sql, source_params + query.where_params).fetchone()[0])
    except duckdb.Error as e:
        raise QueryException(400, None, "Database error", str(e)) from e
    finally:
        con.close()


def _fetch_all_sync(parquet_path: str, query: DuckDBQuery) -> list[dict[str, Any]]:
    rows, _ = _execute_sync(parquet_path, query)
    return rows


async def execute_query(
    resource_id: str,
    query: DuckDBQuery,
) -> tuple[list[dict[str, Any]], int | None]:
    parquet_path = config.parquet_url(resource_id)
    return await asyncio.to_thread(_execute_sync, parquet_path, query)


async def count_rows(resource_id: str, query: DuckDBQuery) -> int:
    parquet_path = config.parquet_url(resource_id)
    return await asyncio.to_thread(_count_sync, parquet_path, query)


async def stream_parquet_data(
    request: Request,
    resource_id: str,
    query: DuckDBQuery,
    format: str,
    response_headers: dict,
) -> StreamResponse:
    total = await count_rows(resource_id, query)
    if total > config.BATCH_SIZE:
        raise QueryException(
            403,
            None,
            "Output is too long",
            f"The output has more than {config.BATCH_SIZE} rows, please consider downloading the source file directly",
        )

    export_query = DuckDBQuery(
        where_sql=query.where_sql,
        where_params=list(query.where_params),
        order_sql=query.order_sql,
        select_sql=query.select_sql,
        group_by_sql=query.group_by_sql,
        is_aggregation=query.is_aggregation,
        limit=config.BATCH_SIZE,
        offset=0,
    )
    rows = await asyncio.to_thread(_fetch_all_sync, config.parquet_url(resource_id), export_query)

    response = web.StreamResponse(headers=response_headers)
    await response.prepare(request)

    if format == "json":
        await response.write(json.dumps(rows, default=str).encode())
    else:
        buffer = StringIO()
        if rows:
            writer = csv.DictWriter(buffer, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        await response.write(buffer.getvalue().encode())

    await response.write_eof()
    return response
