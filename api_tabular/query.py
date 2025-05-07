from typing import AsyncGenerator

from aiohttp import ClientSession, web

from api_tabular import config
from api_tabular.error import handle_exception
from api_tabular.utils import process_total


async def get_resource(session: ClientSession, resource_id: str, columns: list) -> dict:
    q = f"select={','.join(columns)}&resource_id=eq.{resource_id}&order=created_at.desc"
    url = f"{config.PGREST_ENDPOINT}/tables_index?{q}"
    async with session.get(url) as res:
        record = await res.json()
        if not res.ok:
            handle_exception(res.status, "Database error", record, resource_id)
        if not record:
            raise web.HTTPNotFound()
        return record[0]


async def get_resource_data(
    session: ClientSession, resource: dict, sql_query: str
) -> tuple[list[dict], int | None]:
    headers = {"Prefer": "count=exact"}
    url = f"{config.PGREST_ENDPOINT}/{resource['parsing_table']}?{sql_query}"
    skip_total = False
    if any(f".{agg}()" in url for agg in ["count", "max", "min", "sum", "avg"]):
        # the total for aggretated data is wrong, it is always the length of the original table
        skip_total = True
    async with session.get(url, headers=headers) as res:
        if not res.ok:
            handle_exception(res.status, "Database error", await res.json(), resource.get("id"))
        record = await res.json()
        total = process_total(res) if not skip_total else None
        return record, total


async def get_resource_data_streamed(
    session: ClientSession,
    model: str,
    sql_query: str,
    accept_format: str = "text/csv",
    batch_size: int = config.BATCH_SIZE,
) -> AsyncGenerator[bytes, None]:
    url = f"{config.PGREST_ENDPOINT}/{model['parsing_table']}?{sql_query}"
    res = await session.head(f"{url}&limit=1&", headers={"Prefer": "count=exact"})
    if not res.ok:
        handle_exception(res.status, "Database error", await res.json(), None)
    total = process_total(res)
    for i in range(0, total, batch_size):
        async with session.get(
            url=f"{url}&limit={batch_size}&offset={i}", headers={"Accept": accept_format}
        ) as res:
            if not res.ok:
                handle_exception(res.status, "Database error", await res.json(), None)
            async for chunk in res.content.iter_chunked(1024):
                yield chunk
            yield b"\n"


async def get_potential_indexes(session: ClientSession, resource_id: str) -> set[str] | None:
    q = f"select=table_indexes&resource_id=eq.{resource_id}"
    url = f"{config.MAINDB_ENDPOINT}/resources_exceptions?{q}"
    async with session.get(url) as res:
        record = await res.json()
        if not res.ok:
            handle_exception(res.status, "Database error", record, resource_id)
        if not record:
            return None
        # indexes look like {"column_name": "index_type", ...} or None
        indexes: dict = record[0].get("table_indexes", {})
        return set(indexes.keys()) if indexes else None
