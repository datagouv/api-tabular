from aiohttp import web, ClientSession
from api_tabular import config
from api_tabular.error import handle_exception
from api_tabular.utils import process_total


async def get_resource(session: ClientSession, resource_id: str, columns: list):
    q = f"select={','.join(columns)}&resource_id=eq.{resource_id}&order=created_at.desc"
    url = f"{config.PG_RST_URL}/tables_index?{q}"
    async with session.get(url) as res:
        record = await res.json()
        if not res.ok:
            handle_exception(res.status, "Database error", record, resource_id)
        if not record:
            raise web.HTTPNotFound()
        return record[0]


async def get_resource_data(session: ClientSession, resource: dict, sql_query: str):
    headers = {"Prefer": "count=exact"}
    url = f"{config.PG_RST_URL}/{resource['parsing_table']}?{sql_query}"
    async with session.get(url, headers=headers) as res:
        if not res.ok:
            handle_exception(
                res.status, "Database error", await res.json(), resource.get("id")
            )
        record = await res.json()
        total = process_total(res.headers.get("Content-Range"))
        return record, total


async def get_resource_data_streamed(
    session: ClientSession,
    resource: dict,
    sql_query: str,
    accept_format: str = "text/csv",
):
    headers = {"Accept": accept_format}
    url = f"{config.PG_RST_URL}/{resource['parsing_table']}?{sql_query}"
    async with session.get(url, headers=headers) as res:
        if not res.ok:
            handle_exception(
                res.status, "Database error", await res.json(), resource.get("id")
            )
        async for chunk in res.content.iter_chunked(1024):
            yield chunk
