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
    print('---------------------------------')
    print(url)
    print('---------------------------------')
    async with session.get(url, headers=headers) as res:
        if not res.ok:
            handle_exception(
                res.status, "Database error", await res.json(), resource.get("id")
            )
        record = await res.json()
        total = process_total(res.headers.get("Content-Range"))
        print(res.headers)
        return record, total


async def get_resource_data_streamed(
    session: ClientSession,
    model: str,
    sql_query: str,
    accept_format: str = "text/csv",
    batch_size: int = config.BATCH_SIZE,
):
    url = f"{config.PG_RST_URL}/{model['parsing_table']}?{sql_query}"
    res = await session.head(f"{url}&limit=1&", headers={"Prefer": "count=exact"})
    total = process_total(res.headers.get("Content-Range"))
    for i in range(0, total, batch_size):
        async with session.get(
            url=f"{url}&limit={batch_size}&offset={i}", headers={"Accept": accept_format}
        ) as res:
            if not res.ok:
                handle_exception(res.status, "Database error", await res.json(), None)
            async for chunk in res.content.iter_chunked(1024):
                yield chunk
                yield b'\n'
