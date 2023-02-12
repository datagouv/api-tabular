import json

from aiohttp import web, ClientSession

from csvapi import config


class QueryException(web.HTTPException):
    """Re-raise an exception from postgrest as aiohttp exception"""
    def __init__(self, status, data) -> None:
        self.status_code = status
        super().__init__(content_type="application/json", text=json.dumps(data))


async def get_resource(session: ClientSession, resource_id: str, columns: list):
    q = f"select={','.join(columns)}&resource_id=eq.{resource_id}&order=created_at.desc"
    url = f"{config.PG_RST_URL}/tables_index?{q}"
    async with session.get(url) as res:
        record = await res.json()
        if not res.ok:
            raise QueryException(res.status, record)
        if not record:
            raise web.HTTPNotFound()
        return record[0]


async def get_resource_data(session: ClientSession, resource: dict, query_string: str):
    async with session.get(f"{config.PG_RST_URL}/{resource['parsing_table']}?{query_string}") as res:
        if not res.ok:
            raise QueryException(res.status, await res.json())
        async for chunk in res.content.iter_chunked(1024):
            yield chunk
