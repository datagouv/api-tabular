from aiohttp import web, ClientSession
from udata_hydra_csvapi import config
from udata_hydra_csvapi.error import QueryException
from udata_hydra_csvapi.utils import process_total


async def get_resource(session: ClientSession, resource_id: str, columns: list):
    q = f"select={','.join(columns)}&resource_id=eq.{resource_id}&order=created_at.desc"
    url = f"{config.PG_RST_URL}/tables_index?{q}"
    async with session.get(url) as res:
        record = await res.json()
        if not res.ok:
            raise QueryException(res.status, 'Database error', record)
        if not record:
            raise web.HTTPNotFound()
        return record[0]


async def get_resource_data(session: ClientSession, resource: dict, sql_query: str):
    headers = {'Prefer': 'count=exact'}
    url = f"{config.PG_RST_URL}/{resource['parsing_table']}?{sql_query}"
    async with session.get(url, headers=headers) as res:
        if not res.ok:
            raise QueryException(res.status, 'Database error', await res.json())
        record = await res.json()
        total = process_total(res.headers.get('Content-Range'))
        return record, total
