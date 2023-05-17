import json

from aiohttp import web, ClientSession

from udata_hydra_csvapi import config


class QueryException(web.HTTPException):
    """Re-raise an exception from postgrest as aiohttp exception"""
    def __init__(self, status, data) -> None:
        self.status_code = status
        super().__init__(content_type="application/json", text=json.dumps(data))


def build_sql_query_string(request_arg: str, page: int, page_size: int) -> str:
    sql_query = []
    for arg in request_arg:
        value = arg.split('=')[1]
        argument = arg.split('=')[0]
        if '__' in argument:
            column, comparator = argument.split('__')
            if comparator == 'sort':
                if value == 'asc':
                    sql_query.append(f'order={column}.asc')
                elif value == 'desc':
                    sql_query.append(f'order={column}.desc')
            elif comparator == 'exact':
                sql_query.append(f'{column}=eq.{value}')
            elif comparator == 'contains':
                sql_query.append(f'{column}=like.*{value}*')
            elif comparator == 'less':
                sql_query.append(f'{column}=lte.{value}')
            elif comparator == 'greater':
                sql_query.append(f'{column}=gte.{value}')
    sql_query.append(f'limit={page_size}')
    if page > 1:
        offset = page_size * (page - 1)
        sql_query.append(f'offset={offset}')
    return '&'.join(sql_query)


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


async def get_resource_data(session: ClientSession, resource: dict, query_string: str, page: int, page_size: int):
    sql_query = build_sql_query_string(query_string, page, page_size)
    async with session.get(f"{config.PG_RST_URL}/{resource['parsing_table']}?{sql_query}") as res:
        if not res.ok:
            raise QueryException(res.status, await res.json())
        async for chunk in res.content.iter_chunked(1024):
            yield chunk
