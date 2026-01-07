from aiohttp import ClientSession, web
from aiohttp.web_request import Request

from api_tabular import config
from api_tabular.core.data import stream_data
from api_tabular.core.error import handle_exception, QueryException
from api_tabular.core.query import build_sql_query_string
from api_tabular.core.utils import process_total


async def get_resource(session: ClientSession, resource_id: str, columns: list) -> dict:
    # Always include deleted_at and dataset_id for deletion checking, but don't duplicate them
    if "deleted_at" not in columns:
        columns.append("deleted_at")
    if "dataset_id" not in columns:
        columns.append("dataset_id")
    q = f"select={','.join(columns)}&resource_id=eq.{resource_id}&order=created_at.desc"
    url = f"{config.PGREST_ENDPOINT}/tables_index?{q}"
    async with session.get(url) as res:
        record = await res.json()
        if not res.ok:
            handle_exception(res.status, "Database error", record, resource_id)
        if not record:
            raise web.HTTPNotFound()
        if record[0].get("deleted_at") is not None:
            deleted_at: str = record[0]["deleted_at"]
            dataset_id: str | None = record[0].get("dataset_id")
            message = f"Resource {resource_id} has been permanently deleted on {deleted_at} by its producer."
            if dataset_id:
                message += f" You can find more information about this resource at https://www.data.gouv.fr/datasets/{dataset_id}"
            else:
                message += " Contact the resource producer to get more information."
            raise web.HTTPGone(text=message)
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


async def get_potential_indexes(session: ClientSession, resource_id: str) -> set[str] | None:
    q = f"select=table_indexes&resource_id=eq.{resource_id}"
    url = f"{config.PGREST_ENDPOINT}/resources_exceptions?{q}"
    async with session.get(url) as res:
        record = await res.json()
        if not res.ok:
            handle_exception(res.status, "Database error", record, resource_id)
        if not record:
            return None
        # indexes look like {"column_name": "index_type", ...} or None
        indexes: dict = record[0].get("table_indexes", {})
        return set(indexes.keys()) if indexes else None


async def try_build_query(
    request: Request,
    query_string: list[str],
    resource_id: str,
    page_size: int | None = None,
    offset: int = 0,
):
    indexes: set | None = await get_potential_indexes(request.app["csession"], resource_id)
    try:
        sql_query = build_sql_query_string(query_string, resource_id, indexes, page_size, offset)
    except ValueError as e:
        raise QueryException(400, None, "Invalid query string", f"Malformed query: {e}")
    except PermissionError as e:
        raise QueryException(403, None, "Unauthorized parameters", str(e))
    return sql_query


async def stream_resource_data(request: Request, format: str):
    resource_id = request.match_info["rid"]
    query_string = request.query_string.split("&") if request.query_string else []

    sql_query = await try_build_query(request, query_string, resource_id)
    resource = await get_resource(request.app["csession"], resource_id, ["parsing_table"])

    mime = "application/json" if format == "json" else "text/csv"
    response_headers = {
        "Content-Disposition": f'attachment; filename="{resource_id}.{format}"',
        "Content-Type": mime,
    }
    response = web.StreamResponse(headers=response_headers)
    await response.prepare(request)


    async for chunk in stream_data(
        session=request.app["csession"],
        url=f"{config.PGREST_ENDPOINT}/{resource['parsing_table']}?{sql_query}",
        batch_size=config.BATCH_SIZE,
        accept_format=mime,
    ):
        await response.write(chunk)

    await response.write_eof()
    return response
