from aiohttp import ClientSession, web
from aiohttp.web_request import Request

from api_tabular import config
from api_tabular.core.error import QueryException, handle_exception
from api_tabular.tabular.duckdb_exec import execute_query, stream_parquet_data
from api_tabular.tabular.duckdb_query import build_duckdb_query


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


async def get_resource_data(resource_id: str, query) -> tuple[list[dict], int | None]:
    return await execute_query(resource_id, query)


async def try_build_query(
    request: Request,
    query_string: list[str],
    resource_id: str,
    page_size: int | None = None,
    offset: int = 0,
):
    try:
        return build_duckdb_query(query_string, page_size=page_size, offset=offset)
    except ValueError as e:
        raise QueryException(400, None, "Invalid query string", f"Malformed query: {e}")


async def stream_resource_data(request: Request, format: str):
    resource_id = request.match_info["rid"]
    query_string = request.query_string.split("&") if request.query_string else []

    query = await try_build_query(request, query_string, resource_id)
    await get_resource(request.app["csession"], resource_id, [])

    mime = "application/json" if format == "json" else "text/csv"
    response_headers = {
        "Content-Disposition": f'attachment; filename="{resource_id}.{format}"',
        "Content-Type": mime,
    }

    return await stream_parquet_data(
        request=request,
        resource_id=resource_id,
        query=query,
        format=format,
        response_headers=response_headers,
    )
