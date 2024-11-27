import os

import aiohttp_cors
import sentry_sdk
from aiohttp import ClientSession, web
from aiohttp_swagger import setup_swagger
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from api_tabular import config
from api_tabular.error import QueryException, handle_exception
from api_tabular.utils import (
    build_link_with_page,
    build_sql_query_string,
    process_total,
)

routes = web.RouteTableDef()

sentry_sdk.init(
    dsn=config.SENTRY_DSN,
    integrations=[AioHttpIntegration()],
    traces_sample_rate=1.0,
)


async def get_object_data(session: ClientSession, model: str, sql_query: str):
    headers = {"Prefer": "count=exact"}
    url = f"{config.PGREST_ENDPOINT}/{model}?{sql_query}"
    async with session.get(url, headers=headers) as res:
        if not res.ok:
            handle_exception(res.status, "Database error", await res.json(), None)
        record = await res.json()
        total = process_total(res)
        return record, total


async def get_object_data_streamed(
    session: ClientSession,
    model: str,
    sql_query: str,
    accept_format: str = "text/csv",
    batch_size: int = config.BATCH_SIZE,
):
    headers = {"Accept": accept_format, "Prefer": "count=exact"}
    url = f"{config.PGREST_ENDPOINT}/{model}?{sql_query}"
    res = await session.head(f"{url}&limit=1&", headers=headers)
    if not res.ok:
        handle_exception(res.status, "Database error", await res.json(), None)
    total = process_total(res)
    for i in range(0, total, batch_size):
        async with session.get(url=f"{url}&limit={batch_size}&offset={i}", headers=headers) as res:
            if not res.ok:
                handle_exception(res.status, "Database error", await res.json(), None)
            async for chunk in res.content.iter_chunked(1024):
                yield chunk
            yield b"\n"


@routes.get(r"/api/{model}/data/")
async def metrics_data(request):
    """
    Retrieve metric data for a specified model with optional filtering and sorting.
    """
    model = request.match_info["model"]
    query_string = request.query_string.split("&") if request.query_string else []
    page = int(request.query.get("page", "1"))
    page_size = int(request.query.get("page_size", config.PAGE_SIZE_DEFAULT))

    if page_size > config.PAGE_SIZE_MAX:
        raise QueryException(
            400,
            None,
            "Invalid query string",
            f"Page size exceeds allowed maximum: {config.PAGE_SIZE_MAX}",
        )
    if page > 1:
        offset = page_size * (page - 1)
    else:
        offset = 0
    try:
        sql_query = build_sql_query_string(query_string, page_size=page_size, offset=offset)
    except ValueError as e:
        raise QueryException(400, None, "Invalid query string", f"Malformed query: {e}")

    response, total = await get_object_data(request.app["csession"], model, sql_query)

    next = build_link_with_page(request, query_string, page + 1, page_size)
    prev = build_link_with_page(request, query_string, page - 1, page_size)
    body = {
        "data": response,
        "links": {
            "next": next if page_size + offset < total else None,
            "prev": prev if page > 1 else None,
        },
        "meta": {"page": page, "page_size": page_size, "total": total},
    }
    return web.json_response(body)


@routes.get(r"/api/{model}/data/csv/")
async def metrics_data_csv(request):
    model = request.match_info["model"]
    query_string = request.query_string.split("&") if request.query_string else []

    try:
        sql_query = build_sql_query_string(query_string)
    except ValueError as e:
        raise QueryException(400, None, "Invalid query string", f"Malformed query: {e}")

    response_headers = {
        "Content-Disposition": f'attachment; filename="{model}.csv"',
        "Content-Type": "text/csv",
    }
    response = web.StreamResponse(headers=response_headers)
    await response.prepare(request)

    async for chunk in get_object_data_streamed(request.app["csession"], model, sql_query):
        await response.write(chunk)

    return response


@routes.get(r"/health/")
async def get_health(request):
    return web.HTTPOk()


async def app_factory():
    async def on_startup(app):
        app["csession"] = ClientSession()

    async def on_cleanup(app):
        await app["csession"].close()

    app = web.Application()
    app.add_routes(routes)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    cors = aiohttp_cors.setup(
        app,
        defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True, expose_headers="*", allow_headers="*"
            )
        },
    )
    for route in list(app.router.routes()):
        cors.add(route)

    setup_swagger(
        app, swagger_url=config.DOC_PATH, ui_version=3, swagger_from_file="metrics_swagger.yaml"
    )

    return app


def run():
    web.run_app(app_factory(), path=os.environ.get("CSVAPI_APP_SOCKET_PATH"))


if __name__ == "__main__":
    run()
