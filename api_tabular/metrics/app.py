import os
from datetime import datetime, timezone

import aiohttp_cors
import sentry_sdk
import yaml
from aiohttp import ClientSession, web
from aiohttp_swagger import setup_swagger

from api_tabular import config
from api_tabular.core.data import stream_data
from api_tabular.core.error import QueryException, handle_exception
from api_tabular.core.health import check_health
from api_tabular.core.query import build_sql_query_string
from api_tabular.core.sentry import sentry_kwargs
from api_tabular.core.url import build_link_with_page
from api_tabular.core.utils import build_offset, process_total
from api_tabular.core.version import get_app_version

routes = web.RouteTableDef()

sentry_sdk.init(**sentry_kwargs)


async def get_object_data(session: ClientSession, model: str, sql_query: str):
    headers = {"Prefer": "count=exact"}
    url = f"{config.PGREST_ENDPOINT}/{model}?{sql_query}"
    async with session.get(url, headers=headers) as res:
        if not res.ok:
            handle_exception(res.status, "Database error", await res.json(), None)
        record = await res.json()
        total = process_total(res)
        return record, total


@routes.get(r"/api/{model}/data/")
async def metrics_data(request):
    """
    Retrieve metric data for a specified model with optional filtering and sorting.
    """
    model = request.match_info["model"]
    query_string = request.query_string.split("&") if request.query_string else []
    page = int(request.query.get("page", "1"))
    page_size = int(request.query.get("page_size", config.PAGE_SIZE_DEFAULT))

    offset = build_offset(page, page_size)
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

    async for chunk in stream_data(
        session=request.app["csession"],
        url=f"{config.PGREST_ENDPOINT}/{model}?{sql_query}",
        batch_size=config.BATCH_SIZE,
        accept_format="text/csv",
    ):
        await response.write(chunk)

    return response


@routes.get(r"/health/")
async def get_health(request):
    """Return health check status"""
    # pinging a specific metrics table that we know always exists, managed by a DAG (https://github.com/datagouv/datagouvfr_data_pipelines/blob/main/dgv/metrics/sql/create_tables.sql)
    return await check_health(request, f"{config.PGREST_ENDPOINT}/site")


async def app_factory():
    async def on_startup(app):
        app["csession"] = ClientSession()
        app["start_time"] = datetime.now(timezone.utc)
        app["app_version"] = await get_app_version()

        with open("metrics_swagger.yaml", "r") as f:
            swagger_info = yaml.safe_load(f)
        swagger_info["info"]["version"] = app["app_version"]

        setup_swagger(app, swagger_url=config.DOC_PATH, ui_version=3, swagger_info=swagger_info)

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

    return app


def run():
    socket_path = os.environ.get("CSVAPI_APP_SOCKET_PATH")
    if socket_path:
        web.run_app(app_factory(), path=socket_path)
    else:
        port = int(os.environ.get("CSVAPI_APP_PORT", "8006"))
        web.run_app(app_factory(), port=port)


if __name__ == "__main__":
    run()
