import os
import sentry_sdk

from aiohttp import web, ClientSession

from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from api_tabular import config
from api_tabular.query import (
    get_resource,
    get_resource_data,
    get_resource_data_streamed,
)
from api_tabular.utils import build_sql_query_string, build_link_with_page
from api_tabular.error import QueryException

routes = web.RouteTableDef()

sentry_sdk.init(
    dsn=config.SENTRY_DSN,
    integrations=[AioHttpIntegration()],
    traces_sample_rate=1.0,
)


@routes.get(r"/api/resources/{rid}/")
async def resource_meta(request):
    resource_id = request.match_info["rid"]
    resource = await get_resource(
        request.app["csession"], resource_id, ["created_at", "url"]
    )
    return web.json_response(
        {
            "created_at": resource["created_at"],
            "url": resource["url"],
            "links": [
                {
                    "href": f"/api/resources/{resource_id}/profile/",
                    "type": "GET",
                    "rel": "profile",
                },
                {
                    "href": f"/api/resources/{resource_id}/data/",
                    "type": "GET",
                    "rel": "data",
                },
            ],
        }
    )


@routes.get(r"/api/resources/{rid}/profile/")
async def resource_profile(request):
    resource_id = request.match_info["rid"]
    resource = await get_resource(
        request.app["csession"], resource_id, ["profile:csv_detective"]
    )
    return web.json_response(resource)


@routes.get(r"/api/resources/{rid}/data/")
async def resource_data(request):
    resource_id = request.match_info["rid"]
    query_string = request.query_string.split("&") if request.query_string else []
    page = int(request.query.get("page", "1"))
    page_size = int(request.query.get("page_size", config.PAGE_SIZE_DEFAULT))

    if page_size > config.PAGE_SIZE_MAX:
        raise QueryException(
            400, None, "Invalid query string", "Page size exceeds allowed maximum"
        )
    if page > 1:
        offset = page_size * (page - 1)
    else:
        offset = 0

    try:
        sql_query = build_sql_query_string(query_string, page_size, offset)
    except ValueError:
        raise QueryException(400, None, "Invalid query string", "Malformed query")

    resource = await get_resource(
        request.app["csession"], resource_id, ["parsing_table"]
    )
    response, total = await get_resource_data(
        request.app["csession"], resource, sql_query
    )

    next = build_link_with_page(request.path, query_string, page + 1, page_size)
    prev = build_link_with_page(request.path, query_string, page - 1, page_size)
    body = {
        "data": response,
        "links": {
            "profile": f"/api/resources/{resource_id}/profile/",
            "next": next if page_size + offset < total else None,
            "prev": prev if page > 1 else None,
        },
        "meta": {"page": page, "page_size": page_size, "total": total},
    }
    return web.json_response(body)


@routes.get(r"/api/resources/{rid}/data/csv/")
async def resource_data_csv(request):
    resource_id = request.match_info["rid"]
    query_string = request.query_string.split("&") if request.query_string else []

    try:
        sql_query = build_sql_query_string(query_string)
    except ValueError:
        raise QueryException(400, None, "Invalid query string", "Malformed query")

    resource = await get_resource(
        request.app["csession"], resource_id, ["parsing_table"]
    )

    response_headers = {
        "Content-Disposition": f'attachment; filename="{resource_id}.csv"',
        "Content-Type": "text/csv",
    }
    response = web.StreamResponse(headers=response_headers)
    await response.prepare(request)

    async for chunk in get_resource_data_streamed(
        request.app["csession"], resource, sql_query
    ):
        await response.write(chunk)

    await response.write_eof()
    return response


async def app_factory():
    async def on_startup(app):
        app["csession"] = ClientSession()

    async def on_cleanup(app):
        await app["csession"].close()

    app = web.Application()
    app.add_routes(routes)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


def run():
    web.run_app(app_factory(), path=os.environ.get("CSVAPI_APP_SOCKET_PATH"))


if __name__ == "__main__":
    run()
