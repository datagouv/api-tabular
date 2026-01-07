import os
from datetime import datetime, timezone

import aiohttp_cors
import sentry_sdk
import yaml
from aiohttp import ClientSession, web
from aiohttp_swagger import setup_swagger

from api_tabular import config
from api_tabular.core.health import check_health
from api_tabular.core.sentry import sentry_kwargs
from api_tabular.core.swagger import build_swagger_file
from api_tabular.core.url import build_link_with_page, url_for
from api_tabular.core.utils import build_offset
from api_tabular.core.version import get_app_version
from api_tabular.tabular.utils import (
    get_potential_indexes,
    get_resource,
    get_resource_data,
    stream_resource_data,
    try_build_query,
)

routes = web.RouteTableDef()

sentry_sdk.init(**sentry_kwargs)


@routes.get(r"/api/resources/{rid}/", name="meta")
async def resource_meta(request):
    resource_id = request.match_info["rid"]
    resource = await get_resource(request.app["csession"], resource_id, ["created_at", "url"])
    return web.json_response(
        {
            "created_at": resource["created_at"],
            "url": resource["url"],
            "links": [
                {
                    "href": url_for(request, "profile", rid=resource_id, _external=True),
                    "type": "GET",
                    "rel": "profile",
                },
                {
                    "href": url_for(request, "data", rid=resource_id, _external=True),
                    "type": "GET",
                    "rel": "data",
                },
                {
                    "href": url_for(request, "swagger", rid=resource_id, _external=True),
                    "type": "GET",
                    "rel": "swagger",
                },
            ],
        }
    )


@routes.get(r"/api/resources/{rid}/profile/", name="profile")
async def resource_profile(request):
    resource_id = request.match_info["rid"]
    resource: dict = await get_resource(
        request.app["csession"], resource_id, ["profile:csv_detective"]
    )
    indexes: set | None = await get_potential_indexes(request.app["csession"], resource_id)
    resource["indexes"] = list(indexes) if isinstance(indexes, set) else None
    return web.json_response(resource)


@routes.get(r"/api/resources/{rid}/swagger/", name="swagger")
async def resource_swagger(request):
    resource_id = request.match_info["rid"]
    resource: dict = await get_resource(
        request.app["csession"], resource_id, ["profile:csv_detective"]
    )
    indexes: set | None = await get_potential_indexes(request.app["csession"], resource_id)
    columns: dict[str, str] = resource["profile"]["columns"]
    if indexes:
        columns = {col: params for col, params in columns.items() if col in indexes}
    swagger_string = build_swagger_file(columns, resource_id)
    return web.Response(body=swagger_string)


def build_next_page(
    nb_results: int, page_size: int, offset: int, total: int | None, default_next: str
) -> str | None:
    if total is not None:
        # this is for raw or filtering queries
        return default_next if page_size + offset < total else None
    # for aggregation queries, the total is erroneous but we can be somewhat smart for the next page
    if nb_results < page_size:
        return None
    return default_next


@routes.get(r"/api/resources/{rid}/data/", name="data")
async def resource_data(request):
    resource_id = request.match_info["rid"]
    query_string = request.query_string.split("&") if request.query_string else []
    page = int(request.query.get("page", "1"))
    page_size = int(request.query.get("page_size", config.PAGE_SIZE_DEFAULT))

    offset = build_offset(page, page_size)

    sql_query = await try_build_query(request, query_string, resource_id, page_size, offset)
    resource = await get_resource(request.app["csession"], resource_id, ["parsing_table"])
    response, total = await get_resource_data(request.app["csession"], resource, sql_query)

    next = build_link_with_page(request, query_string, page + 1, page_size)
    prev = build_link_with_page(request, query_string, page - 1, page_size)
    body = {
        "data": response,
        "links": {
            "profile": url_for(request, "profile", rid=resource_id, _external=True),
            "swagger": url_for(request, "swagger", rid=resource_id, _external=True),
            "next": build_next_page(
                nb_results=len(response),
                page_size=page_size,
                offset=offset,
                total=total,
                default_next=next,
            ),
            "prev": prev if page > 1 else None,
        },
        "meta": {"page": page, "page_size": page_size},
    }
    if total is not None:
        body["meta"]["total"] = total
    return web.json_response(body)


@routes.get(r"/api/resources/{rid}/data/csv/", name="csv")
async def resource_data_csv(request):
    return await stream_resource_data(request, format="csv")


@routes.get(r"/api/resources/{rid}/data/json/", name="json")
async def resource_data_json(request):
    return await stream_resource_data(request, format="json")


@routes.get(r"/health/")
async def get_health(request):
    """Return health check status"""
    # pinging a specific table that we know always exists
    return await check_health(request, f"{config.PGREST_ENDPOINT}/migrations_csv")


@routes.get(r"/api/aggregation-exceptions/")
async def get_aggregation_exceptions(request):
    """Return the list of resources for which aggregation queries are allowed"""
    return web.json_response(config.ALLOW_AGGREGATION)


async def app_factory():
    async def on_startup(app):
        app["csession"] = ClientSession()
        app["start_time"] = datetime.now(timezone.utc)
        app["app_version"] = await get_app_version()

        with open("ressource_app_swagger.yaml", "r") as f:
            swagger_info = yaml.safe_load(f)
        swagger_info["info"]["version"] = app["app_version"]

        setup_swagger(
            app,
            swagger_url=config.DOC_PATH,
            ui_version=3,
            swagger_info=swagger_info,
        )

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
    web.run_app(app_factory(), path=os.environ.get("CSVAPI_APP_SOCKET_PATH"))


if __name__ == "__main__":
    run()
