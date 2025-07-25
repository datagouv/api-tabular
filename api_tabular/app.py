import os
from datetime import datetime, timezone

import aiohttp_cors
import sentry_sdk
from aiohttp import ClientSession, web
from aiohttp_swagger import setup_swagger
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from api_tabular import config
from api_tabular.error import QueryException
from api_tabular.query import (
    get_potential_indexes,
    get_resource,
    get_resource_data,
    get_resource_data_streamed,
)
from api_tabular.utils import (
    build_link_with_page,
    build_sql_query_string,
    build_swagger_file,
    get_app_version,
    url_for,
)

routes = web.RouteTableDef()

sentry_sdk.init(
    dsn=config.SENTRY_DSN,
    integrations=[AioHttpIntegration()],
    traces_sample_rate=1.0,
)


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

    indexes: set | None = await get_potential_indexes(request.app["csession"], resource_id)
    try:
        sql_query = build_sql_query_string(query_string, resource_id, indexes, page_size, offset)
    except ValueError as e:
        raise QueryException(400, None, "Invalid query string", f"Malformed query: {e}")
    except PermissionError as e:
        raise QueryException(403, None, "Unauthorized parameters", str(e))
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
    resource_id = request.match_info["rid"]
    query_string = request.query_string.split("&") if request.query_string else []

    try:
        sql_query = build_sql_query_string(query_string, resource_id)
    except ValueError:
        raise QueryException(400, None, "Invalid query string", "Malformed query")
    except PermissionError as e:
        raise QueryException(403, None, "Unauthorized parameters", str(e))

    resource = await get_resource(request.app["csession"], resource_id, ["parsing_table"])

    response_headers = {
        "Content-Disposition": f'attachment; filename="{resource_id}.csv"',
        "Content-Type": "text/csv",
    }
    response = web.StreamResponse(headers=response_headers)
    await response.prepare(request)

    async for chunk in get_resource_data_streamed(request.app["csession"], resource, sql_query):
        await response.write(chunk)

    await response.write_eof()
    return response


@routes.get(r"/api/resources/{rid}/data/json/", name="json")
async def resource_data_json(request):
    resource_id = request.match_info["rid"]
    query_string = request.query_string.split("&") if request.query_string else []

    try:
        sql_query = build_sql_query_string(query_string, resource_id)
    except ValueError:
        raise QueryException(400, None, "Invalid query string", "Malformed query")
    except PermissionError as e:
        raise QueryException(403, None, "Unauthorized parameters", str(e))

    resource = await get_resource(request.app["csession"], resource_id, ["parsing_table"])

    response_headers = {
        "Content-Disposition": f'attachment; filename="{resource_id}.json"',
        "Content-Type": "application/json",
    }
    response = web.StreamResponse(headers=response_headers)
    await response.prepare(request)

    async for chunk in get_resource_data_streamed(
        request.app["csession"],
        resource,
        sql_query,
        accept_format="application/json",
    ):
        await response.write(chunk)

    await response.write_eof()
    return response


@routes.get(r"/health/")
async def get_health(request):
    """Return health check status"""
    # pinging a specific table that we know always exists
    url = f"{config.PGREST_ENDPOINT}/migrations_csv"
    async with request.app["csession"].head(url) as res:
        if not res.ok:
            raise QueryException(
                503,
                None,
                "DB unavailable",
                "postgREST has not started yet",
            )
    start_time = request.app["start_time"]
    current_time = datetime.now(timezone.utc)
    uptime_seconds = (current_time - start_time).total_seconds()
    return web.json_response(
        {"status": "ok", "version": request.app["app_version"], "uptime_seconds": uptime_seconds}
    )


@routes.get(r"/api/aggregation-exceptions/")
async def get_aggregation_exceptions(request):
    """Return the list of resources for which aggregation queries are allowed"""
    return web.json_response(config.ALLOW_AGGREGATION)


async def app_factory():
    async def on_startup(app):
        app["csession"] = ClientSession()
        app["start_time"] = datetime.now(timezone.utc)
        app["app_version"] = await get_app_version()

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
        app,
        swagger_url=config.DOC_PATH,
        ui_version=3,
        swagger_from_file="ressource_app_swagger.yaml",
    )

    return app


def run():
    web.run_app(app_factory(), path=os.environ.get("CSVAPI_APP_SOCKET_PATH"))


if __name__ == "__main__":
    run()
