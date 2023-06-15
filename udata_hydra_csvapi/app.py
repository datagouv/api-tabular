import os

from aiohttp import web, ClientSession

from udata_hydra_csvapi import config
from udata_hydra_csvapi.query import get_resource, get_resource_data
from udata_hydra_csvapi.utils import build_sql_query_string, build_link_with_page
from udata_hydra_csvapi.error import QueryException

routes = web.RouteTableDef()


@routes.get(r"/api/resources/{rid}/")
async def resource_meta(request):
    resource_id = request.match_info["rid"]
    resource = await get_resource(request.app["csession"], resource_id, ["created_at", "url"])
    return web.json_response({
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
        ]
    })


@routes.get(r"/api/resources/{rid}/profile/")
async def resource_profile(request):
    resource_id = request.match_info["rid"]
    resource = await get_resource(request.app["csession"], resource_id, ["profile:csv_detective"])
    return web.json_response(resource)


@routes.get(r"/api/resources/{rid}/data/")
async def resource_data(request):
    resource_id = request.match_info["rid"]
    query_string = request.query_string.split('&') if request.query_string else []
    page = int(request.query.get('page', '1'))
    page_size = int(request.query.get('page_size', config.PAGE_SIZE_DEFAULT))

    if page_size > config.PAGE_SIZE_MAX:
        raise QueryException(400, 'Invalid query string', 'Page size exceeds allowed maximum')
    if page > 1:
        offset = page_size * (page - 1)
    else:
        offset = 0

    try:
        sql_query = build_sql_query_string(query_string, page_size, offset)
    except ValueError:
        raise QueryException(400, 'Invalid query string', 'Malformed query')

    resource = await get_resource(request.app["csession"], resource_id, ["parsing_table"])
    response, total = await get_resource_data(request.app["csession"], resource, sql_query)

    body = {
        'data': response,
        'links': {},
        'meta': {
            'page': page,
            'page_size': page_size,
            'total': total
        }
    }
    if page_size + offset < total:
        body['links']['next'] = build_link_with_page(request.path, query_string, page + 1, page_size)
        if page > 1:
            body['links']['prev'] = build_link_with_page(request.path, query_string, page - 1, page_size)

    return web.json_response(body)


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
