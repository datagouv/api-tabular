import os

from aiohttp import web, ClientSession

from udata_hydra_csvapi.query import get_resource, get_resource_data

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
    page = request.rel_url.query.get('page', 1)
    page_size = request.rel_url.query.get('page_size', 50)
    query_string = request.query_string.split('&')
    resource = await get_resource(request.app["csession"], resource_id, ["parsing_table"])
    response = None
    # stream response from postgrest, this might be a big payload
    async for chunk in get_resource_data(request.app["csession"], resource, query_string, page, page_size):
        # build the response after get_resource_data has been called:
        # if a QueryException occurs we don't want to start a chunked streaming response
        if response is None:
            response = web.StreamResponse()
            response.content_type = "application/json"
            await response.prepare(request)
        await response.write(chunk)
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
