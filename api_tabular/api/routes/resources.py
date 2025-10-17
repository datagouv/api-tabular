"""
Resource-related route definitions.
"""

from aiohttp import web

from ..handlers.resource_handlers import (
    handle_resource_data,
    handle_resource_data_csv,
    handle_resource_data_json,
    handle_resource_meta,
    handle_resource_profile,
    handle_resource_swagger,
)

routes = web.RouteTableDef()


@routes.get(r"/api/resources/{rid}/", name="meta")
async def resource_meta(request):
    """Get resource metadata."""
    return await handle_resource_meta(request)


@routes.get(r"/api/resources/{rid}/profile/", name="profile")
async def resource_profile(request):
    """Get resource profile information."""
    return await handle_resource_profile(request)


@routes.get(r"/api/resources/{rid}/swagger/", name="swagger")
async def resource_swagger(request):
    """Get resource Swagger documentation."""
    return await handle_resource_swagger(request)


@routes.get(r"/api/resources/{rid}/data/", name="data")
async def resource_data(request):
    """Get resource data as JSON."""
    return await handle_resource_data(request)


@routes.get(r"/api/resources/{rid}/data/csv/", name="csv")
async def resource_data_csv(request):
    """Get resource data as CSV."""
    return await handle_resource_data_csv(request)


@routes.get(r"/api/resources/{rid}/data/json/", name="json")
async def resource_data_json(request):
    """Get resource data as JSON file."""
    return await handle_resource_data_json(request)
