"""
Main API application factory.

This module creates the aiohttp application with all routes and middleware.
"""

import os
from datetime import datetime, timezone

import aiohttp_cors
import sentry_sdk
from aiohttp import ClientSession, web
from aiohttp_swagger import setup_swagger
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from api_tabular import config
from api_tabular.error import QueryException
from api_tabular.utils import get_app_version

from .routes.resources import routes as resource_routes

# Initialize Sentry
sentry_sdk.init(
    dsn=config.SENTRY_DSN,
    integrations=[AioHttpIntegration()],
    traces_sample_rate=1.0,
)


async def health_handler(request):
    """Handle health check requests."""
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


async def aggregation_exceptions_handler(request):
    """Handle aggregation exceptions requests."""
    return web.json_response(config.ALLOW_AGGREGATION)


async def app_factory():
    """Create and configure the aiohttp application."""

    async def on_startup(app):
        app["csession"] = ClientSession()
        app["start_time"] = datetime.now(timezone.utc)
        app["app_version"] = await get_app_version()

    async def on_cleanup(app):
        await app["csession"].close()

    app = web.Application()

    # Add all routes
    app.add_routes(resource_routes)

    # Add health and utility routes
    app.router.add_get("/health/", health_handler)
    app.router.add_get("/api/aggregation-exceptions/", aggregation_exceptions_handler)

    # Setup startup and cleanup
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    # Setup CORS
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

    # Setup Swagger documentation
    setup_swagger(
        app,
        swagger_url=config.DOC_PATH,
        ui_version=3,
        swagger_from_file="ressource_app_swagger.yaml",
    )

    return app


def run():
    """Run the application."""
    web.run_app(app_factory(), path=os.environ.get("CSVAPI_APP_SOCKET_PATH"))
