from aiohttp import web
from aiohttp.web_urldispatcher import MatchInfoError

CORS_HEADERS = {
    # Using "*" ensures the Nginx cache remains consistent for all origins
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
    "Access-Control-Expose-Headers": "*",
}


def _accepts_method(match_info: web.UrlMappingMatchInfo, method: str) -> bool:
    # No route registers OPTIONS, so an existing path resolves to a 405 listing its allowed methods
    return (
        isinstance(match_info, MatchInfoError)
        and isinstance(match_info.http_exception, web.HTTPMethodNotAllowed)
        and method in match_info.http_exception.allowed_methods
    )


@web.middleware
async def _preflight_middleware(request: web.Request, handler):
    """Answer an OPTIONS preflight with a 204, only for a route accepting the requested method."""
    if request.method == "OPTIONS":
        requested_method = request.headers.get("Access-Control-Request-Method", "GET")
        if _accepts_method(request.match_info, requested_method):
            return web.Response(status=204)
    return await handler(request)


async def add_cors_headers(request: web.Request, response: web.StreamResponse) -> None:
    # on_response_prepare also fires for raised HTTP errors, unhandled 500s and streamed responses
    response.headers.update(CORS_HEADERS)


def setup_cors(app: web.Application) -> None:
    """Enable CORS on an app: answer OPTIONS preflights and add CORS headers to every response."""
    app.middlewares.append(_preflight_middleware)
    app.on_response_prepare.append(add_cors_headers)
