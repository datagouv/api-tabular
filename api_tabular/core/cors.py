from aiohttp import web

CORS_HEADERS = {
    # Using "*" ensures the Nginx cache remains consistent for all origins
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
    "Access-Control-Expose-Headers": "*",
}


@web.middleware
async def cors_middleware(request, handler):
    """
    Middleware to handle the CORS OPTIONS preflight.
    """
    if request.method == "OPTIONS":
        # Handle cases of preflight OPTIONS requests (was managed by aiohttp_cors before)
        return web.Response(status=204)
    return await handler(request)


async def add_cors_headers(request: web.Request, response: web.StreamResponse) -> None:
    # on_response_prepare also fires for raised HTTP errors, unhandled 500s and streamed responses
    response.headers.update(CORS_HEADERS)
