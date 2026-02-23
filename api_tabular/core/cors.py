from aiohttp import web


@web.middleware
async def cors_middleware(request, handler):
    """
    Middleware to handle CORS and the mandatory OPTIONS preflight.
    """
    if request.method == "OPTIONS":
        # Handle cases of preflight OPTIONS requests (was managed by aiohttp_cors before)
        response = web.Response(status=204)
    else:
        response = await handler(request)

    # Add CORS headers to the response
    # Using "*" ensures the Nginx cache remains consistent for all origins
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = (
        "Content-Type, Authorization, X-Requested-With"
    )
    response.headers["Access-Control-Expose-Headers"] = "*"

    return response
