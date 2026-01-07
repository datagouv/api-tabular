from datetime import datetime, timezone

from aiohttp import web
from aiohttp.web_request import Request

from api_tabular.core.error import QueryException


async def check_health(request: Request, url: str):
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
