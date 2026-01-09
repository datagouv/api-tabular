from typing import AsyncGenerator

from aiohttp import ClientSession, web
from aiohttp.web_request import Request

from api_tabular import config
from api_tabular.core.error import QueryException, handle_exception
from api_tabular.core.utils import process_total


async def stream_data(
    session: ClientSession,
    request: Request,
    url: str,
    accept_format: str,
    response_headers: dict,
) -> AsyncGenerator[bytes, None]:
    res = await session.head(f"{url}&limit=1&", headers={"Prefer": "count=exact"})
    if not res.ok:
        handle_exception(res.status, "Database error", await res.json(), None)
    total = process_total(res)
    if total > config.BATCH_SIZE:
        raise QueryException(
            403,
            None,
            "Output is too long",
            f"The output has more than {config.BATCH_SIZE} rows, please consider downloading the source file directly",
        )
    response = web.StreamResponse(headers=response_headers)
    await response.prepare(request)

    async with session.get(
        url=f"{url}&limit={config.BATCH_SIZE}", headers={"Accept": accept_format}
    ) as res:
        if not res.ok:
            handle_exception(res.status, "Database error", await res.json(), None)
        async for chunk in res.content.iter_chunked(1024):
            await response.write(chunk)
        await response.write(b"\n")

    await response.write_eof()
    return response
