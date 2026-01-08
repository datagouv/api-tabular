from typing import AsyncGenerator

from aiohttp import ClientSession

from api_tabular.core.error import handle_exception
from api_tabular.core.utils import process_total


async def stream_data(
    session: ClientSession,
    url: str,
    batch_size: int,
    accept_format: str,
) -> AsyncGenerator[bytes, None]:
    res = await session.head(f"{url}&limit=1&", headers={"Prefer": "count=exact"})
    if not res.ok:
        handle_exception(res.status, "Database error", await res.json(), None)
    total = process_total(res)
    if total > batch_size:
        handle_exception(
            403,
            "Output is too long",
            f"The output has more than {batch_size} rows, please consider downloading the source file directly",

            None,
        )
    async with session.get(
        url=f"{url}&limit={batch_size}", headers={"Accept": accept_format}
    ) as res:
        if not res.ok:
            handle_exception(res.status, "Database error", await res.json(), None)
        async for chunk in res.content.iter_chunked(1024):
            yield chunk
        yield b"\n"
