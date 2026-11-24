import csv
import io
import json
from collections.abc import AsyncGenerator, Collection, Sequence
from contextlib import aclosing
from typing import Any

from fastapi.responses import StreamingResponse
from starlette.types import Receive, Scope, Send

from api_tabular.config import settings
from api_tabular.engine import QueryResult, engine
from api_tabular.errors import QueryException
from api_tabular.serialization import as_csv_value, rows_to_json


def check_output_size(total: int) -> None:
    if total > settings.BATCH_SIZE:
        raise QueryException(
            403,
            "Output is too long",
            f"The output has more than {settings.BATCH_SIZE} rows, "
            "please consider downloading the source file directly",
        )


class ClosingStreamingResponse(StreamingResponse):
    """Closes its body once sent or cut off by the client.

    Starlette leaves the body of a cancelled response to the garbage collector, which only
    collects it at its next cycle pass: until then, the export keeps its slot (engine.stream).
    """

    def __init__(self, content: AsyncGenerator[bytes], **kwargs: Any) -> None:
        super().__init__(content, **kwargs)
        self.content = content

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self.content.aclose()


async def start_stream(sql: str, params: Sequence[Any]) -> AsyncGenerator[QueryResult]:
    """Run the query up to its first chunk, then yield all of them.

    A query failing once run must raise here, before the response status is sent: once the
    streaming has started, an error can only cut the body off.
    """
    chunks = engine.stream(sql, params)
    first = await anext(chunks)

    async def all_chunks() -> AsyncGenerator[QueryResult]:
        async with aclosing(chunks):
            yield first
            async for chunk in chunks:
                yield chunk

    return all_chunks()


async def stream_csv(chunks: AsyncGenerator[QueryResult]) -> AsyncGenerator[bytes]:
    header_written = False
    # closed with the response, it closes the chunks it reads, which `async for` does not do
    async with aclosing(chunks):
        async for chunk in chunks:
            buffer = io.StringIO()
            writer = csv.writer(buffer, lineterminator="\n")
            if not header_written:
                writer.writerow(chunk.columns)
                header_written = True
            for row in chunk.rows:
                writer.writerow([as_csv_value(value) for value in row])
            yield buffer.getvalue().encode("utf-8")


async def stream_json(
    chunks: AsyncGenerator[QueryResult], json_columns: Collection[str]
) -> AsyncGenerator[bytes]:
    separator = ""
    yield b"["
    async with aclosing(chunks):
        async for chunk in chunks:
            rows = rows_to_json(chunk.columns, chunk.rows, json_columns)
            yield (
                separator + ",".join(json.dumps(row, ensure_ascii=False) for row in rows)
            ).encode("utf-8")
            separator = ","
    yield b"]\n"
