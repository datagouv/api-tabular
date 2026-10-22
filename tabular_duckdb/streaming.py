import csv
import io
import json
from collections.abc import AsyncIterator
from contextlib import aclosing

from tabular_duckdb.config import settings
from tabular_duckdb.engine import QueryResult, engine
from tabular_duckdb.errors import QueryException
from tabular_duckdb.serialization import as_csv_value, rows_to_json


def check_output_size(total: int) -> None:
    if total > settings.BATCH_SIZE:
        raise QueryException(
            403,
            "Output is too long",
            f"The output has more than {settings.BATCH_SIZE} rows, "
            "please consider downloading the source file directly",
        )


async def start_stream(sql: str, params: list) -> AsyncIterator[QueryResult]:
    """Run the query up to its first chunk, then yield all of them.

    A query failing once run must raise here, before the response status is sent: once the
    streaming has started, an error can only cut the body off.
    """
    chunks = engine.stream(sql, params)
    first = await anext(chunks)

    async def all_chunks() -> AsyncIterator[QueryResult]:
        async with aclosing(chunks):
            yield first
            async for chunk in chunks:
                yield chunk

    return all_chunks()


async def stream_csv(chunks: AsyncIterator[QueryResult]) -> AsyncIterator[bytes]:
    header_written = False
    async for chunk in chunks:
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        if not header_written:
            writer.writerow(chunk.columns)
            header_written = True
        for row in chunk.rows:
            writer.writerow([as_csv_value(value) for value in row])
        yield buffer.getvalue().encode("utf-8")


async def stream_json(chunks: AsyncIterator[QueryResult]) -> AsyncIterator[bytes]:
    separator = ""
    yield b"["
    async for chunk in chunks:
        rows = rows_to_json(chunk.columns, chunk.rows)
        yield (separator + ",".join(json.dumps(row, ensure_ascii=False) for row in rows)).encode(
            "utf-8"
        )
        separator = ","
    yield b"]\n"
