import csv
import io
import json
from collections.abc import AsyncIterator

from tabular_duckdb.config import settings
from tabular_duckdb.engine import engine
from tabular_duckdb.errors import QueryException
from tabular_duckdb.serialization import as_csv_value, jsonable


def check_output_size(total: int) -> None:
    if total > settings.BATCH_SIZE:
        raise QueryException(
            403,
            None,
            "Output is too long",
            f"The output has more than {settings.BATCH_SIZE} rows, "
            "please consider downloading the source file directly",
        )


async def stream_csv(sql: str, params: list) -> AsyncIterator[bytes]:
    header_written = False
    async for columns, rows in engine.stream(sql, params):
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        if not header_written:
            writer.writerow(columns)
            header_written = True
        for row in rows:
            writer.writerow([as_csv_value(value) for value in row])
        yield buffer.getvalue().encode("utf-8")
    if not header_written:
        yield b""


async def stream_json(sql: str, params: list) -> AsyncIterator[bytes]:
    first = True
    yield b"["
    async for columns, rows in engine.stream(sql, params):
        for row in rows:
            payload = json.dumps(
                {column: jsonable(value) for column, value in zip(columns, row)},
                ensure_ascii=False,
            )
            yield (payload if first else "," + payload).encode("utf-8")
            first = False
    yield b"]"
