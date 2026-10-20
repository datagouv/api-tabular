import csv
import io
import json
from collections.abc import AsyncIterator

from tabular_duckdb.config import settings
from tabular_duckdb.engine import engine
from tabular_duckdb.errors import QueryException
from tabular_duckdb.serialization import as_csv_value, rows_to_json


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


async def stream_json(sql: str, params: list) -> AsyncIterator[bytes]:
    separator = ""
    yield b"["
    async for columns, rows in engine.stream(sql, params):
        chunk = ",".join(json.dumps(row, ensure_ascii=False) for row in rows_to_json(columns, rows))
        yield (separator + chunk).encode("utf-8")
        separator = ","
    yield b"]\n"
