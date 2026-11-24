import asyncio
import gc
import os
import time
from email.utils import formatdate
from pathlib import Path

import duckdb
import pytest

from tabular_duckdb.config import settings

from .conftest import SLOW_QUERY, ParquetFilesHandler, serve


class DatedParquetFilesHandler(ParquetFilesHandler):
    """Dates the files by their mtime, like the bucket dates an object by its upload."""

    def last_modified(self, path: Path) -> str:
        return formatdate(path.stat().st_mtime, usegmt=True)


@pytest.fixture
async def single_worker_engine(start_engine, monkeypatch):
    """An engine with a single data thread: a query only runs once the previous one freed it."""
    monkeypatch.setattr(settings, "QUERY_WORKERS", 1)
    monkeypatch.setattr(settings, "QUERY_TIMEOUT_SECONDS", 60)
    return await start_engine()


async def test_a_cancelled_query_frees_its_thread(single_worker_engine):
    """A client going away interrupts its query, instead of holding a thread until the timeout."""
    slow = asyncio.create_task(single_worker_engine.query(SLOW_QUERY))
    await asyncio.sleep(0.5)
    slow.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(slow, 5)
    result = await asyncio.wait_for(single_worker_engine.query("SELECT 42 AS n"), 5)
    assert result.rows == [(42,)]


async def test_a_cancelled_query_logs_no_error(single_worker_engine, caplog):
    """The interrupted query fails in its thread: an unread failure is logged as an error by
    asyncio, which Sentry reports, for every client going away."""
    slow = asyncio.create_task(single_worker_engine.query(SLOW_QUERY))
    await asyncio.sleep(0.5)
    slow.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(slow, 5)
    del slow
    gc.collect()
    await asyncio.sleep(0)
    assert "never retrieved" not in caplog.text


async def test_the_settings_apply_to_the_connection_of_every_query(single_worker_engine):
    """They are set on the instance, while each query runs on a connection of its own."""
    names = ["http_timeout", "http_retries", "max_expression_depth", "TimeZone", "temp_directory"]
    with single_worker_engine.cursor() as cursor:
        values = cursor.execute(
            "SELECT " + ", ".join(f"current_setting('{name}')" for name in names)
        ).fetchone()
    assert dict(zip(names, values)) == {
        "http_timeout": settings.QUERY_TIMEOUT_SECONDS,
        "http_retries": 0,
        "max_expression_depth": 500,
        "TimeZone": "UTC",
        "temp_directory": settings.DUCKDB_TEMP_DIRECTORY,
    }


async def test_a_storage_that_never_answers_does_not_hold_a_thread(
    silent_storage, start_engine, monkeypatch
):
    """Interrupting DuckDB does not cut a blocked HTTP read: httpfs must give up by itself."""
    monkeypatch.setattr(settings, "QUERY_TIMEOUT_SECONDS", 1)
    engine = await start_engine()
    started = time.monotonic()
    with pytest.raises(duckdb.Error):
        await asyncio.wait_for(
            engine.query(f"SELECT * FROM read_parquet('{silent_storage}/x.parquet')"), 15
        )
    assert time.monotonic() - started < 5


async def test_a_parquet_file_replaced_in_the_bucket_is_read_again(start_engine, tmp_path):
    """The footers are cached: a replaced file must not be read through the old one."""
    path = tmp_path / "r.parquet"

    def upload(rows: int, uploaded_at: int) -> None:
        with duckdb.connect() as conn:
            conn.execute(
                f"COPY (SELECT range AS a FROM range({rows})) TO '{path}' (FORMAT parquet)"
            )
        os.utime(path, (uploaded_at, uploaded_at))

    engine = await start_engine()
    handler = type("Handler", (DatedParquetFilesHandler,), {"directory": tmp_path})
    with serve(handler) as base_url:
        count = f"SELECT count(*) FROM read_parquet('{base_url}/r.parquet')"
        upload(10, 1_700_000_000)
        assert await engine.query_value(count) == 10
        upload(25, 1_700_000_060)
        assert await engine.query_value(count) == 25


async def test_a_storage_failing_the_reads_fails_the_query_at_once(storage, start_engine):
    """httpfs would retry the 503 seven times, with a backoff doubling up to 30 s."""
    engine = await start_engine()
    started = time.monotonic()
    with pytest.raises(duckdb.HTTPException):
        await engine.query(f"DESCRIBE SELECT * FROM read_parquet('{storage}/200/x.parquet')")
    assert time.monotonic() - started < 5
