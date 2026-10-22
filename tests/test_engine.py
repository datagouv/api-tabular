import asyncio
import time

import duckdb
import pytest

from tabular_duckdb.config import settings

SLOW_QUERY = "SELECT sum(hash(range)) FROM range(10000000000)"


@pytest.fixture
async def single_worker_engine(parquet_dir, engine_on, monkeypatch):
    """An engine with a single data thread: a query only runs once the previous one freed it."""
    monkeypatch.setattr(settings, "QUERY_WORKERS", 1)
    monkeypatch.setattr(settings, "QUERY_TIMEOUT", 60)
    return await engine_on(parquet_dir)


async def test_a_cancelled_query_frees_its_thread(single_worker_engine):
    """A client going away interrupts its query, instead of holding a thread for QUERY_TIMEOUT."""
    slow = asyncio.create_task(single_worker_engine.query(SLOW_QUERY))
    await asyncio.sleep(0.5)
    slow.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(slow, 5)
    result = await asyncio.wait_for(single_worker_engine.query("SELECT 42 AS n"), 5)
    assert result.rows == [(42,)]


async def test_the_settings_apply_to_the_connection_of_every_query(single_worker_engine):
    """They are set on the instance, while each query runs on a connection of its own."""
    names = [
        "http_timeout",
        "http_retries",
        "autoload_known_extensions",
        "max_expression_depth",
        "enable_external_access",
        "lock_configuration",
    ]
    with single_worker_engine.cursor() as cursor:
        values = cursor.execute(
            "SELECT " + ", ".join(f"current_setting('{name}')" for name in names)
        ).fetchone()
    assert dict(zip(names, values)) == {
        "http_timeout": settings.QUERY_TIMEOUT,
        "http_retries": 0,
        "autoload_known_extensions": False,
        "max_expression_depth": 500,
        "enable_external_access": False,
        "lock_configuration": True,
    }


async def test_a_storage_that_never_answers_does_not_hold_a_thread(
    silent_storage, engine_on, monkeypatch
):
    """Interrupting DuckDB does not cut a blocked HTTP read: httpfs must give up by itself."""
    monkeypatch.setattr(settings, "QUERY_TIMEOUT", 1)
    engine = await engine_on(silent_storage)
    started = time.monotonic()
    with pytest.raises(duckdb.Error):
        await asyncio.wait_for(
            engine.query(f"SELECT * FROM read_parquet('{silent_storage}/x.parquet')"), 15
        )
    assert time.monotonic() - started < 5


async def test_a_storage_failing_the_reads_fails_the_query_at_once(storage, engine_on):
    """httpfs would retry the 503 seven times, with a backoff doubling up to 30 s."""
    engine = await engine_on(f"{storage}/200")
    started = time.monotonic()
    with pytest.raises(duckdb.HTTPException):
        await engine.query(f"DESCRIBE SELECT * FROM read_parquet('{storage}/200/x.parquet')")
    assert time.monotonic() - started < 5


async def test_the_sandbox_lets_httpfs_verify_https_certificates(closed_port, engine_on):
    """httpfs reads the system CA bundle from disk, which the sandbox must keep reachable."""
    base_url = f"https://127.0.0.1:{closed_port}/parquet"
    engine = await engine_on(base_url)
    # nothing listens on the port: the read fails on the connection, not on the sandbox
    with pytest.raises(duckdb.IOException, match="HTTP HEAD"):
        await engine.query(f"SELECT * FROM '{base_url}/resource.parquet'")
