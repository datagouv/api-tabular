import asyncio
import socket

import duckdb
import pytest

from tabular_duckdb.config import settings
from tabular_duckdb.engine import DuckDBEngine

SLOW_QUERY = "SELECT sum(hash(range)) FROM range(10000000000)"


@pytest.fixture
async def single_worker_engine(parquet_dir, monkeypatch):
    """An engine with a single data thread: a query only runs once the previous one freed it."""
    monkeypatch.setattr(settings, "PARQUET_BASE_URL", parquet_dir)
    monkeypatch.setattr(settings, "QUERY_WORKERS", 1)
    monkeypatch.setattr(settings, "QUERY_TIMEOUT", 60)
    monkeypatch.setattr(settings, "SQL_POOL_SIZE", 1)
    engine = DuckDBEngine()
    await engine.start()
    yield engine
    await engine.stop()


async def test_a_cancelled_query_frees_its_thread(single_worker_engine):
    """A client going away interrupts its query, instead of holding a thread for QUERY_TIMEOUT."""
    slow = asyncio.create_task(single_worker_engine.query(SLOW_QUERY))
    await asyncio.sleep(0.5)
    slow.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(slow, 5)
    result = await asyncio.wait_for(single_worker_engine.query("SELECT 42 AS n"), 5)
    assert result.rows == [(42,)]


async def test_a_broken_sql_connection_is_replaced(single_worker_engine):
    async with single_worker_engine.sql_connection() as conn:
        conn.close()
    async with single_worker_engine.sql_connection() as conn:
        assert conn.execute("SELECT 42").fetchall() == [(42,)]


async def test_the_sandbox_lets_httpfs_verify_https_certificates(monkeypatch):
    """httpfs reads the system CA bundle from disk, which the sandbox must keep reachable."""
    with socket.create_server(("127.0.0.1", 0)) as server:
        port = server.getsockname()[1]
    base_url = f"https://127.0.0.1:{port}/parquet"
    monkeypatch.setattr(settings, "PARQUET_BASE_URL", base_url)
    engine = DuckDBEngine()
    await engine.start()
    try:
        # nothing listens on the port: the read fails on the connection, not on the sandbox
        with pytest.raises(duckdb.IOException, match="HTTP HEAD"):
            await engine.query(f"SELECT * FROM '{base_url}/resource.parquet'")
    finally:
        await engine.stop()
