import asyncio

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
