import asyncio
import time

import pytest

from tabular_duckdb.config import settings
from tabular_duckdb.errors import QueryException
from tabular_duckdb.resources import Resource, ResourceCache

SLOW_RESOURCE_ID = "aaaaaaaa-0000-0000-0000-000000000001"
FAST_RESOURCE_ID = "aaaaaaaa-0000-0000-0000-000000000002"
OTHER_RESOURCE_ID = "aaaaaaaa-0000-0000-0000-000000000003"


@pytest.fixture
def cache(monkeypatch):
    """A cache whose loads of SLOW_RESOURCE_ID hang until `release` is set."""
    cache = ResourceCache()
    cache.release = asyncio.Event()
    cache.loads = []

    async def load(resource_id: str) -> Resource:
        cache.loads.append(resource_id)
        if resource_id == SLOW_RESOURCE_ID:
            await cache.release.wait()
        return Resource(resource_id=resource_id, created_at=None)

    monkeypatch.setattr(cache, "_load", load)
    return cache


async def test_slow_load_does_not_block_other_resources(cache):
    slow = asyncio.create_task(cache.get(SLOW_RESOURCE_ID))
    await asyncio.sleep(0)
    fast = await asyncio.wait_for(cache.get(FAST_RESOURCE_ID), timeout=1)
    assert fast.resource_id == FAST_RESOURCE_ID
    cache.release.set()
    assert (await slow).resource_id == SLOW_RESOURCE_ID


async def test_a_client_going_away_does_not_cancel_the_load_of_the_others(cache):
    first = asyncio.create_task(cache.get(SLOW_RESOURCE_ID))
    second = asyncio.create_task(cache.get(SLOW_RESOURCE_ID))
    await asyncio.sleep(0)
    first.cancel()
    await asyncio.sleep(0)
    cache.release.set()
    assert (await second).resource_id == SLOW_RESOURCE_ID
    assert cache.loads == [SLOW_RESOURCE_ID]


async def test_concurrent_requests_load_a_resource_once(cache):
    first = asyncio.create_task(cache.get(SLOW_RESOURCE_ID))
    second = asyncio.create_task(cache.get(SLOW_RESOURCE_ID))
    await asyncio.sleep(0)
    cache.release.set()
    assert await first is await second
    assert cache.loads == [SLOW_RESOURCE_ID]


async def test_concurrent_requests_share_a_failing_load(monkeypatch):
    """Each one would otherwise wait for its own failure, one after the other."""
    cache = ResourceCache()
    loads = []

    async def failing_load(resource_id: str) -> Resource:
        loads.append(resource_id)
        await asyncio.sleep(0.2)
        raise QueryException(503, "Storage unavailable", "down")

    monkeypatch.setattr(cache, "_load", failing_load)
    started = time.monotonic()
    results = await asyncio.gather(
        *(cache.get(FAST_RESOURCE_ID) for _ in range(3)), return_exceptions=True
    )
    assert all(isinstance(result, QueryException) for result in results)
    assert loads == [FAST_RESOURCE_ID]
    assert time.monotonic() - started < 0.4


@pytest.mark.parametrize("ttl, loads", [(0, 2), (300, 1)])
async def test_cached_resource_expires_after_the_ttl(cache, monkeypatch, ttl, loads):
    monkeypatch.setattr(settings, "RESOURCE_CACHE_TTL", ttl)
    await cache.get(FAST_RESOURCE_ID)
    await cache.get(FAST_RESOURCE_ID)
    assert cache.loads == [FAST_RESOURCE_ID] * loads


async def test_expired_resources_do_not_pile_up(cache, monkeypatch):
    """Every resource ever requested would otherwise stay in memory, inspection included."""
    monkeypatch.setattr(settings, "RESOURCE_CACHE_TTL", 0)
    await cache.get(FAST_RESOURCE_ID)
    await cache.get(OTHER_RESOURCE_ID)
    assert list(cache._entries) == [OTHER_RESOURCE_ID]
