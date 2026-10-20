import asyncio

import pytest

from tabular_duckdb.resources import Resource, ResourceCache

SLOW_RESOURCE_ID = "aaaaaaaa-0000-0000-0000-000000000001"
FAST_RESOURCE_ID = "aaaaaaaa-0000-0000-0000-000000000002"


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
        return Resource(resource_id=resource_id, created_at=None, total_lines=0)

    monkeypatch.setattr(cache, "_load", load)
    return cache


async def test_slow_load_does_not_block_other_resources(cache):
    slow = asyncio.create_task(cache.get(SLOW_RESOURCE_ID))
    await asyncio.sleep(0)
    fast = await asyncio.wait_for(cache.get(FAST_RESOURCE_ID), timeout=1)
    assert fast.resource_id == FAST_RESOURCE_ID
    cache.release.set()
    assert (await slow).resource_id == SLOW_RESOURCE_ID


async def test_concurrent_requests_load_a_resource_once(cache):
    first = asyncio.create_task(cache.get(SLOW_RESOURCE_ID))
    second = asyncio.create_task(cache.get(SLOW_RESOURCE_ID))
    await asyncio.sleep(0)
    cache.release.set()
    assert await first is await second
    assert cache.loads == [SLOW_RESOURCE_ID]
