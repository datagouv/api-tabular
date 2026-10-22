import asyncio

import pytest
from starlette.exceptions import HTTPException

from tabular_duckdb.config import settings
from tabular_duckdb.errors import QueryException
from tabular_duckdb.resources import Resource, ResourceCache, _created_at, head

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


async def test_concurrent_requests_load_a_resource_once(cache):
    first = asyncio.create_task(cache.get(SLOW_RESOURCE_ID))
    second = asyncio.create_task(cache.get(SLOW_RESOURCE_ID))
    await asyncio.sleep(0)
    cache.release.set()
    assert await first is await second
    assert cache.loads == [SLOW_RESOURCE_ID]


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


@pytest.mark.parametrize("status", [403, 404])
def test_head_of_a_missing_object_is_a_404(storage, status):
    """A bucket that does not grant listing answers 403 for a missing object."""
    with pytest.raises(HTTPException) as raised:
        head(f"{storage}/{status}/x.parquet")
    assert raised.value.status_code == 404


def test_head_with_an_unexpected_status(storage):
    with pytest.raises(QueryException) as raised:
        head(f"{storage}/500/x.parquet")
    assert raised.value.status == 502
    assert raised.value.title == "Storage error"


def test_head_of_an_unreachable_storage():
    with pytest.raises(QueryException) as raised:
        head("http://127.0.0.1:1/x.parquet")
    assert raised.value.status == 503
    assert raised.value.title == "Storage unavailable"


def test_head_of_a_storage_that_never_answers(silent_storage, monkeypatch):
    monkeypatch.setattr(settings, "HTTP_TIMEOUT", 1)
    with pytest.raises(QueryException) as raised:
        head(f"{silent_storage}/x.parquet")
    assert raised.value.status == 503
    assert raised.value.title == "Storage unavailable"


def test_created_at_is_the_last_modification_of_the_object(storage, monkeypatch):
    monkeypatch.setattr(settings, "PARQUET_BASE_URL", f"{storage}/200")
    assert _created_at(settings.object_url(FAST_RESOURCE_ID)) == "2024-01-01T12:00:00+00:00"
