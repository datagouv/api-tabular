import pytest

from .conftest import RESOURCE_ID

pytestmark = pytest.mark.asyncio


async def test_api_resource_meta(client):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/")
    assert res.status == 200
