import pytest

from .conftest import RESOURCE_ID, DATE

pytestmark = pytest.mark.asyncio


async def test_api_resource_meta(client, mock_get_resource):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/")
    assert res.status == 200
    assert await res.json() == {
        "created_at": DATE,
        "url": "https://example.com",
        "links": [
            {
                "href": f"/api/resources/{RESOURCE_ID}/profile/",
                "type": "GET",
                "rel": "profile"
            },
            {
                "href": f"/api/resources/{RESOURCE_ID}/data/",
                "type": "GET",
                "rel": "data"
            }
        ]
    }


async def test_api_resource_meta_not_found(client, mock_get_resource_empty):
    res = await client.get("/api/resources/not-a-resource-id/")
    assert res.status == 404


async def test_api_resource_profile(client, mock_get_resource_profile):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/profile/")
    assert res.status == 200
    assert await res.json() == {"profile": {"this": "is-a-profile"}}


async def test_api_resource_profile_not_found(client, mock_get_resource_empty):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/profile/")
    assert res.status == 404


async def test_api_resource_data(client, mock_get_resource_data):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert res.status == 200
    assert await res.json() == {"such": "data"}


async def test_api_resource_data_not_found(client, mock_get_resource_empty):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert res.status == 404


async def test_api_resource_data_table_not_found(client):
    pass
