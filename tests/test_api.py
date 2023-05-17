import pytest

from .conftest import RESOURCE_ID, DATE, PG_RST_URL, TABLES_INDEX_PATTERN

pytestmark = pytest.mark.asyncio


async def test_api_resource_meta(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{
        "created_at": DATE,
        "url": "https://example.com",
    }])
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
    res = await client.get(f"/api/resources/{RESOURCE_ID}/")
    assert res.status == 404


async def test_api_resource_profile(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"profile": {"this": "is-a-profile"}}])
    res = await client.get(f"/api/resources/{RESOURCE_ID}/profile/")
    assert res.status == 200
    assert await res.json() == {"profile": {"this": "is-a-profile"}}


async def test_api_resource_profile_not_found(client, mock_get_resource_empty):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/profile/")
    assert res.status == 404


async def test_api_resource_data(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    rmock.get(f"{PG_RST_URL}/xxx?limit=50", payload={"such": "data"})
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert res.status == 200
    assert await res.json() == {"such": "data"}


async def test_api_resource_data_with_args(client, rmock):
    args = "page=1"
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    rmock.get(f"{PG_RST_URL}/xxx?limit=50", payload={"such": "data"})
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
    assert res.status == 200
    assert await res.json() == {"such": "data"}


async def test_api_resource_data_with_args_case(client, rmock):
    args = "COLUM_NAME__EXACT=BIDULE&page=1"
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    rmock.get(f"{PG_RST_URL}/xxx?colum_name=eq.BIDULE&limit=50", payload={"such": "data"})
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
    assert res.status == 200
    assert await res.json() == {"such": "data"}


async def test_api_resource_data_with_args_error(client, rmock):
    args = "TESTCOLUM_NAME__EXACT=BIDULEpage=1"
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
    assert res.status == 400
    assert await res.json() == "Invalid query string"


async def test_api_resource_data_not_found(client, mock_get_resource_empty):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert res.status == 404


async def test_api_resource_data_table_error(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    rmock.get(f"{PG_RST_URL}/xxx?limit=50", status=502, payload={"such": "error"})
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert res.status == 502
    assert await res.json() == {"such": "error"}
