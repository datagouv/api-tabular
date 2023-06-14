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
    rmock.get(f"{PG_RST_URL}/xxx?limit=20", payload={"such": "data"}, headers={"Content-Range": "0-10/10"})
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert res.status == 200
    body = {
        'data': {"such": "data"},
        'links': {},
        'meta': {'page': 1, 'page_size': 20, 'total': 10}
    }
    assert await res.json() == body


async def test_api_resource_data_with_args(client, rmock):
    args = "page=1"
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    rmock.get(f"{PG_RST_URL}/xxx?limit=20", payload={"such": "data"}, headers={"Content-Range": "0-10/10"})
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
    assert res.status == 200
    body = {
        'data': {"such": "data"},
        'links': {},
        'meta': {'page': 1, 'page_size': 20, 'total': 10}
    }
    assert await res.json() == body


async def test_api_resource_data_with_args_case(client, rmock):
    args = "COLUM_NAME__EXACT=BIDULE&page=1"
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    rmock.get(
        f"{PG_RST_URL}/xxx?colum_name=eq.BIDULE&limit=20",
        payload={"such": "data"}, headers={"Content-Range": "0-10/10"})
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
    assert res.status == 200
    body = {
        'data': {"such": "data"},
        'links': {},
        'meta': {'page': 1, 'page_size': 20, 'total': 10}
    }
    assert await res.json() == body


async def test_api_resource_data_with_args_error(client, rmock):
    args = "TESTCOLUM_NAME__EXACT=BIDULEpage=1"
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
    assert res.status == 400
    assert await res.json() == {'code': 400, 'detail': 'Malformed query', 'title': 'Invalid query string'}


async def test_api_resource_data_not_found(client, mock_get_resource_empty):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert res.status == 404


async def test_api_resource_data_table_error(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    rmock.get(f"{PG_RST_URL}/xxx?limit=20", status=502, payload={"such": "error"})
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert res.status == 502
    assert await res.json() == {'code': 502, 'detail': {'such': 'error'}, 'title': 'Database error'}


async def test_api_percent_encoding_arabic(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    rmock.get(
        f"{PG_RST_URL}/xxx?%D9%85%D9%88%D8%A7%D8%B1%D8%AF=eq.%D9%85%D9%88%D8%A7%D8%B1%D8%AF&limit=20",
        status=200, payload={"such": "data"}, headers={"Content-Range": "0-10/10"})
    res = await client.get(
        f"/api/resources/{RESOURCE_ID}/data/?%D9%85%D9%88%D8%A7%D8%B1%D8%AF__exact=%D9%85%D9%88%D8%A7%D8%B1%D8%AF")
    assert res.status == 200
    body = {
        'data': {"such": "data"},
        'links': {},
        'meta': {'page': 1, 'page_size': 20, 'total': 10}
    }
    assert await res.json() == body


async def test_api_percent_encoding_latin(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    rmock.get(
        f"{PG_RST_URL}/xxx?c_est_déjà_l_été=eq.BIDULE&limit=20",
        status=200, payload={"such": "data"}, headers={"Content-Range": "0-10/10"})
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?C\'est déjà l\'été.__exact=BIDULE")
    assert res.status == 200
    body = {
        'data': {"such": "data"},
        'links': {},
        'meta': {'page': 1, 'page_size': 20, 'total': 10}
    }
    assert await res.json() == body


async def test_api_percent_encoding_cyrillic(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    rmock.get(
        f"{PG_RST_URL}/xxx?компьютер=eq.Компьютер&limit=20",
        status=200, payload={"such": "data"}, headers={"Content-Range": "0-10/10"})
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?Компьютер__exact=Компьютер")
    assert res.status == 200
    body = {
        'data': {"such": "data"},
        'links': {},
        'meta': {'page': 1, 'page_size': 20, 'total': 10}
    }
    assert await res.json() == body


async def test_api_with_unsupported_args(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"parsing_table": "xxx"}])
    rmock.get(
        f"{PG_RST_URL}/xxx?limit=20",
        status=200, payload={"such": "data"}, headers={"Content-Range": "0-10/10"})
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?limit=1&select=numnum")
    assert res.status == 200
    body = {
        'data': {"such": "data"},
        'links': {},
        'meta': {'page': 1, 'page_size': 20, 'total': 10}
    }
    assert await res.json() == body
