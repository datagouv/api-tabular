import pytest

from api_tabular import config
from api_tabular.utils import external_url

from .conftest import DATE, PGREST_ENDPOINT, RESOURCE_ID, TABLES_INDEX_PATTERN

pytestmark = pytest.mark.asyncio


async def test_api_resource_meta(client, rmock):
    rmock.get(
        TABLES_INDEX_PATTERN,
        payload=[
            {
                "created_at": DATE,
                "url": "https://example.com",
            }
        ],
    )
    res = await client.get(f"/api/resources/{RESOURCE_ID}/")
    assert res.status == 200
    assert await res.json() == {
        "created_at": DATE,
        "url": "https://example.com",
        "links": [
            {
                "href": external_url(f"/api/resources/{RESOURCE_ID}/profile/"),
                "type": "GET",
                "rel": "profile",
            },
            {
                "href": external_url(f"/api/resources/{RESOURCE_ID}/data/"),
                "type": "GET",
                "rel": "data",
            },
            {
                "href": external_url(f"/api/resources/{RESOURCE_ID}/swagger/"),
                "type": "GET",
                "rel": "swagger",
            },
        ],
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
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
    rmock.get(
        f"{PGREST_ENDPOINT}/xxx?limit=20&order=__id.asc",
        payload={"such": "data"},
        headers={"Content-Range": "0-10/10"},
    )
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert res.status == 200
    body = {
        "data": {"such": "data"},
        "links": {
            "next": None,
            "prev": None,
            "profile": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/"),
            "swagger": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/"),
        },
        "meta": {"page": 1, "page_size": 20, "total": 10},
    }
    assert await res.json() == body


async def test_api_resource_data_with_args(client, rmock):
    args = "page=1"
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
    rmock.get(
        f"{PGREST_ENDPOINT}/xxx?limit=20&order=__id.asc",
        payload={"such": "data"},
        headers={"Content-Range": "0-10/10"},
    )
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
    assert res.status == 200
    body = {
        "data": {"such": "data"},
        "links": {
            "next": None,
            "prev": None,
            "profile": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/"),
            "swagger": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/"),
        },
        "meta": {"page": 1, "page_size": 20, "total": 10},
    }
    assert await res.json() == body


async def test_api_resource_data_with_args_case(client, rmock):
    args = "COLUM_NAME__EXACT=BIDULE&page=1"
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
    rmock.get(
        f'{PGREST_ENDPOINT}/xxx?"COLUM_NAME"=eq.BIDULE&limit=20&order=__id.asc',
        payload={"such": "data"},
        headers={"Content-Range": "0-10/10"},
    )
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
    assert res.status == 200
    body = {
        "data": {"such": "data"},
        "links": {
            "next": None,
            "prev": None,
            "profile": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/"),
            "swagger": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/"),
        },
        "meta": {"page": 1, "page_size": 20, "total": 10},
    }
    assert await res.json() == body


async def test_api_resource_data_with_args_error(client, rmock):
    args = "TESTCOLUM_NAME__EXACT=BIDULEpage=1"
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
    assert res.status == 400
    assert await res.json() == {
        "errors": [
            {
                "code": None,
                "title": "Invalid query string",
                "detail": "Malformed query: argument 'TESTCOLUM_NAME__EXACT=BIDULEpage=1' could not be parsed",
            }
        ]
    }


async def test_api_resource_data_with_page_size_error(client, rmock):
    args = "page=1&page_size=100"
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
    assert res.status == 400
    assert await res.json() == {
        "errors": [
            {
                "code": None,
                "detail": f"Page size exceeds allowed maximum: {config.PAGE_SIZE_MAX}",
                "title": "Invalid query string",
            }
        ]
    }


async def test_api_resource_data_not_found(client, mock_get_resource_empty):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert res.status == 404


async def test_api_resource_data_table_error(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
    rmock.get(
        f"{PGREST_ENDPOINT}/xxx?limit=20&order=__id.asc", status=502, payload={"such": "error"}
    )
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert res.status == 502
    assert await res.json() == {
        "errors": [{"code": None, "detail": {"such": "error"}, "title": "Database error"}]
    }


async def test_api_percent_encoding_arabic(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
    rmock.get(
        f'{PGREST_ENDPOINT}/xxx?"%D9%85%D9%88%D8%A7%D8%B1%D8%AF"=eq.%D9%85%D9%88%D8%A7%D8%B1%D8%AF&limit=20&order=__id.asc',  # noqa
        status=200,
        payload={"such": "data"},
        headers={"Content-Range": "0-10/10"},
    )
    res = await client.get(
        f"/api/resources/{RESOURCE_ID}/data/?%D9%85%D9%88%D8%A7%D8%B1%D8%AF__exact=%D9%85%D9%88%D8%A7%D8%B1%D8%AF"
    )
    assert res.status == 200
    body = {
        "data": {"such": "data"},
        "links": {
            "next": None,
            "prev": None,
            "profile": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/"),
            "swagger": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/"),
        },
        "meta": {"page": 1, "page_size": 20, "total": 10},
    }
    assert await res.json() == body


async def test_api_with_unsupported_args(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
    rmock.get(
        f"{PGREST_ENDPOINT}/xxx?limit=20&order=__id.asc",
        status=200,
        payload={"such": "data"},
        headers={"Content-Range": "0-10/10"},
    )
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?limit=1&select=numnum")
    assert res.status == 400
    body = {
        "errors": [
            {
                "code": None,
                "title": "Invalid query string",
                "detail": "Malformed query: argument 'limit=1' could not be parsed",
            },
        ],
    }
    assert await res.json() == body


async def test_api_pagination(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
    rmock.get(
        f"{PGREST_ENDPOINT}/xxx?limit=1&order=__id.asc",
        status=200,
        payload=[{"such": "data"}],
        headers={"Content-Range": "0-2/2"},
    )
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?page=1&page_size=1")
    assert res.status == 200
    body = {
        "data": [{"such": "data"}],
        "links": {
            "next": external_url(
                "/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?page=2&page_size=1"
            ),
            "prev": None,
            "profile": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/"),
            "swagger": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/"),
        },
        "meta": {"page": 1, "page_size": 1, "total": 2},
    }
    assert await res.json() == body

    rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
    rmock.get(
        f"{PGREST_ENDPOINT}/xxx?limit=1&offset=1&order=__id.asc",
        status=200,
        payload=[{"such": "data"}],
        headers={"Content-Range": "0-2/2"},
    )
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?page=2&page_size=1")
    assert res.status == 200
    body = {
        "data": [{"such": "data"}],
        "links": {
            "next": None,
            "prev": external_url(
                "/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?page=1&page_size=1"
            ),
            "profile": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/"),
            "swagger": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/"),
        },
        "meta": {"page": 2, "page_size": 1, "total": 2},
    }
    assert await res.json() == body


@pytest.mark.parametrize(
    "params",
    [
        (503, 503, ["errors"]),
        (200, 200, ["status", "version", "uptime_seconds"]),
    ],
)
async def test_health(client, rmock, params):
    postgrest_resp_code, api_expected_resp_code, expected_keys = params
    rmock.head(
        f"{PGREST_ENDPOINT}/migrations_csv",
        status=postgrest_resp_code,
    )
    res = await client.get("/health/")
    assert res.status == api_expected_resp_code
    res_json = await res.json()
    assert all(key in res_json for key in expected_keys)
