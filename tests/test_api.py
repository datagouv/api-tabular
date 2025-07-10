import json
import pytest

from api_tabular import config
from api_tabular.utils import external_url

from .conftest import (
    PGREST_ENDPOINT,
    RESOURCE_EXCEPTION_PATTERN,
    RESOURCE_ID,
    TABLES_INDEX_PATTERN,
    UNKNOWN_RESOURCE_ID,
)

pytestmark = pytest.mark.asyncio


async def test_api_resource_meta(client, base_url, tables_index_rows):
    res = await client.get(f"{base_url}api/resources/{RESOURCE_ID}/")
    assert res.status == 200
    assert await res.json() == {
        "created_at": tables_index_rows[RESOURCE_ID]["created_at"],
        "url": tables_index_rows[RESOURCE_ID]["url"],
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


async def test_api_resource_meta_not_found(client, base_url):
    res = await client.get(f"{base_url}api/resources/{UNKNOWN_RESOURCE_ID}/")
    assert res.status == 404


async def test_api_resource_profile(client, base_url, tables_index_rows):
    res = await client.get(f"{base_url}api/resources/{RESOURCE_ID}/profile/")
    assert res.status == 200
    assert await res.json() == {
        "profile": json.loads(tables_index_rows[RESOURCE_ID]["csv_detective"]),
        "indexes": None,
    }


async def test_api_resource_profile_not_found(client, base_url):
    res = await client.get(f"{base_url}api/resources/{UNKNOWN_RESOURCE_ID}/profile/")
    assert res.status == 404


async def test_api_resource_data(client, base_url, tables_index_rows):
    detection = json.loads(tables_index_rows[RESOURCE_ID]["csv_detective"])
    res = await client.get(f"{base_url}api/resources/{RESOURCE_ID}/data/")
    assert res.status == 200
    body = await res.json()
    assert all(key in body for key in ["data", "links", "meta"])
    assert isinstance(body["data"], list) and len(body["data"]) == 20
    assert (
        isinstance(body["data"][0], dict)
        and all(
            col in body["data"][0]
            for col in detection["header"]
        )
    )
    assert body["links"] == {
        "next": external_url(f"/api/resources/{RESOURCE_ID}/data/?page=2&page_size=20"),
        "prev": None,
        "profile": external_url(f"/api/resources/{RESOURCE_ID}/profile/"),
        "swagger": external_url(f"/api/resources/{RESOURCE_ID}/swagger/"),
    }
    assert body["meta"] == {"page": 1, "page_size": 20, "total": detection["total_lines"]}


@pytest.mark.parametrize(
    "args",
    [
        {"page": 2},
        {"page_size": 40},
        {"page": 4, "page_size": 5},
    ],
)
async def test_api_resource_data_with_meta_args(client, base_url, tables_index_rows, args):
    detection = json.loads(tables_index_rows[RESOURCE_ID]["csv_detective"])
    params = "&".join(f"{k}={v}" for k, v in args.items())
    res = await client.get(f"{base_url}api/resources/{RESOURCE_ID}/data/?{params}")
    assert res.status == 200
    body = await res.json()
    assert len(body["data"]) == args.get("page_size", config.PAGE_SIZE_DEFAULT)
    assert body["links"] == {
        "next": external_url(
            f"/api/resources/{RESOURCE_ID}/data/"
            f"?page={args.get('page', 1) + 1}"
            f"&page_size={args.get('page_size', config.PAGE_SIZE_DEFAULT)}"
        ),
        "prev": external_url(
            f"/api/resources/{RESOURCE_ID}/data/"
            f"?page={args.get('page', 1) - 1}"
            f"&page_size={args.get('page_size', config.PAGE_SIZE_DEFAULT)}"
        ) if args.get("page", 1) > 1 else None,
        "profile": external_url(f"/api/resources/{RESOURCE_ID}/profile/"),
        "swagger": external_url(f"/api/resources/{RESOURCE_ID}/swagger/"),
    }
    assert body["meta"] == {
        "page": args.get("page", 1),
        "page_size": args.get("page_size", config.PAGE_SIZE_DEFAULT),
        "total": detection["total_lines"],
    }


@pytest.mark.parametrize(
    "filters",
    [
        [("id", "exact", "e5b8864b-d1d0-4452-8d83-517d31da7ff2")],
        [("score", "less", 0.3)],
        [("is_true", "differs", True)],
        [("is_true", "differs", False), ("score", "greater", 0.7)],
    ],
)
async def test_api_resource_data_with_data_args(client, base_url, filters):
    args = "&".join(f"{col}__{comp}={str(val).lower()}" for col, comp, val in filters)
    res = await client.get(f"{base_url}api/resources/{RESOURCE_ID}/data/?{args}")
    assert res.status == 200
    body = await res.json()
    # only checking first page
    for row in body["data"]:
        for col, comp, val in filters:
            if comp == "exact":
                assert row[col] == val
            elif comp == "differs":
                assert row[col] != val
            elif comp == "less":
                assert row[col] <= val
            elif comp == "greater":
                assert row[col] >= val


async def test_api_resource_data_with_args_error(client, base_url):
    args = "TESTCOLUM_NAME__EXACT=BIDULEpage=1"
    res = await client.get(f"{base_url}api/resources/{RESOURCE_ID}/data/?{args}")
    assert res.status == 400
    assert await res.json() == {
        "errors": [
            {
                "code": None,
                "title": "Invalid query string",
                "detail": f"Malformed query: argument '{args}' could not be parsed",
            }
        ]
    }


async def test_api_resource_data_with_page_size_error(client, base_url):
    args = f"page=1&page_size={config.PAGE_SIZE_MAX + 1}"
    res = await client.get(f"{base_url}api/resources/{RESOURCE_ID}/data/?{args}")
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


async def test_api_resource_data_not_found(client, base_url):
    res = await client.get(f"{base_url}api/resources/{UNKNOWN_RESOURCE_ID}/data/")
    assert res.status == 404


async def test_api_with_unsupported_args(client, base_url):
    res = await client.get(f"{base_url}api/resources/{RESOURCE_ID}/data/?limit=1&select=numnum")
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


async def test_api_exception_resource_indexes(setup, fake_client, rmock, mocker):
    # fake exception with indexed columns
    indexed_col = ["col1", "col3"]
    rmock.get(
        RESOURCE_EXCEPTION_PATTERN,
        payload=[{"table_indexes": {c: "index" for c in indexed_col}}],
        repeat=True,
    )

    # checking that we have an `indexes` key in the profile endpoint
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"profile": {"this": "is-a-profile"}}])
    res = await fake_client.get(f"/api/resources/{RESOURCE_ID}/profile/")
    assert res.status == 200
    content = await res.json()
    assert content["profile"] == {"this": "is-a-profile"}
    # sorted because it's made from a set so the order might not be preserved
    assert indexed_col == list(sorted(content["indexes"]))

    # checking that the resource is readable with no filter
    table = "xxx"
    rmock.get(
        TABLES_INDEX_PATTERN,
        payload=[{"__id": 1, "id": "test-id", "parsing_table": table}],
        repeat=True,
    )
    rmock.get(
        f"{PGREST_ENDPOINT}/{table}?limit=1&order=__id.asc",
        status=200,
        payload=[{"col1": 1, "col2": 2, "col3": 3, "col4": 4}],
        headers={"Content-Range": "0-2/2"},
    )
    res = await fake_client.get(f"/api/resources/{RESOURCE_ID}/data/?page=1&page_size=1")
    assert res.status == 200

    # checking that the resource can be filtered on an indexed column
    rmock.get(
        f'{PGREST_ENDPOINT}/{table}?"{indexed_col[0]}"=gte.1&limit=1&order=__id.asc',
        status=200,
        payload=[{"col1": 1, "col2": 2, "col3": 3, "col4": 4}],
        headers={"Content-Range": "0-2/2"},
    )
    res = await fake_client.get(
        f"/api/resources/{RESOURCE_ID}/data/?{indexed_col[0]}__greater=1&page=1&page_size=1"
    )
    assert res.status == 200

    # checking that the resource cannot be filtered on a non-indexed column
    non_indexed_col = "col2"
    # postgrest would return a content
    rmock.get(
        f'{PGREST_ENDPOINT}/{table}?"{non_indexed_col}"=gte.1&limit=1&order=__id.asc',
        status=200,
        payload=[{"col1": 1, "col2": 2, "col3": 3, "col4": 4}],
        headers={"Content-Range": "0-2/2"},
    )
    res = await fake_client.get(
        f"/api/resources/{RESOURCE_ID}/data/?{non_indexed_col}__greater=1&page=1&page_size=1"
    )
    # but it's forbidden
    assert res.status == 403

    # if aggregation is allowed:
    mocker.patch("api_tabular.config.ALLOW_AGGREGATION", [RESOURCE_ID])

    # checking that it's not possible on a non-indexed column
    # postgrest would return a content
    rmock.get(
        f'{PGREST_ENDPOINT}/{table}?select="{non_indexed_col}__avg":"{non_indexed_col}".avg()&limit=1',
        status=200,
        payload=[{f"{non_indexed_col}__avg": 2}],
        headers={"Content-Range": "0-2/2"},
    )
    res = await fake_client.get(
        f"/api/resources/{RESOURCE_ID}/data/?{non_indexed_col}__avg&page=1&page_size=1"
    )
    # but it's forbidden
    assert res.status == 403

    # checking that it is possible on an indexed column
    rmock.get(
        f'{PGREST_ENDPOINT}/{table}?select="{indexed_col[0]}__avg":"{indexed_col[0]}".avg()&limit=1',
        status=200,
        payload=[{f"{indexed_col[0]}__avg": 2}],
        headers={"Content-Range": "0-2/2"},
    )
    res = await fake_client.get(
        f"/api/resources/{RESOURCE_ID}/data/?{indexed_col[0]}__avg&page=1&page_size=1"
    )
    assert res.status == 200


async def test_api_exception_resource_no_indexes(setup, fake_client, rmock, mocker):
    # fake exception with no indexed column
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"profile": {"this": "is-a-profile"}}])
    rmock.get(
        RESOURCE_EXCEPTION_PATTERN,
        payload=[{"table_indexes": {}}],
        repeat=True,
    )

    # checking that we have an `indexes` key in the profile endpoint
    res = await fake_client.get(f"/api/resources/{RESOURCE_ID}/profile/")
    assert res.status == 200
    content = await res.json()
    assert content["profile"] == {"this": "is-a-profile"}
    assert content["indexes"] is None

    data = [{f"col{k}": k for k in range(1, 5)}]
    # checking that the resource is readable with no filter
    table = "xxx"
    rmock.get(
        TABLES_INDEX_PATTERN,
        payload=[{"__id": 1, "id": "test-id", "parsing_table": table}],
        repeat=True,
    )
    rmock.get(
        f"{PGREST_ENDPOINT}/{table}?limit=1&order=__id.asc",
        status=200,
        payload=data,
        headers={"Content-Range": "0-2/2"},
    )
    res = await fake_client.get(f"/api/resources/{RESOURCE_ID}/data/?page=1&page_size=1")
    assert res.status == 200

    # checking that the resource can be filtered on all columns
    for k in range(1, 5):
        rmock.get(
            f'{PGREST_ENDPOINT}/{table}?"col{k}"=gte.1&limit=1&order=__id.asc',
            status=200,
            payload=data,
            headers={"Content-Range": "0-2/2"},
        )
        res = await fake_client.get(
            f"/api/resources/{RESOURCE_ID}/data/?col{k}__greater=1&page=1&page_size=1"
        )
        assert res.status == 200

    # if aggregation is allowed:
    mocker.patch("api_tabular.config.ALLOW_AGGREGATION", [RESOURCE_ID])
    # checking that aggregation is allowed on all columns
    for k in range(1, 5):
        rmock.get(
            f'{PGREST_ENDPOINT}/{table}?select="col{k}__avg":"col{k}".avg()&limit=1',
            status=200,
            payload=[{"col2__avg": 2}],
            headers={"Content-Range": "0-2/2"},
        )
        res = await fake_client.get(f"/api/resources/{RESOURCE_ID}/data/?col{k}__avg&page=1&page_size=1")
        assert res.status == 200


@pytest.mark.parametrize(
    "params",
    [
        (503, 503, ["errors"]),
        (200, 200, ["status", "version", "uptime_seconds"]),
    ],
)
async def test_health(setup, fake_client, rmock, params):
    postgrest_resp_code, api_expected_resp_code, expected_keys = params
    rmock.head(
        f"{PGREST_ENDPOINT}/migrations_csv",
        status=postgrest_resp_code,
    )
    res = await fake_client.get("/health/")
    assert res.status == api_expected_resp_code
    res_json = await res.json()
    assert all(key in res_json for key in expected_keys)
