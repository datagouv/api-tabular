import csv
import json
from io import StringIO

import pytest

from api_tabular import config

from .conftest import (
    AGG_ALLOWED_INDEXED_RESOURCE_ID,
    AGG_ALLOWED_RESOURCE_ID,
    DELETED_RESOURCE_ID,
    INDEXED_RESOURCE_ID,
    NULL_VALUES_RESOURCE_ID,
    RESOURCE_ID,
    UNKNOWN_RESOURCE_ID,
)

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "_resource_id",
    [
        AGG_ALLOWED_INDEXED_RESOURCE_ID,
        AGG_ALLOWED_RESOURCE_ID,
        INDEXED_RESOURCE_ID,
        RESOURCE_ID,
    ],
)
async def test_api_resource_meta(client, base_url, tables_index_rows, _resource_id):
    res = await client.get(f"/api/resources/{_resource_id}/")
    assert res.status == 200
    assert await res.json() == {
        "created_at": tables_index_rows[_resource_id]["created_at"],
        "url": tables_index_rows[_resource_id]["url"],
        "links": [
            {
                "href": f"{base_url}/api/resources/{_resource_id}/profile/",
                "type": "GET",
                "rel": "profile",
            },
            {
                "href": f"{base_url}/api/resources/{_resource_id}/data/",
                "type": "GET",
                "rel": "data",
            },
            {
                "href": f"{base_url}/api/resources/{_resource_id}/swagger/",
                "type": "GET",
                "rel": "swagger",
            },
        ],
    }


async def test_api_resource_meta_deleted(client):
    """Test that deleted resources return 410 Gone for metadata endpoint"""
    res = await client.get(f"api/resources/{DELETED_RESOURCE_ID}/")
    assert res.status == 410
    text = await res.text()
    assert "permanently deleted" in text
    assert DELETED_RESOURCE_ID in text


async def test_api_resource_meta_not_found(client):
    res = await client.get(f"/api/resources/{UNKNOWN_RESOURCE_ID}/")
    assert res.status == 404


@pytest.mark.parametrize(
    "_resource_id",
    [
        AGG_ALLOWED_INDEXED_RESOURCE_ID,
        AGG_ALLOWED_RESOURCE_ID,
        INDEXED_RESOURCE_ID,
        RESOURCE_ID,
    ],
)
async def test_api_resource_profile(client, tables_index_rows, exceptions_rows, _resource_id):
    indexes = (
        list(json.loads(exceptions_rows[_resource_id]["table_indexes"]).keys())
        if _resource_id in exceptions_rows
        else None
    )
    res = await client.get(f"/api/resources/{_resource_id}/profile/")
    assert res.status == 200
    body = await res.json()
    assert body["profile"] == json.loads(tables_index_rows[_resource_id]["csv_detective"])
    if indexes is None:
        assert body["indexes"] is None
    else:
        assert sorted(body["indexes"]) == sorted(indexes)


async def test_api_resource_profile_not_found(client):
    res = await client.get(f"/api/resources/{UNKNOWN_RESOURCE_ID}/profile/")
    assert res.status == 404


async def test_api_resource_data(client, base_url, tables_index_rows):
    detection = json.loads(tables_index_rows[RESOURCE_ID]["csv_detective"])
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert res.status == 200
    body = await res.json()
    assert all(key in body for key in ["data", "links", "meta"])
    assert isinstance(body["data"], list) and len(body["data"]) == 20
    assert isinstance(body["data"][0], dict) and all(
        col in body["data"][0] for col in detection["header"]
    )
    assert body["links"] == {
        "next": f"{base_url}/api/resources/{RESOURCE_ID}/data/?page=2&page_size=20",
        "prev": None,
        "profile": f"{base_url}/api/resources/{RESOURCE_ID}/profile/",
        "swagger": f"{base_url}/api/resources/{RESOURCE_ID}/swagger/",
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
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{params}")
    assert res.status == 200
    body = await res.json()
    assert len(body["data"]) == args.get("page_size", config.PAGE_SIZE_DEFAULT)
    assert body["links"] == {
        "next": (
            f"{base_url}/api/resources/{RESOURCE_ID}/data/"
            f"?page={args.get('page', 1) + 1}"
            f"&page_size={args.get('page_size', config.PAGE_SIZE_DEFAULT)}"
        ),
        "prev": (
            f"{base_url}/api/resources/{RESOURCE_ID}/data/"
            f"?page={args.get('page', 1) - 1}"
            f"&page_size={args.get('page_size', config.PAGE_SIZE_DEFAULT)}"
        )
        if args.get("page", 1) > 1
        else None,
        "profile": f"{base_url}/api/resources/{RESOURCE_ID}/profile/",
        "swagger": f"{base_url}/api/resources/{RESOURCE_ID}/swagger/",
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
async def test_api_resource_data_with_data_args(client, filters):
    args = "&".join(f"{col}__{comp}={str(val).lower()}" for col, comp, val in filters)
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
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


async def test_api_resource_data_with_args_error(client):
    args = "TESTCOLUM_NAME__EXACT=BIDULEpage=1"
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
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


async def test_api_resource_data_with_page_size_error(client):
    args = f"page=1&page_size={config.PAGE_SIZE_MAX + 1}"
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


async def test_api_resource_data_not_found(client):
    res = await client.get(f"/api/resources/{UNKNOWN_RESOURCE_ID}/data/")
    assert res.status == 404


async def test_api_with_unsupported_args(client):
    arg = "limit=1"
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{arg}")
    assert res.status == 400
    body = {
        "errors": [
            {
                "code": None,
                "title": "Invalid query string",
                "detail": f"Malformed query: argument '{arg}' could not be parsed",
            },
        ],
    }
    assert await res.json() == body


@pytest.mark.parametrize(
    "params",
    [
        (INDEXED_RESOURCE_ID, True),
        (AGG_ALLOWED_INDEXED_RESOURCE_ID, False),
    ],
)
async def test_api_exception_resource_indexes(client, tables_index_rows, exceptions_rows, params):
    _resource_id, forbidden = params
    detection = json.loads(tables_index_rows[_resource_id]["csv_detective"])
    indexes = list(json.loads(exceptions_rows[_resource_id]["table_indexes"]).keys())
    res = await client.get(f"/api/resources/{_resource_id}/profile/")
    assert res.status == 200
    content = await res.json()
    assert content["profile"] == detection
    # sorted because it's made from a set so the order might not be preserved
    assert sorted(indexes) == list(sorted(content["indexes"]))

    # checking that the resource is readable with no filter
    res = await client.get(f"/api/resources/{_resource_id}/data/?page=1&page_size=1")
    assert res.status == 200

    # checking that the resource can be filtered on any columns
    for col in detection["columns"].keys():
        if detection["columns"][col]["python_type"] == "json":
            # can't handle json type for now
            continue
        res = await client.get(
            f"/api/resources/{_resource_id}/data/?{col}__exact=1&page=1&page_size=1"
        )
        assert res.status == 200

    # checking that the resource cannot be aggregated on a non-indexed column
    non_indexed_cols = [col for col in detection["columns"].keys() if col not in indexes]
    for col in non_indexed_cols:
        res = await client.get(
            f"/api/resources/{_resource_id}/data/?{col}__groupby&page=1&page_size=1"
        )
        assert res.status == 403

    # checking whether aggregation is allowed on indexed columns
    for idx in indexes:
        res = await client.get(
            f"/api/resources/{_resource_id}/data/?{idx}__groupby&page=1&page_size=1"
        )
        assert res.status == 403 if forbidden else 200


@pytest.mark.parametrize(
    "params",
    [
        (RESOURCE_ID, True),
        (AGG_ALLOWED_RESOURCE_ID, False),
    ],
)
async def test_api_exception_resource_no_indexes(client, tables_index_rows, params):
    _resource_id, forbidden = params
    detection = json.loads(tables_index_rows[_resource_id]["csv_detective"])
    # checking that we have an `indexes` key in the profile endpoint
    res = await client.get(f"/api/resources/{_resource_id}/profile/")
    assert res.status == 200
    content = await res.json()
    assert content["profile"] == detection
    assert content["indexes"] is None

    # checking that the resource is readable with no filter
    res = await client.get(f"/api/resources/{_resource_id}/data/?page=1&page_size=1")
    assert res.status == 200

    # checking that the resource can be filtered on all columns
    for col, results in detection["columns"].items():
        if results["python_type"] == "json":
            continue
        res = await client.get(
            f"/api/resources/{_resource_id}/data/?{col}__exact=1&page=1&page_size=1"
        )
        assert res.status == 200

    # if aggregation is allowed:
    # checking whether aggregation is allowed on all columns or none
    for col, results in detection["columns"].items():
        if results["python_type"] == "json":
            continue
        res = await client.get(
            f"/api/resources/{_resource_id}/data/?{col}__groupby&page=1&page_size=1"
        )
        assert res.status == 403 if forbidden else 200


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
        f"{config.PGREST_ENDPOINT}/migrations_csv",
        status=postgrest_resp_code,
    )
    res = await client.get("/health/")
    assert res.status == api_expected_resp_code
    res_json = await res.json()
    assert all(key in res_json for key in expected_keys)


async def test_aggregation_exceptions(client):
    res = await client.get(f"/api/aggregation-exceptions/")
    aggregations = await res.json()
    assert aggregations["allowed"] == False
    assert aggregations["exceptions"] == config.ALLOW_AGGREGATION_EXCEPTIONS


@pytest.mark.parametrize(
    "_resource_id,batch_size",
    [
        (AGG_ALLOWED_INDEXED_RESOURCE_ID, None),
        (AGG_ALLOWED_RESOURCE_ID, None),
        (INDEXED_RESOURCE_ID, None),
        (RESOURCE_ID, None),
        (RESOURCE_ID, 5),
    ],
)
async def test_api_csv_export(client, mocker, tables_index_rows, _resource_id, batch_size):
    if batch_size is not None:
        mocker.patch("api_tabular.config.BATCH_SIZE", batch_size)
    detection = json.loads(tables_index_rows[_resource_id]["csv_detective"])
    res = await client.get(f"/api/resources/{_resource_id}/data/csv/")
    if batch_size is not None:
        assert res.status == 403
        return
    assert res.status == 200
    content = await res.text()
    reader = csv.reader(StringIO(content))
    columns = next(reader)
    rows = [r for r in reader]
    # __id is added by tabular-api
    assert columns == ["__id"] + list(detection["columns"])
    assert len(rows) == detection["total_lines"]


@pytest.mark.parametrize(
    "_resource_id,batch_size",
    [
        (AGG_ALLOWED_INDEXED_RESOURCE_ID, None),
        (AGG_ALLOWED_RESOURCE_ID, None),
        (INDEXED_RESOURCE_ID, None),
        (RESOURCE_ID, None),
        (RESOURCE_ID, 5),
    ],
)
async def test_api_json_export(client, mocker, tables_index_rows, _resource_id, batch_size):
    if batch_size is not None:
        mocker.patch("api_tabular.config.BATCH_SIZE", batch_size)
    detection = json.loads(tables_index_rows[_resource_id]["csv_detective"])
    res = await client.get(f"/api/resources/{_resource_id}/data/json/")
    if batch_size is not None:
        assert res.status == 403
        return
    assert res.status == 200
    content = await res.text()
    rows = json.loads(content)
    assert len(rows) == detection["total_lines"]
    for row in rows:
        # __id is added by tabular-api
        assert list(row.keys()) == ["__id"] + list(detection["columns"])


async def test_api_resource_with_null_values(client):
    # in this table we have exactly two NULL values per column, 10 rows in total
    response = await client.get(f"/api/resources/{NULL_VALUES_RESOURCE_ID}/profile/")
    profile = await response.json()
    columns = [col for col in profile["profile"]["columns"].keys()]
    for col in columns:
        res = await client.get(f"/api/resources/{NULL_VALUES_RESOURCE_ID}/data/?{col}__isnull")
        body = await res.json()
        assert len(body["data"]) == 2
        assert all(row[col] is None for row in body["data"])
        res = await client.get(f"/api/resources/{NULL_VALUES_RESOURCE_ID}/data/?{col}__isnotnull")
        body = await res.json()
        assert len(body["data"]) == 8
        assert all(row[col] is not None for row in body["data"])
        # checking that `differs` can return NULL values
        if profile["profile"]["columns"][col]["python_type"] == "json":
            # except for json type
            continue
        value = 1
        res = await client.get(
            f"/api/resources/{NULL_VALUES_RESOURCE_ID}/data/?{col}__differs={value}"
        )
        body = await res.json()
        assert all(row[col] != value for row in body["data"])
        assert len([row for row in body["data"] if row[col] is None]) == 2
