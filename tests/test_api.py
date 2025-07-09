import pytest

from api_tabular import config
from api_tabular.utils import external_url

from .conftest import (
    DATE,
    PGREST_ENDPOINT,
    RESOURCE_EXCEPTION_PATTERN,
    RESOURCE_ID,
    TABLES_INDEX_PATTERN,
)

pytestmark = pytest.mark.asyncio


# async def test_api_resource_meta(client, rmock):
#     rmock.get(
#         TABLES_INDEX_PATTERN,
#         payload=[
#             {
#                 "created_at": DATE,
#                 "url": "https://example.com",
#             }
#         ],
#     )
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/")
#     assert res.status == 200
#     assert await res.json() == {
#         "created_at": DATE,
#         "url": "https://example.com",
#         "links": [
#             {
#                 "href": external_url(f"/api/resources/{RESOURCE_ID}/profile/"),
#                 "type": "GET",
#                 "rel": "profile",
#             },
#             {
#                 "href": external_url(f"/api/resources/{RESOURCE_ID}/data/"),
#                 "type": "GET",
#                 "rel": "data",
#             },
#             {
#                 "href": external_url(f"/api/resources/{RESOURCE_ID}/swagger/"),
#                 "type": "GET",
#                 "rel": "swagger",
#             },
#         ],
#     }


# async def test_api_resource_meta_not_found(client, mock_get_resource_empty):
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/")
#     assert res.status == 404


# async def test_api_resource_profile(client, rmock, mock_get_not_exception):
#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"profile": {"this": "is-a-profile"}}])
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/profile/")
#     assert res.status == 200
#     assert await res.json() == {"profile": {"this": "is-a-profile"}, "indexes": None}


# async def test_api_resource_profile_not_found(client, mock_get_resource_empty):
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/profile/")
#     assert res.status == 404


# async def test_api_resource_data(client, rmock, mock_get_not_exception):
#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
#     rmock.get(
#         f"{PGREST_ENDPOINT}/xxx?limit=20&order=__id.asc",
#         payload={"such": "data"},
#         headers={"Content-Range": "0-10/10"},
#     )
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
#     assert res.status == 200
#     body = {
#         "data": {"such": "data"},
#         "links": {
#             "next": None,
#             "prev": None,
#             "profile": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/"),
#             "swagger": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/"),
#         },
#         "meta": {"page": 1, "page_size": 20, "total": 10},
#     }
#     assert await res.json() == body


# async def test_api_resource_data_with_args(client, rmock, mock_get_not_exception):
#     args = "page=1"
#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
#     rmock.get(
#         f"{PGREST_ENDPOINT}/xxx?limit=20&order=__id.asc",
#         payload={"such": "data"},
#         headers={"Content-Range": "0-10/10"},
#     )
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
#     assert res.status == 200
#     body = {
#         "data": {"such": "data"},
#         "links": {
#             "next": None,
#             "prev": None,
#             "profile": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/"),
#             "swagger": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/"),
#         },
#         "meta": {"page": 1, "page_size": 20, "total": 10},
#     }
#     assert await res.json() == body


# async def test_api_resource_data_with_args_case(client, rmock, mock_get_not_exception):
#     args = "COLUM_NAME__EXACT=BIDULE&page=1"
#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
#     rmock.get(
#         f'{PGREST_ENDPOINT}/xxx?"COLUM_NAME"=eq.BIDULE&limit=20&order=__id.asc',
#         payload={"such": "data"},
#         headers={"Content-Range": "0-10/10"},
#     )
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
#     assert res.status == 200
#     body = {
#         "data": {"such": "data"},
#         "links": {
#             "next": None,
#             "prev": None,
#             "profile": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/"),
#             "swagger": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/"),
#         },
#         "meta": {"page": 1, "page_size": 20, "total": 10},
#     }
#     assert await res.json() == body


# async def test_api_resource_data_with_args_error(client, rmock, mock_get_not_exception):
#     args = "TESTCOLUM_NAME__EXACT=BIDULEpage=1"
#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
#     assert res.status == 400
#     assert await res.json() == {
#         "errors": [
#             {
#                 "code": None,
#                 "title": "Invalid query string",
#                 "detail": "Malformed query: argument 'TESTCOLUM_NAME__EXACT=BIDULEpage=1' could not be parsed",
#             }
#         ]
#     }


# async def test_api_resource_data_with_page_size_error(client, rmock):
#     args = "page=1&page_size=100"
#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}")
#     assert res.status == 400
#     assert await res.json() == {
#         "errors": [
#             {
#                 "code": None,
#                 "detail": f"Page size exceeds allowed maximum: {config.PAGE_SIZE_MAX}",
#                 "title": "Invalid query string",
#             }
#         ]
#     }


# async def test_api_resource_data_not_found(client, mock_get_resource_empty, mock_get_not_exception):
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
#     assert res.status == 404


# async def test_api_resource_data_table_error(client, rmock, mock_get_not_exception):
#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
#     rmock.get(
#         f"{PGREST_ENDPOINT}/xxx?limit=20&order=__id.asc", status=502, payload={"such": "error"}
#     )
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/data/")
#     assert res.status == 502
#     assert await res.json() == {
#         "errors": [{"code": None, "detail": {"such": "error"}, "title": "Database error"}]
#     }


# async def test_api_percent_encoding_arabic(client, rmock, mock_get_not_exception):
#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
#     rmock.get(
#         f'{PGREST_ENDPOINT}/xxx?"%D9%85%D9%88%D8%A7%D8%B1%D8%AF"=eq.%D9%85%D9%88%D8%A7%D8%B1%D8%AF&limit=20&order=__id.asc',  # noqa
#         status=200,
#         payload={"such": "data"},
#         headers={"Content-Range": "0-10/10"},
#     )
#     res = await client.get(
#         f"/api/resources/{RESOURCE_ID}/data/?%D9%85%D9%88%D8%A7%D8%B1%D8%AF__exact=%D9%85%D9%88%D8%A7%D8%B1%D8%AF"
#     )
#     assert res.status == 200
#     body = {
#         "data": {"such": "data"},
#         "links": {
#             "next": None,
#             "prev": None,
#             "profile": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/"),
#             "swagger": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/"),
#         },
#         "meta": {"page": 1, "page_size": 20, "total": 10},
#     }
#     assert await res.json() == body


# async def test_api_with_unsupported_args(client, rmock, mock_get_not_exception):
#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
#     rmock.get(
#         f"{PGREST_ENDPOINT}/xxx?limit=20&order=__id.asc",
#         status=200,
#         payload={"such": "data"},
#         headers={"Content-Range": "0-10/10"},
#     )
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?limit=1&select=numnum")
#     assert res.status == 400
#     body = {
#         "errors": [
#             {
#                 "code": None,
#                 "title": "Invalid query string",
#                 "detail": "Malformed query: argument 'limit=1' could not be parsed",
#             },
#         ],
#     }
#     assert await res.json() == body


# async def test_api_pagination(client, rmock, mock_get_not_exception):
#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
#     rmock.get(
#         f"{PGREST_ENDPOINT}/xxx?limit=1&order=__id.asc",
#         status=200,
#         payload=[{"such": "data"}],
#         headers={"Content-Range": "0-2/2"},
#     )
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?page=1&page_size=1")
#     assert res.status == 200
#     body = {
#         "data": [{"such": "data"}],
#         "links": {
#             "next": external_url(
#                 "/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?page=2&page_size=1"
#             ),
#             "prev": None,
#             "profile": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/"),
#             "swagger": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/"),
#         },
#         "meta": {"page": 1, "page_size": 1, "total": 2},
#     }
#     assert await res.json() == body

#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"__id": 1, "id": "test-id", "parsing_table": "xxx"}])
#     rmock.get(
#         f"{PGREST_ENDPOINT}/xxx?limit=1&offset=1&order=__id.asc",
#         status=200,
#         payload=[{"such": "data"}],
#         headers={"Content-Range": "0-2/2"},
#     )
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?page=2&page_size=1")
#     assert res.status == 200
#     body = {
#         "data": [{"such": "data"}],
#         "links": {
#             "next": None,
#             "prev": external_url(
#                 "/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/data/?page=1&page_size=1"
#             ),
#             "profile": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/profile/"),
#             "swagger": external_url("/api/resources/aaaaaaaa-1111-bbbb-2222-cccccccccccc/swagger/"),
#         },
#         "meta": {"page": 2, "page_size": 1, "total": 2},
#     }
#     assert await res.json() == body


# async def test_api_exception_resource_indexes(client, rmock, mocker):
#     # fake exception with indexed columns
#     indexed_col = ["col1", "col3"]
#     rmock.get(
#         RESOURCE_EXCEPTION_PATTERN,
#         payload=[{"table_indexes": {c: "index" for c in indexed_col}}],
#         repeat=True,
#     )

#     # checking that we have an `indexes` key in the profile endpoint
#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"profile": {"this": "is-a-profile"}}])
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/profile/")
#     assert res.status == 200
#     content = await res.json()
#     assert content["profile"] == {"this": "is-a-profile"}
#     # sorted because it's made from a set so the order might not be preserved
#     assert indexed_col == list(sorted(content["indexes"]))

#     # checking that the resource is readable with no filter
#     table = "xxx"
#     rmock.get(
#         TABLES_INDEX_PATTERN,
#         payload=[{"__id": 1, "id": "test-id", "parsing_table": table}],
#         repeat=True,
#     )
#     rmock.get(
#         f"{PGREST_ENDPOINT}/{table}?limit=1&order=__id.asc",
#         status=200,
#         payload=[{"col1": 1, "col2": 2, "col3": 3, "col4": 4}],
#         headers={"Content-Range": "0-2/2"},
#     )
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?page=1&page_size=1")
#     assert res.status == 200

#     # checking that the resource can be filtered on an indexed column
#     rmock.get(
#         f'{PGREST_ENDPOINT}/{table}?"{indexed_col[0]}"=gte.1&limit=1&order=__id.asc',
#         status=200,
#         payload=[{"col1": 1, "col2": 2, "col3": 3, "col4": 4}],
#         headers={"Content-Range": "0-2/2"},
#     )
#     res = await client.get(
#         f"/api/resources/{RESOURCE_ID}/data/?{indexed_col[0]}__greater=1&page=1&page_size=1"
#     )
#     assert res.status == 200

#     # checking that the resource cannot be filtered on a non-indexed column
#     non_indexed_col = "col2"
#     # postgrest would return a content
#     rmock.get(
#         f'{PGREST_ENDPOINT}/{table}?"{non_indexed_col}"=gte.1&limit=1&order=__id.asc',
#         status=200,
#         payload=[{"col1": 1, "col2": 2, "col3": 3, "col4": 4}],
#         headers={"Content-Range": "0-2/2"},
#     )
#     res = await client.get(
#         f"/api/resources/{RESOURCE_ID}/data/?{non_indexed_col}__greater=1&page=1&page_size=1"
#     )
#     # but it's forbidden
#     assert res.status == 403

#     # if aggregation is allowed:
#     mocker.patch("api_tabular.config.ALLOW_AGGREGATION", [RESOURCE_ID])

#     # checking that it's not possible on a non-indexed column
#     # postgrest would return a content
#     rmock.get(
#         f'{PGREST_ENDPOINT}/{table}?select="{non_indexed_col}__avg":"{non_indexed_col}".avg()&limit=1',
#         status=200,
#         payload=[{f"{non_indexed_col}__avg": 2}],
#         headers={"Content-Range": "0-2/2"},
#     )
#     res = await client.get(
#         f"/api/resources/{RESOURCE_ID}/data/?{non_indexed_col}__avg&page=1&page_size=1"
#     )
#     # but it's forbidden
#     assert res.status == 403

#     # checking that it is possible on an indexed column
#     rmock.get(
#         f'{PGREST_ENDPOINT}/{table}?select="{indexed_col[0]}__avg":"{indexed_col[0]}".avg()&limit=1',
#         status=200,
#         payload=[{f"{indexed_col[0]}__avg": 2}],
#         headers={"Content-Range": "0-2/2"},
#     )
#     res = await client.get(
#         f"/api/resources/{RESOURCE_ID}/data/?{indexed_col[0]}__avg&page=1&page_size=1"
#     )
#     assert res.status == 200


# async def test_api_exception_resource_no_indexes(client, rmock, mocker):
#     # fake exception with no indexed column
#     rmock.get(TABLES_INDEX_PATTERN, payload=[{"profile": {"this": "is-a-profile"}}])
#     rmock.get(
#         RESOURCE_EXCEPTION_PATTERN,
#         payload=[{"table_indexes": {}}],
#         repeat=True,
#     )

#     # checking that we have an `indexes` key in the profile endpoint
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/profile/")
#     assert res.status == 200
#     content = await res.json()
#     assert content["profile"] == {"this": "is-a-profile"}
#     assert content["indexes"] is None

#     data = [{f"col{k}": k for k in range(1, 5)}]
#     # checking that the resource is readable with no filter
#     table = "xxx"
#     rmock.get(
#         TABLES_INDEX_PATTERN,
#         payload=[{"__id": 1, "id": "test-id", "parsing_table": table}],
#         repeat=True,
#     )
#     rmock.get(
#         f"{PGREST_ENDPOINT}/{table}?limit=1&order=__id.asc",
#         status=200,
#         payload=data,
#         headers={"Content-Range": "0-2/2"},
#     )
#     res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?page=1&page_size=1")
#     assert res.status == 200

#     # checking that the resource can be filtered on all columns
#     for k in range(1, 5):
#         rmock.get(
#             f'{PGREST_ENDPOINT}/{table}?"col{k}"=gte.1&limit=1&order=__id.asc',
#             status=200,
#             payload=data,
#             headers={"Content-Range": "0-2/2"},
#         )
#         res = await client.get(
#             f"/api/resources/{RESOURCE_ID}/data/?col{k}__greater=1&page=1&page_size=1"
#         )
#         assert res.status == 200

#     # if aggregation is allowed:
#     mocker.patch("api_tabular.config.ALLOW_AGGREGATION", [RESOURCE_ID])
#     # checking that aggregation is allowed on all columns
#     for k in range(1, 5):
#         rmock.get(
#             f'{PGREST_ENDPOINT}/{table}?select="col{k}__avg":"col{k}".avg()&limit=1',
#             status=200,
#             payload=[{"col2__avg": 2}],
#             headers={"Content-Range": "0-2/2"},
#         )
#         res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?col{k}__avg&page=1&page_size=1")
#         assert res.status == 200


# @pytest.mark.parametrize(
#     "params",
#     [
#         (503, 503, ["errors"]),
#         (200, 200, ["status", "version", "uptime_seconds"]),
#     ],
# )
# async def test_health(client, rmock, params):
#     postgrest_resp_code, api_expected_resp_code, expected_keys = params
#     rmock.head(
#         f"{PGREST_ENDPOINT}/migrations_csv",
#         status=postgrest_resp_code,
#     )
#     res = await client.get("/health/")
#     assert res.status == api_expected_resp_code
#     res_json = await res.json()
#     assert all(key in res_json for key in expected_keys)


async def test_tmp_api_resource_meta(client, base_url, tables_index_rows):
    # import aiohttp
    # async with aiohttp.ClientSession() as session:
    #     async with session.get(f"{base_url}api/resources/{RESOURCE_ID}/") as res:
    #         record = await res.json()
    #         print(record)
    #         assert res.status == 200
    #         assert "links" in record

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
