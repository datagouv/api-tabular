import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from api_tabular.metrics.app import app_factory as metrics_app_factory

from .conftest import RESOURCE_ID, UNKNOWN_RESOURCE_ID

pytestmark = pytest.mark.asyncio

ORIGIN = {"Origin": "http://front.example"}
EXPECTED_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
    "Access-Control-Expose-Headers": "*",
}


@pytest_asyncio.fixture
async def metrics_client():
    app = await metrics_app_factory()
    async with TestClient(TestServer(app)) as client:
        yield client


def assert_cors_headers_set_once(res) -> None:
    for header, value in EXPECTED_CORS_HEADERS.items():
        assert res.headers.getall(header, []) == [value], header


async def test_cors_headers_on_successful_response(client):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?page_size=1", headers=ORIGIN)
    assert res.status == 200
    assert_cors_headers_set_once(res)


async def test_cors_headers_on_preflight(client):
    res = await client.options(f"/api/resources/{RESOURCE_ID}/data/", headers=ORIGIN)
    assert res.status == 204
    assert_cors_headers_set_once(res)


@pytest.mark.parametrize("export_format", ["csv", "json"])
async def test_cors_headers_on_streamed_export(client, export_format):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/{export_format}/", headers=ORIGIN)
    assert res.status == 200
    assert_cors_headers_set_once(res)
    assert await res.text()


async def test_cors_headers_on_400_keep_error_body(client):
    args = "limit=1"
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?{args}", headers=ORIGIN)
    assert res.status == 400
    assert_cors_headers_set_once(res)
    assert await res.json() == {
        "errors": [
            {
                "code": None,
                "title": "Invalid query string",
                "detail": f"Malformed query: argument '{args}' could not be parsed",
            }
        ]
    }


async def test_cors_headers_on_403_keep_error_body(client):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?score__groupby", headers=ORIGIN)
    assert res.status == 403
    assert_cors_headers_set_once(res)
    assert await res.json() == {
        "errors": [
            {
                "code": None,
                "title": "Unauthorized parameters",
                "detail": "Aggregation parameters `groupby` are not allowed "
                f"for resource '{RESOURCE_ID}'",
            }
        ]
    }


async def test_cors_headers_on_404_unknown_resource(client):
    res = await client.get(f"/api/resources/{UNKNOWN_RESOURCE_ID}/data/", headers=ORIGIN)
    assert res.status == 404
    assert_cors_headers_set_once(res)
    assert await res.text() == "404: Not Found"


async def test_cors_headers_on_404_unknown_route(client):
    res = await client.get("/api/unknown-route/", headers=ORIGIN)
    assert res.status == 404
    assert_cors_headers_set_once(res)


async def test_cors_headers_on_unhandled_500(client):
    res = await client.get(f"/api/resources/{RESOURCE_ID}/data/?page_size=abc", headers=ORIGIN)
    assert res.status == 500
    assert_cors_headers_set_once(res)
    assert (await res.text()).startswith("500 Internal Server Error")


async def test_cors_headers_on_metrics_400(metrics_client):
    res = await metrics_client.get("/api/site/data/?limit=1", headers=ORIGIN)
    assert res.status == 400
    assert_cors_headers_set_once(res)
