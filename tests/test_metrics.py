from typing import Any, AsyncGenerator

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer

from api_tabular.metrics.app import app_factory

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def metrics_client() -> AsyncGenerator[TestClient, Any]:
    app = await app_factory()
    async with TestClient(TestServer(app)) as client:
        yield client


@pytest.mark.parametrize(
    "args,detail",
    [
        ("page_size=0", "Page size must be at least 1"),
        ("page_size=-1", "Page size must be at least 1"),
        ("page_size=abc", "Page size must be an integer"),
        ("page_size=1.5", "Page size must be an integer"),
        ("page_size=", "Page size must be an integer"),
        ("page=abc", "Page must be an integer"),
        ("page=0", "Page must be at least 1"),
        ("page=-1", "Page must be at least 1"),
    ],
)
async def test_metrics_data_with_invalid_pagination_returns_400(metrics_client, args, detail):
    res = await metrics_client.get(f"/api/site/data/?{args}")
    assert res.status == 400
    assert await res.json() == {
        "errors": [{"code": None, "detail": detail, "title": "Invalid query string"}]
    }
