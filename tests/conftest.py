import re

import pytest
import pytest_asyncio

from aiohttp.test_utils import TestClient, TestServer
from aioresponses import aioresponses

from api_tabular import config
from api_tabular.app import app_factory

PG_RST_URL = "https://example.com"
RESOURCE_ID = "60963939-6ada-46bc-9a29-b288b16d969b"
DATE = "2023-01-01T00:00:00.000000+00:00"
TABLES_INDEX_PATTERN = re.compile(
    rf"^https://example\.com/tables_index\?.*resource_id=eq.{RESOURCE_ID}.*$"
)


@pytest.fixture(autouse=True)
def setup():
    config.override(PG_RST_URL=PG_RST_URL)


@pytest.fixture(autouse=True)
def rmock():
    # passthrough for local requests (aiohttp TestServer)
    with aioresponses(passthrough=["http://127.0.0.1"]) as m:
        yield m


@pytest_asyncio.fixture
async def client():
    app = await app_factory()
    async with TestClient(TestServer(app)) as client:
        yield client


@pytest.fixture
def mock_get_resource_empty(rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[])
