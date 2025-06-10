import re

import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer
from aioresponses import aioresponses

from api_tabular import config
from api_tabular.app import app_factory

PGREST_ENDPOINT = "https://example.com"
RESOURCE_ID = "aaaaaaaa-1111-bbbb-2222-cccccccccccc"
DATE = "2023-01-01T00:00:00.000000+00:00"
TABLES_INDEX_PATTERN = re.compile(
    rf"^https://example\.com/tables_index\?.*resource_id=eq.{RESOURCE_ID}.*$"
)
RESOURCE_EXCEPTION_PATTERN = re.compile(
    rf"^https://example\.com/resources_exceptions\?.*resource_id=eq.{RESOURCE_ID}.*$"
)


@pytest.fixture(autouse=True)
def setup():
    config.override(PGREST_ENDPOINT=PGREST_ENDPOINT)


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


@pytest.fixture
def mock_get_no_indexes(rmock):
    rmock.get(RESOURCE_EXCEPTION_PATTERN, payload=[], repeat=True)
