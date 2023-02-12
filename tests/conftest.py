import re

import pytest
import pytest_asyncio

from aiohttp.test_utils import TestClient, TestServer
from aioresponses import aioresponses

from udata_hydra_csvapi import config
from udata_hydra_csvapi.app import app_factory

PG_RST_URL = "https://example.com"
RESOURCE_ID = "60963939-6ada-46bc-9a29-b288b16d969b"
DATE = "2023-01-01T00:00:00.000000+00:00"


@pytest.fixture(autouse=True)
def setup():
    config.override(PG_RST_URL=PG_RST_URL)


@pytest.fixture
def rmock():
    # passthrough for local requests (aiohttp TestServer)
    with aioresponses(passthrough=["http://127.0.0.1"]) as m:
        yield m


@pytest_asyncio.fixture
async def client():
    app = await app_factory()
    async with TestClient(TestServer(app)) as client:
        yield client


@pytest.fixture(autouse=True)
def mock_postgrest(rmock):
    pattern = re.compile(fr"^https://example\.com/tables_index\?.*{RESOURCE_ID}.*$")
    rmock.get(pattern, payload=[{
        "created_at": DATE,
        "url": "https://example.com",
    }])
    pattern = re.compile(r"^https://example\.com/tables_index\?.*$")
    rmock.get(pattern, payload=[])
