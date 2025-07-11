import csv
import re
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer
from aioresponses import aioresponses

from api_tabular import config
from api_tabular.app import app_factory

PGREST_ENDPOINT = "https://example.com"
RESOURCE_ID = "aaaaaaaa-1111-bbbb-2222-cccccccccccc"
UNKNOWN_RESOURCE_ID = "aaaaaaaa-1111-bbbb-2222-cccccccccccA"
INDEXED_RESOURCE_ID = "aaaaaaaa-5555-bbbb-6666-cccccccccccc"
TABLES_INDEX_PATTERN = re.compile(
    rf"^https://example\.com/tables_index\?.*resource_id=eq.{RESOURCE_ID}.*$"
)
RESOURCE_EXCEPTION_PATTERN = re.compile(
    rf"^https://example\.com/resources_exceptions\?.*resource_id=eq.{RESOURCE_ID}.*$"
)


@pytest.fixture
def setup():
    config.override(PGREST_ENDPOINT=PGREST_ENDPOINT)


@pytest.fixture
def rmock():
    # passthrough for local requests (aiohttp TestServer)
    with aioresponses(passthrough=["http://127.0.0.1"]) as m:
        yield m


@pytest_asyncio.fixture
async def client():
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
def mock_get_not_exception(rmock):
    rmock.get(RESOURCE_EXCEPTION_PATTERN, payload=[], repeat=True)


@pytest_asyncio.fixture
async def fake_client():
    app = await app_factory()
    async with TestClient(TestServer(app)) as client:
        yield client


@pytest.fixture
def base_url():
    yield f"{config.SCHEME}://{config.SERVER_NAME}"


def timestamptz_to_utc_iso(date_str: str) -> str:
    """To convert the dates in TIMESTAMPTZ format (inserted into postgres) to isoformat"""
    return (
        datetime.fromisoformat((date_str + ":00").replace(" ", "T"))
        .astimezone(timezone.utc)
        .isoformat()
    )


@pytest.fixture
def tables_index_rows():
    base_directory = Path(__file__).parent.parent
    rows = {}
    with open(base_directory / "db/tables_index.csv", mode="r") as file:
        reader = csv.reader(file)
        columns = next(reader)
        for row in reader:
            row_dict = {
                col: value if col != "created_at" else timestamptz_to_utc_iso(value)
                for col, value in zip(columns, row)
            }
            rows[row_dict["resource_id"]] = row_dict
    yield rows
