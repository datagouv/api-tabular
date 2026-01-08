import csv
from datetime import datetime, timezone
from pathlib import Path
from re import M
from typing import Any, AsyncGenerator, Generator

import aiohttp
import pytest
import pytest_asyncio
from aiohttp.test_utils import TestClient, TestServer
from aioresponses import aioresponses

from api_tabular import config
from api_tabular.tabular.app import app_factory

RESOURCE_ID = "aaaaaaaa-1111-bbbb-2222-cccccccccccc"
UNKNOWN_RESOURCE_ID = "aaaaaaaa-1111-bbbb-2222-cccccccccccA"
INDEXED_RESOURCE_ID = "aaaaaaaa-5555-bbbb-6666-cccccccccccc"
AGG_ALLOWED_RESOURCE_ID = "dddddddd-7777-eeee-8888-ffffffffffff"
AGG_ALLOWED_INDEXED_RESOURCE_ID = "aaaaaaaa-9999-bbbb-1010-cccccccccccc"
DELETED_RESOURCE_ID = "deadbeef-dead-beef-dead-beefdeadbeef"
NULL_VALUES_RESOURCE_ID = "dddddddd-1111-eeee-1212-ffffffffffff"


@pytest.fixture
def rmock():
    # passthrough for local requests (aiohttp TestServer)
    with aioresponses(passthrough=["http://127.0.0.1"]) as m:
        yield m


@pytest_asyncio.fixture
async def fake_client() -> AsyncGenerator[TestClient, Any]:
    app = await app_factory()
    async with TestClient(TestServer(app)) as client:
        yield client


@pytest.fixture
def base_url() -> Generator[str, Any, Any]:
    yield f"{config.SCHEME}://{config.SERVER_NAME}"


def timestamptz_to_utc_iso(date_str: str) -> str:
    """To convert the dates in TIMESTAMPTZ format (inserted into postgres) to isoformat"""
    return (
        datetime.fromisoformat((date_str + ":00").replace(" ", "T"))
        .astimezone(timezone.utc)
        .isoformat()
    )


def csv_to_dict(file_name: str) -> dict:
    base_directory = Path(__file__).parent.parent
    rows = {}
    with open(base_directory / f"db/{file_name}.csv", mode="r") as file:
        reader = csv.reader(file)
        columns = next(reader)
        for row in reader:
            row_dict = {
                col: value
                if col not in ["created_at", "deleted_at"]
                else timestamptz_to_utc_iso(value)
                if value
                else None
                for col, value in zip(columns, row)
            }
            rows[row_dict["resource_id"]] = row_dict
    return rows


@pytest.fixture
def tables_index_rows() -> Generator[dict, Any, Any]:
    yield csv_to_dict("tables_index")


@pytest.fixture
def exceptions_rows() -> Generator[dict, Any, Any]:
    yield csv_to_dict("exceptions")
