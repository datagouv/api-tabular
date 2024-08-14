import pytest
import csv
import json

from .conftest import RESOURCE_ID, TABLES_INDEX_PATTERN

pytestmark = pytest.mark.asyncio


async def test_swagger_endpoint(client, rmock):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"profile": {"columns": {}}}])
    res = await client.get(f"/api/resources/{RESOURCE_ID}/swagger/")
    assert res.status == 200


async def test_swagger_content(client, rmock):
    with open("db/sample.csv", newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        # getting the csv-detective output in the test file
        row, col = 1, 2
        for idx_r, r in enumerate(spamreader):
            if idx_r == row:
                inspection = json.loads(r[col])
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"profile": {"columns": inspection["columns"]}}])
    res = await client.get(f"/api/resources/{RESOURCE_ID}/swagger/")
    assert res.status == 200
    # NOT DONE YET
