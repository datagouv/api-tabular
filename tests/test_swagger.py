import csv
import json

import pytest
import yaml

from api_tabular.utils import OPERATORS_DESCRIPTIONS, TYPE_POSSIBILITIES

from .conftest import RESOURCE_EXCEPTION_PATTERN, RESOURCE_ID, TABLES_INDEX_PATTERN

pytestmark = pytest.mark.asyncio


async def test_swagger_endpoint(client, rmock, mock_get_not_exception):
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"profile": {"columns": {}}}])
    res = await client.get(f"/api/resources/{RESOURCE_ID}/swagger/")
    assert res.status == 200


@pytest.mark.parametrize(
    "allow_aggregation",
    [
        False,
        True,
    ],
)
async def test_swagger_content(client, rmock, allow_aggregation, mocker, mock_get_not_exception):
    if allow_aggregation:
        mocker.patch("api_tabular.config.ALLOW_AGGREGATION", [RESOURCE_ID])
    with open("db/sample.csv", newline="") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        # getting the csv-detective output in the test file
        row, col = 1, 2
        for idx_r, r in enumerate(spamreader):
            if idx_r == row:
                inspection = json.loads(r[col])
    columns = {c: v["python_type"] for c, v in inspection["columns"].items()}
    rmock.get(TABLES_INDEX_PATTERN, payload=[{"profile": {"columns": inspection["columns"]}}])
    res = await client.get(f"/api/resources/{RESOURCE_ID}/swagger/")
    swagger = await res.text()
    swagger_dict = yaml.safe_load(swagger)

    for output in ["json", "csv"]:
        params = swagger_dict["paths"][
            f"/api/resources/{RESOURCE_ID}/data/{'' if output == 'json' else 'csv/'}"
        ]["parameters"]
        params = [p["name"] for p in params]
        for c in columns:
            for p in TYPE_POSSIBILITIES[columns[c]]:
                _params = (
                    ["greater", "less", "strictly_greater", "strictly_less"]
                    if p == "compare"
                    else [p]
                )
                value = "value"
                if p == "sort":
                    value = "asc"
                elif p == "in":
                    value = "value1,value2,..."
                for _p in _params:
                    if allow_aggregation:
                        if (
                            f"{c}__{_p}={value}" not in params  # filters
                            and f"{c}__{_p}" not in params  # aggregators
                        ):
                            raise ValueError(f"{c}__{_p} is missing in {output} output")
                    else:
                        if (
                            not OPERATORS_DESCRIPTIONS.get(_p, {}).get("is_aggregator")
                            and f"{c}__{_p}={value}" not in params  # filters are in
                        ):
                            raise ValueError(f"{c}__{_p} is missing in {output} output")
                        if (
                            OPERATORS_DESCRIPTIONS.get(_p, {}).get("is_aggregator")
                            and f"{c}__{_p}" in params  # aggregators are out
                        ):
                            raise ValueError(f"{c}__{_p} is in {output} output but should not")


@pytest.mark.parametrize(
    "allow_aggregation",
    [
        False,
        True,
    ],
)
async def test_swagger_with_indexes(rmock, client, mocker, allow_aggregation):
    if allow_aggregation:
        mocker.patch("api_tabular.config.ALLOW_AGGREGATION", [RESOURCE_ID])

    indexed_col = ["col1", "col3"]
    non_indexed_col = ["col2", "col4"]
    rmock.get(
        RESOURCE_EXCEPTION_PATTERN,
        payload=[{"table_indexes": {c: "index" for c in indexed_col}}],
        repeat=True,
    )
    rmock.get(
        TABLES_INDEX_PATTERN,
        payload=[
            {
                "profile": {
                    "columns": {
                        c: {"python_type": "int", "format": "int", "score": 1.5}
                        for c in ["col1", "col2", "col3", "col4"]
                    }
                }
            }
        ],
    )
    res = await client.get(f"/api/resources/{RESOURCE_ID}/swagger/")
    swagger = await res.text()
    swagger_dict = yaml.safe_load(swagger)

    for output in ["json", "csv"]:
        params = swagger_dict["paths"][
            f"/api/resources/{RESOURCE_ID}/data/{'' if output == 'json' else 'csv/'}"
        ]["parameters"]
        params = set([p["name"].split("__")[0] for p in params if "__" in p["name"]])
        assert all(c in params for c in indexed_col)
        assert not any(c in params for c in non_indexed_col)
