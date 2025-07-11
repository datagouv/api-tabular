import json

import pytest
import yaml

from api_tabular.utils import OPERATORS_DESCRIPTIONS, TYPE_POSSIBILITIES

from .conftest import (
    AGG_ALLOWED_INDEXED_RESOURCE_ID,
    AGG_ALLOWED_RESOURCE_ID,
    INDEXED_RESOURCE_ID,
    RESOURCE_ID,
)

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "_resource_id",
    [
        AGG_ALLOWED_INDEXED_RESOURCE_ID,
        AGG_ALLOWED_RESOURCE_ID,
        INDEXED_RESOURCE_ID,
        RESOURCE_ID,
    ],
)
async def test_swagger_endpoint(client, base_url, _resource_id):
    res = await client.get(f"{base_url}/api/resources/{_resource_id}/swagger/")
    assert res.status == 200


@pytest.mark.parametrize(
    "params",
    [
        (RESOURCE_ID, False),
        (AGG_ALLOWED_RESOURCE_ID, True),
    ],
)
async def test_swagger_no_indexes(client, base_url, tables_index_rows, params):
    _resource_id, allow_aggregation = params
    detection = json.loads(tables_index_rows[_resource_id]["csv_detective"])
    columns = {c: v["python_type"] for c, v in detection["columns"].items()}
    res = await client.get(f"{base_url}/api/resources/{_resource_id}/swagger/")
    swagger = await res.text()
    swagger_dict = yaml.safe_load(swagger)

    missing = []
    for output in ["json", "csv"]:
        params = swagger_dict["paths"][
            f"/api/resources/{_resource_id}/data/{'' if output == 'json' else 'csv/'}"
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
                            missing.append(f"{c}__{_p} is missing in {output} output")
                    else:
                        if (
                            not OPERATORS_DESCRIPTIONS.get(_p, {}).get("is_aggregator")
                            and f"{c}__{_p}={value}" not in params  # filters are in
                        ):
                            missing.append(f"{c}__{_p} is missing in {output} output")
                        if (
                            OPERATORS_DESCRIPTIONS.get(_p, {}).get("is_aggregator")
                            and f"{c}__{_p}" in params  # aggregators are out
                        ):
                            missing.append(f"{c}__{_p} is in {output} output but should not")
    if missing:
        raise ValueError("\n" + ";\n".join(missing))


@pytest.mark.parametrize(
    "_resource_id",
    [
        AGG_ALLOWED_INDEXED_RESOURCE_ID,
        INDEXED_RESOURCE_ID,
    ],
)
async def test_swagger_with_indexes(
    client, base_url, tables_index_rows, exceptions_rows, _resource_id
):
    detection = json.loads(tables_index_rows[_resource_id]["csv_detective"])
    indexes = list(json.loads(exceptions_rows[_resource_id]["table_indexes"]).keys())
    non_indexed_cols = [col for col in detection["columns"].keys() if col not in indexes]
    res = await client.get(f"{base_url}/api/resources/{_resource_id}/swagger/")
    swagger = await res.text()
    swagger_dict = yaml.safe_load(swagger)

    for output in ["json", "csv"]:
        params = swagger_dict["paths"][
            f"/api/resources/{_resource_id}/data/{'' if output == 'json' else 'csv/'}"
        ]["parameters"]
        params = set([p["name"].split("__")[0] for p in params if "__" in p["name"]])
        assert all(c in params for c in indexes)
        assert not any(c in params for c in non_indexed_cols)
