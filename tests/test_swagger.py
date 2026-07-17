import json

import pytest
import yaml

from api_tabular.core.swagger import OPERATORS_DESCRIPTIONS, TYPE_POSSIBILITIES

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
async def test_swagger_endpoint(client, _resource_id):
    res = await client.get(f"/api/resources/{_resource_id}/swagger/")
    assert res.status == 200


@pytest.mark.parametrize(
    "_resource_id",
    [
        RESOURCE_ID,
        AGG_ALLOWED_RESOURCE_ID,
        INDEXED_RESOURCE_ID,
        AGG_ALLOWED_INDEXED_RESOURCE_ID,
    ],
)
async def test_swagger_includes_all_columns_and_aggregators(
    client, tables_index_rows, _resource_id
):
    detection = json.loads(tables_index_rows[_resource_id]["csv_detective"])
    columns = {c: v["python_type"] for c, v in detection["columns"].items()}
    res = await client.get(f"/api/resources/{_resource_id}/swagger/")
    swagger = await res.text()
    swagger_dict = yaml.safe_load(swagger)

    missing = []
    for output in ["json", "csv"]:
        params = swagger_dict["paths"][
            f"/api/resources/{_resource_id}/data/{'' if output == 'json' else 'csv/'}"
        ]["parameters"]
        params = {p["name"]: p for p in params}
        for c in columns:
            for p in TYPE_POSSIBILITIES[columns[c]]:
                _params = (
                    ["greater", "less", "strictly_greater", "strictly_less"]
                    if p == "compare"
                    else [p]
                )
                for _p in _params:
                    if f"{c}__{_p}" not in params:
                        missing.append(f"{c}__{_p} is missing in {output} output")
                    elif OPERATORS_DESCRIPTIONS.get(_p, {}).get("is_aggregator"):
                        assert params[f"{c}__{_p}"].get("allowEmptyValue")
    if missing:
        raise ValueError("\n" + ";\n".join(missing))
