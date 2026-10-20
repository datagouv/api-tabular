import os

import duckdb
import pytest

# any parquet file of the bucket works, these two are only the ones the suite exercises
RESOURCE_ID = os.environ.get("TABULAR_TEST_RESOURCE_ID", "3b6b2281-b9d9-4959-ae9d-c2c166dff118")
BIG_RESOURCE_ID = os.environ.get(
    "TABULAR_TEST_BIG_RESOURCE_ID", "d5f400de-ae3f-4966-8cb6-a85c70c6c24a"
)
UNKNOWN_RESOURCE_ID = "00000000-0000-0000-0000-000000000000"
BASE_URL = os.environ.get("TABULAR_API_URL", "http://localhost:8005")

# a parquet schema close to the test resource of the PostgREST-based API
COLUMNS = {
    "__id": "BIGINT",
    "id": "VARCHAR",
    "score": "DOUBLE",
    "decompte": "BIGINT",
    "is_true": "BOOLEAN",
    "birth": "DATE",
    "liste": "VARCHAR",
    "column_name": "VARCHAR",
    "first_col": "VARCHAR",
    "second_col": "BIGINT",
    "col1": "VARCHAR",
    "col2": "VARCHAR",
    "second.col": "DOUBLE",
    "third col": "BIGINT",
    "fourth..col": "VARCHAR",
}

SOURCE = "source"


@pytest.fixture
def columns() -> dict[str, str]:
    return dict(COLUMNS)


@pytest.fixture(scope="session")
def duckdb_connection():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


@pytest.fixture(scope="session")
def api():
    httpx = pytest.importorskip("httpx")
    client = httpx.Client(base_url=BASE_URL, timeout=120)
    try:
        client.get("/health/")
    except httpx.HTTPError:
        message = f"no API running on {BASE_URL}, start it with `docker compose up -d`"
        # in CI a missing API is a broken setup, skipping would turn the suite green silently
        if os.environ.get("CI"):
            pytest.fail(message)
        pytest.skip(message)
    yield client
    client.close()


@pytest.fixture(scope="session")
def profile(api) -> dict:
    """The resource profile, used to drive the tests whatever the parquet file is."""
    response = api.get(f"/api/resources/{RESOURCE_ID}/profile/")
    assert response.status_code == 200, response.text
    return response.json()["profile"]


@pytest.fixture(scope="session")
def string_column(profile) -> str:
    """A string column with a limited number of values, good for filtering assertions."""
    candidates = [
        name
        for name, description in profile["columns"].items()
        if description["python_type"] == "string"
        and 1 < (profile["profile"].get(name, {}).get("nb_distinct") or 0) < 100
    ]
    if not candidates:
        pytest.skip("no suitable string column in this resource")
    return candidates[0]


@pytest.fixture(scope="session")
def sortable_column(profile) -> str:
    candidates = [
        name
        for name, description in profile["columns"].items()
        if description["python_type"] in ("date", "datetime", "int", "float")
    ]
    if not candidates:
        pytest.skip("no sortable column in this resource")
    return candidates[0]
