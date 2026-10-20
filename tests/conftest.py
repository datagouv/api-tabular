import duckdb
import pytest
from fastapi.testclient import TestClient

from tabular_duckdb.config import settings
from tabular_duckdb.main import app

RESOURCE_ID = "aaaaaaaa-1111-bbbb-2222-cccccccccccc"
UNKNOWN_RESOURCE_ID = "00000000-0000-0000-0000-000000000000"
TOTAL_LINES = 60

# row i (1-based, which is also its __id) is fully determined by i, so that the expected
# results of a query can be written down in the tests
RESOURCE_SQL = f"""
SELECT
    'id-' || i AS id,
    (i % 10) / 10 AS score,
    (i % 7)::BIGINT AS decompte,
    i % 2 = 0 AS is_true,
    (DATE '1950-01-01' + to_years(i::INTEGER))::DATE AS birth,
    CASE WHEN i % 5 = 0 THEN NULL ELSE 'Élu ' || i END AS "Nom de l'élu",
    ['a', 'b'] AS liste
FROM range(1, {TOTAL_LINES + 1}) AS t(i)
"""
HEADER = ["id", "score", "decompte", "is_true", "birth", "Nom de l'élu", "liste"]


@pytest.fixture(scope="session")
def parquet_dir(tmp_path_factory) -> str:
    directory = tmp_path_factory.mktemp("parquet")
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY ({RESOURCE_SQL}) TO '{directory}/{RESOURCE_ID}.parquet' (FORMAT parquet)"
        )
    return str(directory)


@pytest.fixture(scope="session")
def client(parquet_dir):
    """The API, run in process against the parquet files written by `parquet_dir`."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(settings, "PARQUET_BASE_URL", parquet_dir)
        patch.setattr(settings, "SERVER_NAME", "testserver")
        with TestClient(app) as test_client:
            yield test_client


@pytest.fixture
def base_url() -> str:
    return "http://testserver"


@pytest.fixture
def duckdb_connection():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()
