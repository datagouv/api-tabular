"""The SQL endpoint: what a query is refused, and what the DuckDB instance prevents it from doing."""

import socket
from pathlib import Path

import duckdb
import pytest

from tabular_duckdb.config import settings
from tabular_duckdb.engine import DuckDBEngine

from .conftest import RESOURCE_ID, TOTAL_LINES

SQL_URL = f"/api/resources/{RESOURCE_ID}/sql/"

NOT_A_SELECT = [
    "CREATE TABLE evil AS SELECT 1",
    "CREATE OR REPLACE VIEW resource AS SELECT 1",
    "CREATE TEMP MACRO lower(x) AS 'pwned'",
    "INSERT INTO resource VALUES (1)",
    "DELETE FROM resource",
    "UPDATE resource SET x = 1",
    "DROP VIEW resource",
    "COPY (SELECT 1) TO '/tmp/evil.csv'",
    "EXPORT DATABASE '/tmp/evil'",
    "ATTACH '/tmp/evil.db' AS evil",
    "INSTALL aws",
    "LOAD aws",
    "SET disabled_filesystems=''",
    "PRAGMA database_list",
    "CALL pragma_table_info('resource')",
    # secrets belong to the instance: one created by a query would be seen by all the others
    "CREATE SECRET leak (TYPE s3, KEY_ID 'a', SECRET 'b')",
    "CREATE SECRET proxy (TYPE http, HTTP_PROXY 'attacker.example:8080')",
    "DROP SECRET IF EXISTS proxy",
]

MULTI_STATEMENTS = [
    "SELECT 1; SELECT 2",
    "SELECT * FROM resource; DROP VIEW resource",
    "SELECT 1) LIMIT 1; SELECT * FROM (SELECT 1",
]

NOT_ALLOWED_FUNCTIONS = [
    "SELECT * FROM read_csv('/etc/passwd')",
    "SELECT * FROM read_text('/etc/passwd')",
    "SELECT * FROM read_blob('/proc/self/environ')",
    "SELECT * FROM glob('/**')",
    "SELECT * FROM read_parquet('https://example.com/private.parquet')",
    "SELECT * FROM duckdb_settings()",
    "SELECT current_setting('memory_limit')",
    "SELECT pg_get_viewdef(1)",
    "SELECT * FROM resource WHERE id IN (SELECT repeat('x', 3))",
    "WITH a AS (SELECT string_agg(id, '') AS s FROM resource) SELECT * FROM a",
]

# a path in a FROM clause is a table name, not a function call: the sandbox refuses it
OUTSIDE_THE_SANDBOX = [
    "SELECT * FROM 'https://example.com/private.parquet'",
    "SELECT * FROM '/etc/passwd.csv'",
]

ALLOWED_STATEMENTS = [
    "SELECT count(*) AS n FROM resource",
    "SELECT decompte, count(*) AS n FROM resource GROUP BY 1 ORDER BY n DESC",
    "WITH a AS (SELECT * FROM resource) SELECT count(*) FROM a",
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
    "SELECT __id FROM resource WHERE __id < 10 ORDER BY __id",
    "FROM resource SELECT count(*)",
    "SELECT date_trunc('month', birth) AS m, median(score), sum(decompte) / count(*) FROM resource GROUP BY 1",
    "SELECT CASE WHEN score > 0.5 THEN 'high' ELSE 'low' END AS level, count(DISTINCT id) FROM resource GROUP BY 1",
    "SELECT coalesce(\"Nom de l'élu\", 'none') AS name, CAST(birth AS VARCHAR) FROM resource",
    "SELECT EXTRACT(year FROM birth) AS y, max(score) FROM resource GROUP BY ALL",
    "SELECT count(*) FROM resource;",
    "SELECT count(*) FROM resource -- a trailing comment",
]


@pytest.mark.parametrize("statement", NOT_A_SELECT + MULTI_STATEMENTS)
def test_only_a_single_select_is_accepted(client, statement):
    response = client.get(SQL_URL, params={"q": statement})
    assert response.status_code == 400
    assert response.json()["errors"][0]["title"] == "Invalid SQL query"


@pytest.mark.parametrize("statement", NOT_ALLOWED_FUNCTIONS)
def test_only_basic_functions_are_allowed(client, statement):
    response = client.get(SQL_URL, params={"q": statement})
    assert response.status_code == 400
    assert "Functions not allowed" in response.json()["errors"][0]["detail"]


@pytest.mark.parametrize("statement", OUTSIDE_THE_SANDBOX)
def test_nothing_outside_the_parquet_prefix_can_be_read(client, statement):
    response = client.get(SQL_URL, params={"q": statement})
    assert response.status_code == 400
    assert "Permission Error" in response.json()["errors"][0]["detail"]


@pytest.mark.parametrize("statement", ALLOWED_STATEMENTS)
def test_allowed_statements(client, statement):
    assert client.get(SQL_URL, params={"q": statement}).status_code == 200


def test_empty_query_is_rejected(client):
    assert client.get(SQL_URL, params={"q": "   "}).status_code == 400


def test_too_long_query_is_rejected(client, monkeypatch):
    monkeypatch.setattr(settings, "SQL_MAX_LENGTH", 20)
    response = client.get(SQL_URL, params={"q": "SELECT count(*) AS n FROM resource"})
    assert response.status_code == 400
    assert "characters" in response.json()["errors"][0]["detail"]


# --- the DuckDB instance -------------------------------------------------------------


@pytest.fixture
async def instance(parquet_dir, monkeypatch):
    """The engine, whose single readable prefix is `parquet_dir`."""
    monkeypatch.setattr(settings, "PARQUET_BASE_URL", parquet_dir)
    engine = DuckDBEngine()
    await engine.start()
    yield engine
    await engine.stop()


async def execute(engine: DuckDBEngine, sql: str) -> list[tuple]:
    """Run any statement on a connection of its own, like a request of the SQL endpoint."""
    with engine.cursor() as cursor:
        return await engine.run(cursor, lambda: cursor.execute(sql).fetchall())


async def test_what_a_connection_creates_disappears_with_it(instance):
    """Statements run on throwaway connections: none can tamper with the next query."""
    await execute(instance, "CREATE TEMP MACRO lower(x) AS 'pwned'")
    await execute(instance, "CREATE TEMP TABLE leftover AS SELECT 1")
    assert await execute(instance, "SELECT lower('A')") == [("a",)]
    with pytest.raises(duckdb.CatalogException):
        await execute(instance, "SELECT * FROM leftover")


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TABLE shared AS SELECT 1",
        "CREATE MACRO lower(x) AS 'pwned'",
        "ATTACH ':memory:' AS writable",
    ],
)
async def test_nothing_shared_can_be_changed(instance, statement):
    with pytest.raises(duckdb.Error):
        await execute(instance, statement)


@pytest.mark.parametrize(
    "statement",
    [
        "SET enable_external_access=true",
        "SET allowed_directories=['/']",
        "SET lock_configuration=false",
    ],
)
async def test_the_sandbox_cannot_be_loosened(instance, statement):
    with pytest.raises(duckdb.Error):
        await execute(instance, statement)


async def test_the_parquet_prefix_is_readable(instance, parquet_dir):
    rows = await execute(
        instance, f"SELECT count(*) FROM read_parquet('{parquet_dir}/{RESOURCE_ID}.parquet')"
    )
    assert rows == [(TOTAL_LINES,)]


@pytest.mark.parametrize(
    "outside",
    [
        "{tmp}/outside.parquet",
        # the prefix is a directory, not the start of a path
        "{prefix}_private/outside.parquet",
        "{prefix}/../outside.parquet",
    ],
)
async def test_local_files_outside_the_prefix_are_denied(instance, parquet_dir, tmp_path, outside):
    path = Path(outside.format(tmp=tmp_path, prefix=parquet_dir))
    path.parent.mkdir(exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        conn.execute(f"COPY (SELECT 1 AS a) TO '{path}' (FORMAT parquet)")
    with pytest.raises(duckdb.PermissionException):
        await execute(instance, f"SELECT * FROM read_parquet('{path}')")


@pytest.fixture
async def remote_instance(monkeypatch):
    """The engine reading an HTTP prefix where nothing listens: a read let through fails to
    connect, a read denied by the sandbox fails before."""
    with socket.create_server(("127.0.0.1", 0)) as server:
        port = server.getsockname()[1]
    prefix = f"http://127.0.0.1:{port}/parquet"
    monkeypatch.setattr(settings, "PARQUET_BASE_URL", prefix)
    engine = DuckDBEngine()
    await engine.start()
    yield engine, prefix
    await engine.stop()


async def test_the_remote_prefix_is_let_through(remote_instance):
    engine, prefix = remote_instance
    with pytest.raises(duckdb.IOException):
        await execute(engine, f"SELECT * FROM read_parquet('{prefix}/x.parquet')")


@pytest.mark.parametrize(
    "outside",
    [
        "{prefix}_private/x.parquet",
        "{prefix}-other/x.parquet",
        "{host}/other/x.parquet",
        "http://other-host/parquet/x.parquet",
    ],
)
async def test_other_remote_prefixes_and_hosts_are_denied(remote_instance, outside):
    engine, prefix = remote_instance
    url = outside.format(prefix=prefix, host=prefix.removesuffix("/parquet"))
    with pytest.raises(duckdb.PermissionException):
        await execute(engine, f"SELECT * FROM read_parquet('{url}')")
