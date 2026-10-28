"""The SQL endpoint: what a query is refused, and what it is allowed."""

import pytest

from tabular_duckdb.config import settings

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

NOT_ALLOWED = [
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
    "SELECT count(*) OVER () FROM resource",
    "SELECT * FROM resource UNION SELECT * FROM resource",
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT n FROM r",
    "SELECT x -> x + 1 FROM resource",
    "SELECT * FROM resource USING SAMPLE 10%",
]

# an unqualified name could resolve to a catalog view of DuckDB, a path to a file
UNKNOWN_TABLES = [
    "SELECT * FROM pg_settings",
    "SELECT * FROM information_schema.tables",
    "SELECT * FROM main.resource",
    "SELECT * FROM 'https://example.com/private.parquet'",
    "SELECT * FROM '/etc/passwd.csv'",
    "SELECT * FROM resource, pg_settings",
    "SELECT * FROM resource WHERE id IN (SELECT name FROM pg_settings)",
    "WITH a AS (SELECT * FROM pg_settings) SELECT * FROM a",
    # a CTE only sees the ones before it
    "WITH a AS (SELECT * FROM pg_settings), pg_settings AS (SELECT * FROM resource) SELECT * FROM a",
]

ALLOWED_STATEMENTS = [
    "SELECT count(*) AS n FROM resource",
    "SELECT decompte, count(*) AS n FROM resource GROUP BY 1 ORDER BY n DESC",
    "WITH a AS (SELECT * FROM resource) SELECT count(*) FROM a",
    "WITH a AS (SELECT * FROM resource), b AS (SELECT * FROM a) SELECT count(*) FROM b",
    "SELECT __id FROM resource WHERE __id < 10 ORDER BY __id",
    "FROM resource SELECT count(*)",
    "SELECT date_trunc('month', birth) AS m, median(score), sum(decompte) / count(*) FROM resource GROUP BY 1",
    "SELECT CASE WHEN score > 0.5 THEN 'high' ELSE 'low' END AS level, count(DISTINCT id) FROM resource GROUP BY 1",
    "SELECT coalesce(\"Nom de l'élu\", 'none') AS name, CAST(birth AS VARCHAR) FROM resource",
    "SELECT EXTRACT(year FROM birth) AS y, date_part('month', birth) AS m, max(score) FROM resource GROUP BY ALL",
    (
        "SELECT decompte, sum(score) AS s FROM resource WHERE decompte IN (1, 2) "
        "AND score BETWEEN 0 AND 1 AND id IS NOT NULL GROUP BY decompte HAVING count(*) > 1 "
        "ORDER BY s DESC NULLS LAST LIMIT 10 OFFSET 1"
    ),
    (
        "SELECT r.decompte, count(*) * 100.0 / t.total AS share FROM resource r, "
        "(SELECT count(*) AS total FROM resource) t GROUP BY r.decompte, t.total"
    ),
    "SELECT count(*) FROM resource;",
    "SELECT count(*) FROM resource -- a trailing comment",
]


@pytest.mark.parametrize("statement", NOT_A_SELECT + MULTI_STATEMENTS)
def test_only_a_single_select_is_accepted(client, statement):
    response = client.get(SQL_URL, params={"q": statement})
    assert response.status_code == 400
    assert response.json()["errors"][0]["title"] == "Invalid SQL query"


@pytest.mark.parametrize("statement", NOT_ALLOWED)
def test_only_basic_processing_is_allowed(client, statement):
    response = client.get(SQL_URL, params={"q": statement})
    assert response.status_code == 400
    assert response.json()["errors"][0]["detail"].startswith("Not allowed: ")


@pytest.mark.parametrize("statement", UNKNOWN_TABLES)
def test_only_the_resource_and_the_ctes_can_be_read(client, statement):
    response = client.get(SQL_URL, params={"q": statement})
    assert response.status_code == 400
    assert response.json()["errors"][0]["detail"].startswith("Unknown table")


def test_ctes_are_only_allowed_at_the_start_of_the_query(client):
    """A nested CTE would need its own scoping of the table names."""
    statement = "SELECT * FROM (WITH a AS (SELECT * FROM resource) SELECT * FROM a)"
    response = client.get(SQL_URL, params={"q": statement})
    assert response.status_code == 400
    assert "CTE" in response.json()["errors"][0]["detail"]


def test_a_comment_cannot_close_itself_into_the_query(client):
    """The query runs as regenerated from its tree, without the comments."""
    statement = "SELECT count(*) AS n FROM resource -- */ UNION SELECT 0 /*"
    response = client.get(SQL_URL, params={"q": statement})
    assert response.status_code == 200
    assert response.json()["data"] == [{"n": TOTAL_LINES}]


def test_a_deeply_nested_query_is_rejected(client):
    statement = "SELECT " + "(" * 3000 + "1" + ")" * 3000
    response = client.get(SQL_URL, params={"q": statement})
    assert response.status_code == 400
    assert response.json()["errors"][0]["title"] == "Invalid SQL query"


def test_a_parse_error_tells_where(client):
    response = client.get(SQL_URL, params={"q": "SELECT FROM WHERE"})
    assert response.status_code == 400
    assert "(line 1, column 17)" in response.json()["errors"][0]["detail"]


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
