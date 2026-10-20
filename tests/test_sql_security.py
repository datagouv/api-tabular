"""Every query here must be rejected before DuckDB executes anything."""

import pytest

from tabular_duckdb.errors import QueryException
from tabular_duckdb.sql import validate_sql

WRITE_STATEMENTS = [
    "CREATE TABLE evil AS SELECT 1",
    "CREATE OR REPLACE VIEW resource AS SELECT 1",
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
    "CREATE SECRET leak (TYPE s3, KEY_ID 'a', SECRET 'b')",
]

FILE_ACCESS_STATEMENTS = [
    "SELECT * FROM read_parquet('s3://hydra/other.parquet')",
    "SELECT * FROM read_csv('/etc/passwd')",
    "SELECT * FROM read_text('/etc/passwd')",
    "SELECT * FROM read_blob('/etc/passwd')",
    "SELECT * FROM glob('/**')",
    "SELECT * FROM 's3://other-bucket/private.parquet'",
    'SELECT * FROM "s3://other-bucket/private.parquet"',
    "SELECT * FROM parquet_metadata('s3://hydra/other.parquet')",
]

CATALOG_STATEMENTS = [
    "SELECT * FROM duckdb_secrets()",
    "SELECT * FROM duckdb_settings()",
    "SELECT * FROM duckdb_extensions()",
    "SELECT current_setting('s3_secret_access_key')",
    "SELECT * FROM other_table",
    "SELECT * FROM resource JOIN other_table ON true",
    "SELECT (SELECT count(*) FROM other_table) FROM resource",
    # a CTE name must not unlock a qualified table, nor a table outside of the CTE's scope
    "WITH duckdb_views AS (SELECT 1) SELECT sql FROM system.main.duckdb_views",
    "SELECT * FROM (WITH duckdb_databases AS (SELECT 1) SELECT 1), duckdb_databases",
]

MULTI_STATEMENTS = [
    "SELECT 1; SELECT 2",
    "SELECT * FROM resource; DROP VIEW resource",
    "SELECT * FROM resource; COPY (SELECT 1) TO '/tmp/evil.csv'",
]

ALLOWED_STATEMENTS = [
    "SELECT count(*) AS n FROM resource",
    'SELECT "Code du département" AS dep, count(*) AS n FROM resource GROUP BY 1 ORDER BY n DESC',
    "WITH per_dep AS (SELECT 1 AS n) SELECT sum(n) FROM per_dep",
    "WITH a AS (SELECT * FROM resource) SELECT count(*) FROM a",
    "WITH a AS (SELECT 1 AS n), b AS (SELECT n FROM a) SELECT n FROM b",
    "WITH a AS (SELECT 1 AS n) SELECT * FROM (SELECT n FROM a)",
    "SELECT __id FROM resource WHERE __id < 10 ORDER BY __id",
    "SELECT unnest([1, 2, 3]) AS x",
    "SELECT * FROM generate_series(1, 5)",
    "FROM resource SELECT count(*)",
    "SELECT r.__id FROM resource r LIMIT 1",
]


@pytest.mark.parametrize(
    "statement",
    WRITE_STATEMENTS + FILE_ACCESS_STATEMENTS + CATALOG_STATEMENTS + MULTI_STATEMENTS,
)
def test_rejected_statements(duckdb_connection, statement):
    with pytest.raises(QueryException) as raised:
        validate_sql(duckdb_connection, statement)
    assert raised.value.status == 400


@pytest.mark.parametrize("statement", ALLOWED_STATEMENTS)
def test_allowed_statements(duckdb_connection, statement):
    validate_sql(duckdb_connection, statement)


def test_empty_query_is_rejected(duckdb_connection):
    with pytest.raises(QueryException):
        validate_sql(duckdb_connection, "   ")


def test_too_long_query_is_rejected(duckdb_connection, monkeypatch):
    monkeypatch.setattr("tabular_duckdb.sql.settings.SQL_MAX_LENGTH", 20)
    with pytest.raises(QueryException, match="characters"):
        validate_sql(duckdb_connection, "SELECT " + ", ".join(["1"] * 50) + " FROM resource")


def test_trailing_semicolon_is_accepted(duckdb_connection):
    assert validate_sql(duckdb_connection, "SELECT count(*) FROM resource;") == (
        "SELECT count_star() FROM resource"
    )


def test_executed_sql_is_rebuilt_from_the_validated_tree(duckdb_connection):
    """What runs is DuckDB's own rendering of the tree, so trailing comments cannot survive."""
    assert (
        validate_sql(duckdb_connection, "SELECT 1 AS a -- ; DROP VIEW resource") == "SELECT 1 AS a"
    )
    assert (
        validate_sql(duckdb_connection, 'SELECT "Code du département" FROM resource')
        == 'SELECT "Code du département" FROM resource'
    )


def test_nothing_is_executed_during_validation(duckdb_connection):
    """Validation must not create anything, even for a rejected write statement."""
    with pytest.raises(QueryException):
        validate_sql(duckdb_connection, "CREATE TABLE side_effect AS SELECT 1")
    tables = duckdb_connection.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'side_effect'"
    ).fetchone()
    assert tables[0] == 0
