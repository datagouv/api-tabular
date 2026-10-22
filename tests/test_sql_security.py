"""The SQL endpoint: validation of the queries, then the sandbox of the connections running them."""

import socket
from pathlib import Path

import duckdb
import pytest

from tabular_duckdb.config import settings
from tabular_duckdb.engine import DuckDBEngine
from tabular_duckdb.errors import QueryException
from tabular_duckdb.sql import validate_sql

from .conftest import RESOURCE_ID

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
    # in its body, a CTE only sees the CTEs declared before it: DuckDB binds the other names,
    # its own included, to the catalog
    "WITH x AS (SELECT sql FROM duckdb_views), duckdb_views AS (SELECT 1 AS sql) SELECT * FROM x",
    "WITH duckdb_views AS (SELECT sql FROM duckdb_views) SELECT * FROM duckdb_views",
    # built-in macros whose body queries the catalog only show as a function call in the tree
    "SELECT pg_get_viewdef(range) FROM range(0, 100)",
    "SELECT pg_get_constraintdef(range) FROM range(0, 100)",
    "SELECT format_type(range, 0) FROM range(0, 100)",
    "SELECT get_block_size('memory')",
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
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r",
    "SELECT __id FROM resource WHERE __id < 10 ORDER BY __id",
    "SELECT unnest([1, 2, 3]) AS x",
    "SELECT * FROM generate_series(1, 5)",
    "FROM resource SELECT count(*)",
    "SELECT r.__id FROM resource r LIMIT 1",
    # identifiers are case insensitive in DuckDB
    "SELECT count(*) FROM Resource",
    "SELECT count(*) FROM MAIN.resource",
    "SELECT count(*) FROM TEMP.main.resource",
]


@pytest.mark.parametrize(
    "statement",
    WRITE_STATEMENTS + FILE_ACCESS_STATEMENTS + CATALOG_STATEMENTS + MULTI_STATEMENTS,
)
def test_rejected_statements(duckdb_connection, statement):
    """Every query here must be rejected before DuckDB executes anything."""
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
        'SELECT count_star() FROM "resource"'
    )


def test_executed_sql_is_rebuilt_from_the_validated_tree(duckdb_connection):
    """What runs is DuckDB's own rendering of the tree, so trailing comments cannot survive."""
    assert (
        validate_sql(duckdb_connection, "SELECT 1 AS a -- ; DROP VIEW resource") == "SELECT 1 AS a"
    )
    assert (
        validate_sql(duckdb_connection, 'SELECT "Code du département" FROM resource')
        == 'SELECT "Code du département" FROM "resource"'
    )


def test_nothing_is_executed_during_validation(duckdb_connection):
    """Validation must not create anything, even for a rejected write statement."""
    with pytest.raises(QueryException):
        validate_sql(duckdb_connection, "CREATE TABLE side_effect AS SELECT 1")
    tables = duckdb_connection.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'side_effect'"
    ).fetchone()
    assert tables[0] == 0


@pytest.fixture
def sandboxed_connection(parquet_dir, monkeypatch):
    """A connection of the SQL endpoint pool, whose only readable prefix is `parquet_dir`."""
    monkeypatch.setattr(settings, "PARQUET_BASE_URL", parquet_dir)
    connection = DuckDBEngine()._new_sql_connection()
    yield connection
    connection.close()


def test_sandbox_reads_the_parquet_prefix(sandboxed_connection, parquet_dir):
    count = sandboxed_connection.execute(
        f"SELECT count(*) FROM read_parquet('{parquet_dir}/{RESOURCE_ID}.parquet')"
    ).fetchone()
    assert count[0] > 0


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT * FROM read_text('/etc/passwd')",
        "SET enable_external_access=true",
        "SET allowed_directories=['/']",
    ],
)
def test_sandbox_denies_anything_else(sandboxed_connection, statement):
    """Even a statement that went through the validation could not escape the sandbox."""
    with pytest.raises(duckdb.Error):
        sandboxed_connection.execute(statement)


@pytest.mark.parametrize(
    "outside",
    [
        "{tmp}/outside.parquet",
        # the prefix is a directory, not the start of a path
        "{prefix}_private/outside.parquet",
        "{prefix}/../outside.parquet",
    ],
)
def test_sandbox_denies_files_outside_the_prefix(
    sandboxed_connection, parquet_dir, tmp_path, outside
):
    path = Path(outside.format(tmp=tmp_path, prefix=parquet_dir))
    path.parent.mkdir(exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        conn.execute(f"COPY (SELECT 1 AS a) TO '{path}' (FORMAT parquet)")
    with pytest.raises(duckdb.Error):
        sandboxed_connection.execute(f"SELECT * FROM read_parquet('{path}')")


@pytest.fixture
def remote_prefix():
    """An HTTP prefix where nothing listens: a read the sandbox lets through fails to connect."""
    with socket.create_server(("127.0.0.1", 0)) as server:
        port = server.getsockname()[1]
    return f"http://127.0.0.1:{port}/parquet"


@pytest.fixture
def remote_sandboxed_connection(remote_prefix, monkeypatch):
    monkeypatch.setattr(settings, "PARQUET_BASE_URL", remote_prefix)
    connection = DuckDBEngine()._new_sql_connection()
    yield connection
    connection.close()


def test_remote_sandbox_lets_the_prefix_through(remote_sandboxed_connection, remote_prefix):
    with pytest.raises(duckdb.IOException):
        remote_sandboxed_connection.execute(
            f"SELECT * FROM read_parquet('{remote_prefix}/x.parquet')"
        )


@pytest.mark.parametrize(
    "outside",
    [
        "{prefix}_private/x.parquet",
        "{prefix}-other/x.parquet",
        "{host}/other/x.parquet",
        "http://other-host/parquet/x.parquet",
    ],
)
def test_remote_sandbox_denies_other_prefixes_and_hosts(
    remote_sandboxed_connection, remote_prefix, outside
):
    url = outside.format(prefix=remote_prefix, host=remote_prefix.removesuffix("/parquet"))
    with pytest.raises(duckdb.PermissionException):
        remote_sandboxed_connection.execute(f"SELECT * FROM read_parquet('{url}')")
