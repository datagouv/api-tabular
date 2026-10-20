import csv
import io

import duckdb
import pytest
import yaml

from tabular_duckdb.config import settings
from tabular_duckdb.engine import engine
from tabular_duckdb.errors import QueryException
from tabular_duckdb.main import build_link_with_page

from .conftest import (
    CORRUPT_RESOURCE_ID,
    FLOATS_RESOURCE_ID,
    HEADER,
    INSPECTED_RESOURCE_ID,
    INSPECTION,
    RARE_NULL_RESOURCE_ID,
    RARE_NULL_TOTAL_LINES,
    RESOURCE_ID,
    TOTAL_LINES,
    UNKNOWN_RESOURCE_ID,
)

DATA_URL = f"/api/resources/{RESOURCE_ID}/data/"
FIRST_ROW = {
    "__id": 1,
    "id": "id-1",
    "score": 0.1,
    "decompte": 1,
    "is_true": False,
    "birth": "1951-01-01",
    "Nom de l'élu": "Élu 1",
    "liste": ["a", "b"],
}


def test_health(client):
    response = client.get("/health/")
    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"status", "version", "uptime_since"}
    assert body["status"] == "ok"


def test_health_when_duckdb_is_down(client, monkeypatch):
    async def broken_ping():
        raise duckdb.ConnectionException("connection closed")

    monkeypatch.setattr(engine, "ping", broken_ping)
    response = client.get("/health/")
    assert response.status_code == 503
    assert response.json()["errors"][0]["title"] == "DB unavailable"


def test_health_when_the_storage_denies_the_base_url(client, storage, monkeypatch):
    """A 403 on the base URL still proves that the storage answers."""
    monkeypatch.setattr(settings, "PARQUET_BASE_URL", f"{storage}/403")
    assert client.get("/health/").status_code == 200


def test_health_when_the_storage_is_unreachable(client, monkeypatch):
    monkeypatch.setattr(settings, "PARQUET_BASE_URL", "http://127.0.0.1:1")
    response = client.get("/health/")
    assert response.status_code == 503
    assert response.json()["errors"][0]["title"] == "Storage unavailable"


def test_health_when_the_storage_never_answers(client, silent_storage, monkeypatch):
    monkeypatch.setattr(settings, "PARQUET_BASE_URL", silent_storage)
    monkeypatch.setattr(settings, "HTTP_TIMEOUT", 1)
    response = client.get("/health/")
    assert response.status_code == 503
    assert response.json()["errors"][0]["title"] == "Storage unavailable"


def test_query_timeout(client, monkeypatch):
    monkeypatch.setattr(settings, "QUERY_TIMEOUT", 1)
    with pytest.raises(QueryException) as raised:
        client.portal.call(engine.query, "SELECT sum(hash(range)) FROM range(10000000000)")
    assert raised.value.status == 400
    assert raised.value.title == "Query timeout"


def test_health_and_data_when_every_query_slot_is_taken(client, monkeypatch):
    """Slow /data/ queries must neither make /health/ fail nor queue the others forever."""
    monkeypatch.setattr(settings, "QUERY_TIMEOUT", 1)
    slots = settings.QUERY_WORKERS
    for _ in range(slots):
        client.portal.call(engine._slots.acquire)
    try:
        assert client.get("/health/").status_code == 200
        response = client.get(DATA_URL)
    finally:
        for _ in range(slots):
            client.portal.call(engine._slots.release)
    assert response.status_code == 503
    assert response.json()["errors"][0]["title"] == "Server busy"


def test_resource_meta(client, base_url):
    response = client.get(f"/api/resources/{RESOURCE_ID}/")
    assert response.status_code == 200
    assert response.json() == {
        # local files have no Last-Modified header to report
        "created_at": None,
        "url": None,
        "links": [
            {
                "href": f"{base_url}/api/resources/{RESOURCE_ID}/profile/",
                "type": "GET",
                "rel": "profile",
            },
            {
                "href": f"{base_url}/api/resources/{RESOURCE_ID}/data/",
                "type": "GET",
                "rel": "data",
            },
            {
                "href": f"{base_url}/api/resources/{RESOURCE_ID}/swagger/",
                "type": "GET",
                "rel": "swagger",
            },
            {
                "href": f"{base_url}/api/resources/{RESOURCE_ID}/sql/",
                "type": "GET",
                "rel": "sql",
            },
        ],
    }


@pytest.mark.parametrize("suffix", ["", "profile/", "swagger/", "data/", "data/csv/", "sql/"])
def test_unknown_resource(client, suffix):
    response = client.get(f"/api/resources/{UNKNOWN_RESOURCE_ID}/{suffix}")
    assert response.status_code == 404
    # the plain text body of aiohttp, which served the PostgREST-based API
    assert response.headers["content-type"].startswith("text/plain")
    assert response.text == "404: Not Found"


def test_malformed_resource_id(client):
    response = client.get("/api/resources/not-an-uuid/data/")
    assert response.status_code == 404
    assert response.text == "404: Not Found"


@pytest.mark.parametrize("suffix", ["", "data/", "sql/?q=SELECT 1 AS n FROM resource"])
def test_invalid_resource_file(client, parquet_dir, suffix):
    response = client.get(f"/api/resources/{CORRUPT_RESOURCE_ID}/{suffix}")
    assert response.status_code == 400
    error = response.json()["errors"][0]
    assert error["title"] == "Invalid resource file"
    # the storage location is not disclosed
    assert parquet_dir not in error["detail"]
    assert f"<resource>/{CORRUPT_RESOURCE_ID}.parquet" in error["detail"]


@pytest.mark.parametrize(
    "path",
    ["/health/", f"/api/resources/{RESOURCE_ID}/", f"/api/resources/{RESOURCE_ID}/data/"],
)
def test_head_requests(client, path):
    response = client.head(path)
    assert response.status_code == 200
    assert response.content == b""


def test_resource_profile(client):
    response = client.get(f"/api/resources/{RESOURCE_ID}/profile/")
    assert response.status_code == 200
    body = response.json()
    assert body["indexes"] is None
    profile = body["profile"]
    assert profile["header"] == HEADER
    assert profile["total_lines"] == TOTAL_LINES
    assert {name: column["python_type"] for name, column in profile["columns"].items()} == {
        "id": "string",
        "score": "float",
        "decompte": "int",
        "is_true": "bool",
        "birth": "date",
        "Nom de l'élu": "string",
        "liste": "json",
    }
    # SUMMARIZE renders min and max as strings, whatever the column type
    assert profile["profile"]["decompte"]["min"] == "0"
    assert profile["profile"]["decompte"]["max"] == "6"
    assert profile["profile"]["Nom de l'élu"]["nb_missing_values"] == TOTAL_LINES // 5
    assert profile["profile"]["decompte"]["nb_missing_values"] == 0


def test_resource_profile_counts_a_rare_missing_value(client):
    response = client.get(f"/api/resources/{RARE_NULL_RESOURCE_ID}/profile/")
    profile = response.json()["profile"]
    assert profile["total_lines"] == RARE_NULL_TOTAL_LINES
    assert profile["profile"]["value"]["nb_missing_values"] == 1


def test_resource_profile_without_statistics(client, monkeypatch):
    monkeypatch.setattr(settings, "PROFILE_STATS", False)
    profile = client.get(f"/api/resources/{RESOURCE_ID}/profile/").json()["profile"]
    assert profile["total_lines"] == TOTAL_LINES
    assert profile["profile"] == {}


def test_resource_profile_from_the_parquet_inspection(client):
    """The csv-detective inspection embedded by hydra is served as is."""
    response = client.get(f"/api/resources/{INSPECTED_RESOURCE_ID}/profile/")
    assert response.status_code == 200
    assert response.json() == {"profile": INSPECTION, "indexes": None}


def test_resource_swagger_uses_the_inspection_types(client):
    """`liste` is stored as a string but csv-detective typed it as json: null checks only."""
    response = client.get(f"/api/resources/{INSPECTED_RESOURCE_ID}/swagger/")
    swagger = yaml.safe_load(response.text)
    names = {
        p["name"]
        for p in swagger["paths"][f"/api/resources/{INSPECTED_RESOURCE_ID}/data/"]["parameters"]
    }
    assert {name for name in names if name.startswith("liste__")} == {
        "liste__isnull",
        "liste__isnotnull",
    }
    assert "code__contains" in names


def test_resource_data_with_a_parquet_inspection(client):
    response = client.get(f"/api/resources/{INSPECTED_RESOURCE_ID}/data/")
    assert response.status_code == 200
    body = response.json()
    assert body["data"] == [
        {"__id": 1, "code": "62", "liste": "[1, 2]", "n": 3},
        {"__id": 2, "code": "59", "liste": None, "n": 4},
    ]
    assert body["meta"]["total"] == 2


def test_resource_swagger(client):
    response = client.get(f"/api/resources/{RESOURCE_ID}/swagger/")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert f"/api/resources/{RESOURCE_ID}/data/" in response.text


def test_resource_data(client, base_url):
    response = client.get(DATA_URL)
    assert response.status_code == 200
    body = response.json()
    assert body["data"][0] == FIRST_ROW
    assert [row["__id"] for row in body["data"]] == list(range(1, 21))
    assert body["links"] == {
        "next": f"{base_url}{DATA_URL}?page=2&page_size=20",
        "prev": None,
        "profile": f"{base_url}/api/resources/{RESOURCE_ID}/profile/",
        "swagger": f"{base_url}/api/resources/{RESOURCE_ID}/swagger/",
    }
    assert body["meta"] == {"page": 1, "page_size": 20, "total": TOTAL_LINES}


@pytest.mark.parametrize("args", [{"page": 2}, {"page_size": 40}, {"page": 4, "page_size": 5}])
def test_resource_data_with_meta_args(client, base_url, args):
    page = args.get("page", 1)
    page_size = args.get("page_size", settings.PAGE_SIZE_DEFAULT)
    response = client.get(DATA_URL, params=args)
    assert response.status_code == 200
    body = response.json()
    first = (page - 1) * page_size + 1
    assert [row["__id"] for row in body["data"]] == list(range(first, first + page_size))
    assert body["links"] == {
        "next": f"{base_url}{DATA_URL}?page={page + 1}&page_size={page_size}",
        "prev": f"{base_url}{DATA_URL}?page={page - 1}&page_size={page_size}" if page > 1 else None,
        "profile": f"{base_url}/api/resources/{RESOURCE_ID}/profile/",
        "swagger": f"{base_url}/api/resources/{RESOURCE_ID}/swagger/",
    }
    assert body["meta"] == {"page": page, "page_size": page_size, "total": TOTAL_LINES}


def test_pagination_links_keep_filters_on_columns_named_page():
    link = build_link_with_page(DATA_URL, ["pages__exact=3", "page=2", "page_size=5"], 3, 5)
    assert link.endswith(f"{DATA_URL}?pages__exact=3&page=3&page_size=5")


def test_resource_data_last_page_has_no_next(client):
    body = client.get(DATA_URL, params={"page": 3}).json()
    assert body["links"]["next"] is None


def test_resource_data_with_page_size_error(client):
    response = client.get(DATA_URL, params={"page": 1, "page_size": settings.PAGE_SIZE_MAX + 1})
    assert response.status_code == 400
    assert response.json() == {
        "errors": [
            {
                "code": None,
                "title": "Invalid query string",
                "detail": f"Page size exceeds allowed maximum: {settings.PAGE_SIZE_MAX}",
            }
        ]
    }


@pytest.mark.parametrize(
    "name, value, detail",
    [
        ("page", "0", "`page` must be greater than 0"),
        ("page", "-3", "`page` must be greater than 0"),
        ("page_size", "0", "`page_size` must be greater than 0"),
        ("page_size", "-1", "`page_size` must be greater than 0"),
        ("page", "abc", "`page` must be an integer, got 'abc'"),
    ],
)
def test_resource_data_with_invalid_page(client, name, value, detail):
    response = client.get(DATA_URL, params={name: value})
    assert response.status_code == 400
    assert response.json() == {
        "errors": [{"code": None, "title": "Invalid query string", "detail": detail}]
    }


def test_resource_data_filter(client):
    response = client.get(DATA_URL, params={"decompte__exact": 3, "is_true__exact": "true"})
    assert response.status_code == 200
    body = response.json()
    expected = [i for i in range(1, TOTAL_LINES + 1) if i % 7 == 3 and i % 2 == 0]
    assert [row["__id"] for row in body["data"]] == expected
    assert body["meta"]["total"] == len(expected)


@pytest.mark.parametrize(
    "argument",
    [
        "limit=1",
        # a missing `&` makes an argument with two `=`
        "TESTCOLUM_NAME__EXACT=BIDULEpage=1",
    ],
)
def test_resource_data_with_unparseable_argument(client, argument):
    response = client.get(f"{DATA_URL}?{argument}")
    assert response.status_code == 400
    assert response.json() == {
        "errors": [
            {
                "code": None,
                "title": "Invalid query string",
                "detail": f"Malformed query: argument '{argument}' could not be parsed",
            }
        ]
    }


def test_resource_data_contains_wildcard(client):
    """PostgREST treats `*` as a wildcard in `ilike` patterns."""
    response = client.get(DATA_URL, params={"id__contains": "d-*0"})
    assert response.json()["meta"]["total"] == len(
        [i for i in range(1, TOTAL_LINES + 1) if i % 10 == 0]
    )


def test_resource_data_value_not_castable(client):
    response = client.get(DATA_URL, params={"decompte__greater": "12.5"})
    assert response.status_code == 400


def test_resource_data_database_error(client):
    """An error only DuckDB detects, such as the average of a text column."""
    response = client.get(f"{DATA_URL}?id__avg")
    assert response.status_code == 400
    assert response.json()["errors"][0]["title"] == "Database error"


def test_resource_data_or_group(client):
    response = client.get(DATA_URL, params={"or": '("Nom de l\'élu"__isnull,decompte__exact.6)'})
    assert response.status_code == 200
    expected = [i for i in range(1, TOTAL_LINES + 1) if i % 5 == 0 or i % 7 == 6]
    assert response.json()["meta"]["total"] == len(expected)


def test_resource_data_columns(client):
    response = client.get(DATA_URL, params={"columns": "id,decompte", "page_size": 1})
    assert response.json()["data"] == [{"id": "id-1", "decompte": 1}]


def test_resource_data_unknown_column(client):
    response = client.get(DATA_URL, params={"nope__exact": 1})
    assert response.status_code == 400
    assert "unknown column" in response.json()["errors"][0]["detail"]


@pytest.mark.parametrize(
    "page, page_size, groups, has_next",
    [
        (1, 3, [0, 1, 2], True),
        (3, 3, [6], False),
        # like the PostgREST-based API, a full page always links to the next one
        (1, 7, list(range(7)), True),
    ],
)
def test_resource_data_aggregation(client, page, page_size, groups, has_next):
    response = client.get(
        f"{DATA_URL}?decompte__groupby&id__count&page={page}&page_size={page_size}"
    )
    assert response.status_code == 200
    body = response.json()
    assert body["data"] == [
        {
            "decompte": group,
            "id__count": sum(1 for i in range(1, TOTAL_LINES + 1) if i % 7 == group),
        }
        for group in groups
    ]
    # the total of an aggregation is not computed
    assert body["meta"] == {"page": page, "page_size": page_size}
    assert (body["links"]["next"] is not None) == has_next


def test_resource_data_groupby_alone(client):
    response = client.get(f"{DATA_URL}?decompte__groupby&page_size=2")
    assert response.status_code == 200
    body = response.json()
    assert body["data"] == [{"decompte": 1}, {"decompte": 2}]
    assert body["meta"]["total"] == TOTAL_LINES


def test_resource_data_aggregation_with_columns(client):
    response = client.get(f"{DATA_URL}?decompte__groupby&columns=decompte")
    assert response.status_code == 400
    assert response.json()["errors"][0] == {
        "code": None,
        "title": "Invalid query string",
        "detail": "Malformed query: the argument `columns` cannot be set alongside aggregators",
    }


def test_resource_data_csv_aggregation(client):
    response = client.get(f"{DATA_URL}csv/?is_true__groupby&decompte__sum")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.text)))
    expected_sum = {
        value: sum(i % 7 for i in range(1, TOTAL_LINES + 1) if (i % 2 == 0) == value)
        for value in (False, True)
    }
    assert rows == [
        ["is_true", "decompte__sum"],
        ["false", str(expected_sum[False])],
        ["true", str(expected_sum[True])],
    ]


def test_aggregation_exceptions(client):
    response = client.get("/api/aggregation-exceptions/")
    assert response.status_code == 200
    assert response.json() == {"allowed": True, "exceptions": []}


def test_resource_data_csv(client):
    response = client.get(f"{DATA_URL}csv/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    rows = list(csv.reader(io.StringIO(response.text)))
    assert rows[0] == ["__id"] + HEADER
    assert len(rows) == TOTAL_LINES + 1
    assert rows[1] == ["1", "id-1", "0.1", "1", "false", "1951-01-01", "Élu 1", '["a", "b"]']


def test_resource_data_csv_non_finite_floats(client):
    """Rendered like PostgreSQL does, which is also what pandas reads back as floats."""
    response = client.get(f"/api/resources/{FLOATS_RESOURCE_ID}/data/csv/")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.text)))
    assert rows == [
        ["__id", "value"],
        ["1", "NaN"],
        ["2", "Infinity"],
        ["3", "-Infinity"],
        ["4", ""],
        ["5", "1.5"],
    ]


def test_resource_data_json(client):
    response = client.get(f"{DATA_URL}json/")
    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == TOTAL_LINES
    assert rows[0] == FIRST_ROW
    assert all(list(row) == ["__id"] + HEADER for row in rows)
    assert response.text.endswith("]\n")


def test_resource_data_downloads_in_several_chunks(client, monkeypatch):
    monkeypatch.setattr(settings, "STREAM_CHUNK_SIZE", 7)
    rows = client.get(f"{DATA_URL}json/").json()
    assert [row["__id"] for row in rows] == list(range(1, TOTAL_LINES + 1))
    lines = list(csv.reader(io.StringIO(client.get(f"{DATA_URL}csv/").text)))
    assert lines[0] == ["__id"] + HEADER
    assert [line[0] for line in lines[1:]] == [str(i) for i in range(1, TOTAL_LINES + 1)]


def test_resource_data_downloads_without_any_row(client):
    """The CSV keeps its header, so that it can still be read with its columns."""
    csv_response = client.get(f"{DATA_URL}csv/?decompte__exact=99")
    assert list(csv.reader(io.StringIO(csv_response.text))) == [["__id"] + HEADER]
    assert client.get(f"{DATA_URL}json/?decompte__exact=99").json() == []


@pytest.mark.parametrize("suffix", ["data/", "data/json/"])
def test_resource_data_non_finite_floats_in_json(client, suffix):
    """NaN and infinities are not valid JSON."""
    response = client.get(f"/api/resources/{FLOATS_RESOURCE_ID}/{suffix}")
    assert response.status_code == 200
    body = response.json()
    rows = body["data"] if suffix == "data/" else body
    assert [row["value"] for row in rows] == [None, None, None, None, 1.5]


@pytest.mark.parametrize("output", ["csv", "json"])
def test_resource_data_download_too_long(client, monkeypatch, output):
    monkeypatch.setattr(settings, "BATCH_SIZE", 5)
    response = client.get(f"{DATA_URL}{output}/")
    assert response.status_code == 403
    assert response.json()["errors"][0]["title"] == "Output is too long"


def test_sql_group_by(client):
    response = client.get(
        f"/api/resources/{RESOURCE_ID}/sql/",
        params={"q": "SELECT decompte, count(*) AS n FROM resource GROUP BY 1 ORDER BY 1"},
    )
    assert response.status_code == 200
    body = response.json()
    counts = {d: len([i for i in range(1, TOTAL_LINES + 1) if i % 7 == d]) for d in range(7)}
    assert body["data"] == [{"decompte": d, "n": n} for d, n in counts.items()]
    assert body["meta"]["truncated"] is False
    assert body["meta"]["table"] == "resource"


def test_sql_row_limit(client, monkeypatch):
    monkeypatch.setattr(settings, "SQL_MAX_ROWS", 10)
    response = client.get(
        f"/api/resources/{RESOURCE_ID}/sql/", params={"q": "SELECT __id FROM resource"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["truncated"] is True
    assert body["meta"]["rows"] == body["meta"]["row_limit"] == 10


def test_sql_cte(client):
    response = client.get(
        f"/api/resources/{RESOURCE_ID}/sql/",
        params={
            "q": "WITH counts AS (SELECT decompte, count(*) AS n FROM resource GROUP BY 1) "
            "SELECT count(*) AS groups, sum(n) AS total FROM counts"
        },
    )
    assert response.status_code == 200
    assert response.json()["data"] == [{"groups": 7, "total": TOTAL_LINES}]


def test_sql_missing_query(client):
    assert client.get(f"/api/resources/{RESOURCE_ID}/sql/").status_code == 400


@pytest.mark.parametrize(
    "query",
    [
        "CREATE TABLE evil AS SELECT 1",
        "DELETE FROM resource",
        "COPY (SELECT 1) TO '/tmp/evil.csv'",
        "SELECT * FROM read_csv('/etc/passwd')",
        f"SELECT * FROM read_parquet('<base>/{RESOURCE_ID}.parquet')",
        "SELECT * FROM duckdb_secrets()",
        "SELECT 1; DROP VIEW resource",
        "PRAGMA database_list",
        "SET disabled_filesystems=''",
        "SELECT * FROM other_table",
    ],
)
def test_sql_rejects_dangerous_queries(client, parquet_dir, query):
    # even its own parquet file cannot be reached through a table function
    query = query.replace("<base>", parquet_dir)
    response = client.get(f"/api/resources/{RESOURCE_ID}/sql/", params={"q": query})
    assert response.status_code == 400
    assert response.json()["errors"][0]["title"] == "Invalid SQL query"


def test_sql_busy_pool(client, monkeypatch):
    """When every connection is held by slow queries, the next ones must not wait forever."""
    monkeypatch.setattr(settings, "SQL_TIMEOUT", 1)
    pool = engine._sql_pool
    held = [pool.get_nowait() for _ in range(pool.qsize())]
    try:
        response = client.get(
            f"/api/resources/{RESOURCE_ID}/sql/", params={"q": "SELECT 1 AS n FROM resource"}
        )
    finally:
        for conn in held:
            pool.put_nowait(conn)
    assert response.status_code == 503
    assert response.json()["errors"][0]["title"] == "Server busy"


def test_sql_rejects_a_cte_referenced_before_its_declaration(client, parquet_dir):
    """DuckDB would bind the name to its system view, which holds the storage location."""
    query = (
        "WITH x AS (SELECT sql FROM duckdb_views), duckdb_views AS (SELECT 1 AS sql) "
        "SELECT * FROM x"
    )
    response = client.get(f"/api/resources/{RESOURCE_ID}/sql/", params={"q": query})
    assert response.status_code == 400
    assert parquet_dir not in response.text


def test_sql_timeout_then_the_connection_is_reused(client, monkeypatch):
    monkeypatch.setattr(settings, "SQL_TIMEOUT", 1)
    response = client.get(
        f"/api/resources/{RESOURCE_ID}/sql/",
        params={"q": "SELECT sum(hash(range)) AS n FROM range(10000000000)"},
    )
    assert response.status_code == 400
    assert response.json()["errors"][0]["title"] == "Query timeout"
    # every connection of the pool, the interrupted one included, still answers
    for _ in range(settings.SQL_POOL_SIZE):
        response = client.get(
            f"/api/resources/{RESOURCE_ID}/sql/",
            params={"q": "SELECT count(*) AS n FROM resource"},
        )
        assert response.json()["data"] == [{"n": TOTAL_LINES}]


def test_sql_still_works_after_a_rejected_query(client):
    client.get(f"/api/resources/{RESOURCE_ID}/sql/", params={"q": "DELETE FROM resource"})
    response = client.get(
        f"/api/resources/{RESOURCE_ID}/sql/", params={"q": "SELECT count(*) AS n FROM resource"}
    )
    assert response.status_code == 200
    assert response.json()["data"] == [{"n": TOTAL_LINES}]


def test_doc_is_served(client):
    assert client.get("/api/doc").status_code == 200
    # the path where the PostgREST-based API served its spec
    assert "Tabular API" in client.get("/api/doc/swagger.json").json()["info"]["title"]
