import csv
import io

import duckdb
import pytest

from tabular_duckdb.config import settings
from tabular_duckdb.engine import engine

from .conftest import HEADER, RESOURCE_ID, TOTAL_LINES, UNKNOWN_RESOURCE_ID

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
    async def broken_query(*args, **kwargs):
        raise duckdb.ConnectionException("connection closed")

    monkeypatch.setattr(engine, "query", broken_query)
    response = client.get("/health/")
    assert response.status_code == 503
    assert response.json()["errors"][0]["title"] == "DB unavailable"


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
    assert client.get(f"/api/resources/{UNKNOWN_RESOURCE_ID}/{suffix}").status_code == 404


def test_malformed_resource_id(client):
    assert client.get("/api/resources/not-an-uuid/data/").status_code == 404


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


def test_resource_swagger(client):
    response = client.get(f"/api/resources/{RESOURCE_ID}/swagger/")
    assert response.status_code == 200
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


def test_resource_data_filter(client):
    response = client.get(DATA_URL, params={"decompte__exact": 3, "is_true__exact": "true"})
    assert response.status_code == 200
    body = response.json()
    expected = [i for i in range(1, TOTAL_LINES + 1) if i % 7 == 3 and i % 2 == 0]
    assert [row["__id"] for row in body["data"]] == expected
    assert body["meta"]["total"] == len(expected)


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


def test_resource_data_aggregation_is_rejected(client):
    response = client.get(f"{DATA_URL}?decompte__groupby")
    assert response.status_code == 400
    assert "/sql/" in response.json()["errors"][0]["detail"]


def test_resource_data_csv(client):
    response = client.get(f"{DATA_URL}csv/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    rows = list(csv.reader(io.StringIO(response.text)))
    assert rows[0] == ["__id"] + HEADER
    assert len(rows) == TOTAL_LINES + 1
    assert rows[1] == ["1", "id-1", "0.1", "1", "false", "1951-01-01", "Élu 1", '["a", "b"]']


def test_resource_data_json(client):
    response = client.get(f"{DATA_URL}json/")
    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == TOTAL_LINES
    assert rows[0] == FIRST_ROW
    assert all(list(row) == ["__id"] + HEADER for row in rows)


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


def test_sql_still_works_after_a_rejected_query(client):
    client.get(f"/api/resources/{RESOURCE_ID}/sql/", params={"q": "DELETE FROM resource"})
    response = client.get(
        f"/api/resources/{RESOURCE_ID}/sql/", params={"q": "SELECT count(*) AS n FROM resource"}
    )
    assert response.status_code == 200
    assert response.json()["data"] == [{"n": TOTAL_LINES}]


def test_doc_is_served(client):
    assert client.get("/api/doc").status_code == 200
    assert "Tabular API" in client.get("/api/openapi.json").json()["info"]["title"]
