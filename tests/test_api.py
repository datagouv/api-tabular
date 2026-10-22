import csv
import io
import shutil
from pathlib import Path

import duckdb
import pytest
import sentry_sdk
import yaml
from fastapi.testclient import TestClient
from sentry_sdk.transport import Transport

from tabular_duckdb.config import settings
from tabular_duckdb.engine import engine
from tabular_duckdb.errors import QueryException, query_timeout
from tabular_duckdb.main import app, build_link_with_page
from tabular_duckdb.resources import resource_cache

from .conftest import (
    CORRUPT_RESOURCE_ID,
    FLOATS_RESOURCE_ID,
    HEADER,
    ID_COLUMN_RESOURCE_ID,
    INSPECTED_RESOURCE_ID,
    INSPECTION,
    RARE_NULL_RESOURCE_ID,
    RARE_NULL_TOTAL_LINES,
    RESOURCE_ID,
    ROW_NUMBER_COLUMN_RESOURCE_ID,
    TOTAL_LINES,
    UNKNOWN_RESOURCE_ID,
)

DATA_URL = f"/api/resources/{RESOURCE_ID}/data/"
# its parquet file is deleted by a test, which writes it first
DELETED_RESOURCE_ID = "aaaaaaaa-3333-bbbb-4444-cccccccccccc"
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
    error = response.json()["errors"][0]
    assert error["title"] == "Storage unavailable"
    assert "127.0.0.1" not in error["detail"]


class RecordingTransport(Transport):
    """Keeps the events instead of sending them to Sentry."""

    def __init__(self, options=None):
        super().__init__(options)
        self.events = []

    def capture_envelope(self, envelope):
        if event := envelope.get_event():
            self.events.append(event)


@pytest.fixture
def sentry_events():
    sentry_sdk.init(dsn="https://key@sentry.invalid/1", transport=RecordingTransport)
    transport = sentry_sdk.get_client().transport
    assert isinstance(transport, RecordingTransport)
    yield transport.events
    sentry_sdk.init()


@pytest.fixture
def restart_on(client, monkeypatch):
    """Restart the engine on another storage: its sandbox only opens the prefix set at start."""
    original = settings.PARQUET_BASE_URL

    def restart(base_url: str) -> None:
        monkeypatch.setattr(settings, "PARQUET_BASE_URL", base_url)
        monkeypatch.setattr(resource_cache, "_entries", {})
        client.portal.call(engine.stop)
        client.portal.call(engine.start)

    yield restart
    settings.PARQUET_BASE_URL = original
    client.portal.call(engine.stop)
    client.portal.call(engine.start)


def test_storage_error_is_reported_without_the_storage_location(
    client, storage, restart_on, sentry_events
):
    restart_on(f"{storage}/500")
    response = client.get(f"/api/resources/{UNKNOWN_RESOURCE_ID}/")
    assert response.status_code == 502
    error = response.json()["errors"][0]
    assert error["title"] == "Storage error"
    assert storage not in error["detail"]
    assert [event["event_id"] for event in sentry_events] == [error["code"]]
    assert sentry_events[0]["tags"]["title"] == "Storage error"


@pytest.mark.parametrize("status", [500, 503])
def test_health_when_the_storage_fails(client, storage, monkeypatch, status):
    monkeypatch.setattr(settings, "PARQUET_BASE_URL", f"{storage}/{status}")
    response = client.get("/health/")
    assert response.status_code == 503
    assert response.json()["errors"][0]["title"] == "Storage unavailable"


def test_health_does_not_report_to_sentry(client, monkeypatch, sentry_events):
    """It is polled every few seconds: an outage would otherwise flood Sentry."""
    monkeypatch.setattr(settings, "PARQUET_BASE_URL", "http://127.0.0.1:1")
    assert client.get("/health/").status_code == 503
    assert sentry_events == []


@pytest.mark.parametrize("status", [403, 404])
def test_missing_parquet_file(client, storage, restart_on, sentry_events, status):
    """A bucket that does not grant listing answers 403 for a missing object."""
    restart_on(f"{storage}/{status}")
    assert client.get(f"/api/resources/{UNKNOWN_RESOURCE_ID}/").status_code == 404
    assert sentry_events == []


def test_parquet_file_deleted_while_its_resource_is_cached(client, parquet_dir):
    path = Path(parquet_dir) / f"{DELETED_RESOURCE_ID}.parquet"
    shutil.copy(Path(parquet_dir) / f"{RESOURCE_ID}.parquet", path)
    assert client.get(f"/api/resources/{DELETED_RESOURCE_ID}/data/").status_code == 200
    path.unlink()
    assert client.get(f"/api/resources/{DELETED_RESOURCE_ID}/data/").status_code == 404


def test_sql_on_a_missing_file_is_the_client_error(client, http_parquet_dir, sentry_events):
    """The query names the file itself: neither a storage failure nor a 404 of the resource."""
    query = f"SELECT count(*) AS n FROM '{http_parquet_dir}/{UNKNOWN_RESOURCE_ID}.parquet'"
    response = client.get(f"/api/resources/{RESOURCE_ID}/sql/", params={"q": query})
    assert response.status_code == 400
    assert response.json()["errors"][0]["title"] == "Invalid SQL query"
    assert sentry_events == []


def test_storage_failing_while_the_parquet_file_is_read(client, storage, restart_on, sentry_events):
    """The HEAD succeeds, then the storage fails the reads of DuckDB: a 502, not the file's fault."""
    restart_on(f"{storage}/200")
    response = client.get(f"/api/resources/{UNKNOWN_RESOURCE_ID}/")
    assert response.status_code == 502
    error = response.json()["errors"][0]
    assert error["title"] == "Storage error"
    assert storage not in error["detail"]
    assert [event["event_id"] for event in sentry_events] == [error["code"]]


def test_cors_headers_on_a_server_error(monkeypatch):
    async def broken(resource_id):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(resource_cache, "get", broken)
    # outside of its context manager, so that the application started by `client` is reused
    response = TestClient(app, raise_server_exceptions=False).get(f"/api/resources/{RESOURCE_ID}/")
    assert response.status_code == 500
    assert response.headers["access-control-allow-origin"] == "*"


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


def test_exports_open_at_once_are_bounded(client, monkeypatch):
    """An open export keeps its result between chunks, at the pace of its client."""
    monkeypatch.setattr(settings, "QUERY_TIMEOUT", 1)
    exports = [engine.stream("SELECT 1 AS n") for _ in range(settings.QUERY_WORKERS)]

    async def first_chunk(export):
        return await anext(export)

    for export in exports:
        client.portal.call(first_chunk, export)
    try:
        response = client.get(f"{DATA_URL}csv/")
    finally:
        for export in exports:
            client.portal.call(export.aclose)
    assert response.status_code == 503
    assert response.json()["errors"][0]["title"] == "Server busy"
    assert client.get(f"{DATA_URL}csv/").status_code == 200


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
        # the Last-Modified header of the object
        "created_at": "2024-01-01T12:00:00+00:00",
        "url": f"https://www.data.gouv.fr/api/1/datasets/r/{RESOURCE_ID}",
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


def test_resource_id_with_a_trailing_newline(client):
    """`$` would match before the newline, which then breaks the HEAD on the storage."""
    response = client.get(f"/api/resources/{RESOURCE_ID}%0A/data/")
    assert response.status_code == 404


def test_cors_headers_without_origin(client):
    """The same headers whatever the origin, as the Nginx cache serves a response to them all."""
    response = client.get(DATA_URL)
    assert response.headers["access-control-allow-origin"] == "*"
    assert response.headers["access-control-expose-headers"] == "*"


@pytest.mark.parametrize("headers", [{}, {"Origin": "https://www.data.gouv.fr"}])
def test_cors_preflight(client, headers):
    response = client.options(DATA_URL, headers=headers)
    assert response.status_code == 204
    assert response.headers["access-control-allow-origin"] == "*"
    assert response.headers["access-control-allow-methods"] == "GET, OPTIONS"


@pytest.mark.parametrize("suffix", ["", "data/", "sql/?q=SELECT 1 AS n FROM resource"])
def test_invalid_resource_file(client, http_parquet_dir, suffix):
    response = client.get(f"/api/resources/{CORRUPT_RESOURCE_ID}/{suffix}")
    assert response.status_code == 400
    error = response.json()["errors"][0]
    assert error["title"] == "Invalid resource file"
    # the storage location is not disclosed
    assert http_parquet_dir not in error["detail"]
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
    # a nested column, which the parquet footer only describes through its leaves
    assert profile["profile"]["liste"]["nb_missing_values"] == 0


def test_resource_profile_counts_rare_missing_values_across_row_groups(client, parquet_dir):
    path = f"{parquet_dir}/{RARE_NULL_RESOURCE_ID}.parquet"
    with duckdb.connect(":memory:") as conn:
        [(row_groups,)] = conn.execute(
            f"SELECT num_row_groups FROM parquet_file_metadata('{path}')"
        ).fetchall()
    assert row_groups > 1
    response = client.get(f"/api/resources/{RARE_NULL_RESOURCE_ID}/profile/")
    profile = response.json()["profile"]
    assert profile["total_lines"] == RARE_NULL_TOTAL_LINES
    assert profile["profile"]["value"]["nb_missing_values"] == 2


def test_resource_profile_without_statistics(client, monkeypatch):
    monkeypatch.setattr(settings, "PROFILE_STATS", False)
    profile = client.get(f"/api/resources/{RESOURCE_ID}/profile/").json()["profile"]
    assert profile["total_lines"] == TOTAL_LINES
    assert profile["profile"] == {}


def test_resource_profile_of_a_file_too_big_to_scan_in_time(client, monkeypatch):
    """Only the statistics scan the file: when that times out, the profile comes without them."""
    # loading the resource only describes the file, it is not part of what times out here
    assert client.get(f"/api/resources/{RESOURCE_ID}/").status_code == 200
    query = engine.query

    async def scans_time_out(sql, params=None):
        if "read_parquet" in sql:
            raise query_timeout(settings.QUERY_TIMEOUT)
        return await query(sql, params)

    monkeypatch.setattr(engine, "query", scans_time_out)
    response = client.get(f"/api/resources/{RESOURCE_ID}/profile/")
    assert response.status_code == 200
    profile = response.json()["profile"]
    assert profile["header"] == HEADER
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


def test_a_column_named_like_the_row_id_is_replaced_by_it(client):
    """`__id` stays the row number of the API, instead of the file's column hiding it."""
    response = client.get(f"/api/resources/{ID_COLUMN_RESOURCE_ID}/data/")
    assert response.status_code == 200
    assert response.json()["data"] == [
        {"__id": 1, "value": 0},
        {"__id": 2, "value": 1},
        {"__id": 3, "value": 2},
    ]


def test_a_column_named_like_the_row_numbers_is_refused_once(client, sentry_events):
    """DuckDB cannot number the rows of such a file: a clear error, not a failure per query."""
    response = client.get(f"/api/resources/{ROW_NUMBER_COLUMN_RESOURCE_ID}/data/")
    assert response.status_code == 400
    error = response.json()["errors"][0]
    assert error["title"] == "Invalid resource file"
    assert "File_Row_Number" in error["detail"]
    assert sentry_events == []


def test_resource_data_database_error(client, http_parquet_dir):
    """An error only DuckDB detects, such as the average of a text column."""
    response = client.get(f"{DATA_URL}?id__avg")
    assert response.status_code == 400
    error = response.json()["errors"][0]
    assert error["title"] == "Database error"
    # the storage location is not disclosed
    assert http_parquet_dir not in error["detail"]


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
def test_resource_data_download_failing_once_run(client, output):
    """The count passes, the query fails: the error must come before a 200 is sent."""
    response = client.get(f"{DATA_URL}{output}/?decompte__sum&id__sort=asc")
    assert response.status_code == 400
    assert response.json()["errors"][0]["title"] == "Database error"


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


@pytest.mark.parametrize("table", ["Resource", "RESOURCE", "MAIN.resource", "TEMP.main.resource"])
def test_sql_table_name_is_case_insensitive(client, table):
    """Like every identifier in DuckDB."""
    response = client.get(
        f"/api/resources/{RESOURCE_ID}/sql/", params={"q": f"SELECT count(*) AS n FROM {table}"}
    )
    assert response.status_code == 200
    assert response.json()["data"] == [{"n": TOTAL_LINES}]


def test_sql_missing_query(client):
    assert client.get(f"/api/resources/{RESOURCE_ID}/sql/").status_code == 400


@pytest.mark.parametrize(
    "query, function",
    [
        # one 300 MB value, which the rows fetched into Python would hold in full
        ("SELECT repeat('x', 300000000) AS s", "repeat"),
        ("SELECT string_agg(id, ' ') AS s FROM resource", "string_agg"),
        ("SELECT string_agg(id, ' ') OVER () AS s FROM resource", "string_agg"),
        ("SELECT list(id) AS s FROM resource", "list"),
        ("SELECT id || id AS s FROM resource", "||"),
        ("SELECT * FROM range(1000000000)", "range"),
    ],
)
def test_sql_refuses_the_functions_fabricating_data(client, query, function):
    """An answer may only hold values of the file: its size is then bounded by the data."""
    response = client.get(f"/api/resources/{RESOURCE_ID}/sql/", params={"q": query})
    assert response.status_code == 400
    assert function in response.json()["errors"][0]["detail"]


def test_sql_basic_processing(client):
    query = (
        "SELECT year(birth) AS y, count(*) AS n, round(avg(score), 1) AS score, max(decompte) AS m "
        "FROM resource WHERE id ILIKE 'id-1%' GROUP BY 1 ORDER BY 1 LIMIT 2"
    )
    response = client.get(f"/api/resources/{RESOURCE_ID}/sql/", params={"q": query})
    assert response.status_code == 200
    assert response.json()["data"] == [
        {"y": 1951, "n": 1, "score": 0.1, "m": 1},
        {"y": 1960, "n": 1, "score": 0.0, "m": 3},
    ]


def test_sql_when_every_query_slot_is_taken(client, monkeypatch):
    """When slow queries hold every slot, the next ones must not wait forever."""
    monkeypatch.setattr(settings, "SQL_TIMEOUT", 1)
    slots = settings.QUERY_WORKERS
    for _ in range(slots):
        client.portal.call(engine._slots.acquire)
    try:
        response = client.get(
            f"/api/resources/{RESOURCE_ID}/sql/", params={"q": "SELECT 1 AS n FROM resource"}
        )
    finally:
        for _ in range(slots):
            client.portal.call(engine._slots.release)
    assert response.status_code == 503
    assert response.json()["errors"][0]["title"] == "Server busy"


def test_sql_timeout_then_the_next_query_runs(client, monkeypatch):
    monkeypatch.setattr(settings, "SQL_TIMEOUT", 1)
    response = client.get(
        f"/api/resources/{RESOURCE_ID}/sql/",
        # 60^6 rows to count: with the allowed functions, only the resource can produce rows
        params={
            "q": "SELECT count(*) AS n FROM resource a, resource b, resource c, resource d, resource e, resource f"
        },
    )
    assert response.status_code == 400
    assert response.json()["errors"][0]["title"] == "Query timeout"
    response = client.get(
        f"/api/resources/{RESOURCE_ID}/sql/", params={"q": "SELECT count(*) AS n FROM resource"}
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
