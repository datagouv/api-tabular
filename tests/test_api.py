"""Integration tests, run against a running API (see README).

They are written against whatever parquet file `RESOURCE_ID` points to: expectations are derived
from the resource profile rather than hardcoded, so the suite works for any resource of the bucket.
"""

import csv
import io

import pytest

from .conftest import BIG_RESOURCE_ID, RESOURCE_ID, UNKNOWN_RESOURCE_ID

# even its own object cannot be reached through a table function
OWN_OBJECT_URL = f"https://hydra.s3.rbx.io.cloud.ovh.net/parquet/{RESOURCE_ID}.parquet"


def most_frequent_value(api, column: str) -> tuple[str, int]:
    """Use the SQL endpoint to get a value and its exact count, to check /data/ against it."""
    response = api.get(
        f"/api/resources/{RESOURCE_ID}/sql/",
        params={
            "q": f'SELECT "{column}" AS value, count(*) AS n FROM resource '
            "WHERE value IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT 1"
        },
    )
    assert response.status_code == 200, response.text
    row = response.json()["data"][0]
    return row["value"], row["n"]


def test_health(api):
    response = api.get("/health/")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_resource_meta(api):
    response = api.get(f"/api/resources/{RESOURCE_ID}/")
    assert response.status_code == 200
    body = response.json()
    assert body["created_at"]
    assert {link["rel"] for link in body["links"]} == {"profile", "data", "swagger", "sql"}


def test_resource_profile(api, profile):
    assert profile["total_lines"] > 0
    assert profile["header"]
    assert set(profile["columns"]) == set(profile["header"])
    assert all("python_type" in column for column in profile["columns"].values())
    # statistics are computed by SUMMARIZE
    assert any(stats.get("nb_distinct") for stats in profile["profile"].values())


def test_resource_profile_has_no_indexes(api):
    response = api.get(f"/api/resources/{RESOURCE_ID}/profile/")
    assert response.json()["indexes"] is None


def test_resource_swagger(api, string_column):
    response = api.get(f"/api/resources/{RESOURCE_ID}/swagger/")
    assert response.status_code == 200
    assert f"/api/resources/{RESOURCE_ID}/data/" in response.text
    assert f"{string_column}__exact" in response.text
    # aggregation operators are gone from the resource swagger
    assert "__groupby" not in response.text
    assert "__avg" not in response.text


def test_resource_data(api, profile):
    response = api.get(f"/api/resources/{RESOURCE_ID}/data/")
    assert response.status_code == 200
    body = response.json()
    assert len(body["data"]) == min(20, profile["total_lines"])
    assert body["data"][0]["__id"] == 1
    assert body["meta"] == {"page": 1, "page_size": 20, "total": profile["total_lines"]}
    assert body["links"]["prev"] is None
    assert "page=2" in body["links"]["next"]


def test_resource_data_pagination(api):
    response = api.get(f"/api/resources/{RESOURCE_ID}/data/?page=3&page_size=5")
    body = response.json()
    assert [row["__id"] for row in body["data"]] == [11, 12, 13, 14, 15]
    assert body["meta"]["page"] == 3
    assert "page=2" in body["links"]["prev"]


def test_resource_data_page_size_above_maximum(api):
    response = api.get(f"/api/resources/{RESOURCE_ID}/data/?page_size=5000")
    assert response.status_code == 400
    assert "Page size exceeds" in response.json()["errors"][0]["detail"]


def test_resource_data_filter_matches_sql_count(api, string_column):
    value, count = most_frequent_value(api, string_column)
    response = api.get(
        f"/api/resources/{RESOURCE_ID}/data/", params={f"{string_column}__exact": value}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] == count
    assert all(row[string_column] == value for row in body["data"])


def test_resource_data_sort(api, sortable_column):
    response = api.get(
        f"/api/resources/{RESOURCE_ID}/data/",
        params={f"{sortable_column}__sort": "asc", "page_size": 50},
    )
    assert response.status_code == 200
    values = [row[sortable_column] for row in response.json()["data"] if row[sortable_column]]
    assert values == sorted(values)


def test_resource_data_contains_and_columns(api, string_column):
    value, _ = most_frequent_value(api, string_column)
    response = api.get(
        f"/api/resources/{RESOURCE_ID}/data/",
        params={f"{string_column}__contains": value[:3].lower(), "columns": string_column},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["total"] > 0
    assert set(body["data"][0].keys()) == {string_column}


def test_resource_data_isnotnull(api, string_column):
    response = api.get(f"/api/resources/{RESOURCE_ID}/data/?{string_column}__isnotnull")
    assert response.status_code == 200
    assert all(row[string_column] is not None for row in response.json()["data"])


def test_resource_data_or_group(api, string_column):
    value, count = most_frequent_value(api, string_column)
    response = api.get(
        f"/api/resources/{RESOURCE_ID}/data/",
        params={"or": f'("{string_column}"__exact.{value},"{string_column}"__isnull)'},
    )
    assert response.status_code == 200
    assert response.json()["meta"]["total"] >= count


def test_resource_data_unknown_column(api):
    response = api.get(f"/api/resources/{RESOURCE_ID}/data/?nope__exact=1")
    assert response.status_code == 400
    assert "unknown column" in response.json()["errors"][0]["detail"]


def test_resource_data_aggregation_is_rejected(api, string_column):
    response = api.get(f"/api/resources/{RESOURCE_ID}/data/?{string_column}__groupby")
    assert response.status_code == 400
    assert "/sql/" in response.json()["errors"][0]["detail"]


def test_unknown_resource(api):
    response = api.get(f"/api/resources/{UNKNOWN_RESOURCE_ID}/data/")
    assert response.status_code == 404


def test_malformed_resource_id(api):
    response = api.get("/api/resources/not-an-uuid/data/")
    assert response.status_code == 404


def test_another_resource_of_the_bucket(api):
    """Any <resource_id>.parquet of the bucket is queryable, without any ingestion step."""
    response = api.get(f"/api/resources/{BIG_RESOURCE_ID}/data/?page_size=3")
    assert response.status_code == 200
    body = response.json()
    assert len(body["data"]) == 3
    assert body["meta"]["total"] > 0


def test_resource_data_csv(api, string_column):
    value, _ = most_frequent_value(api, string_column)
    response = api.get(
        f"/api/resources/{RESOURCE_ID}/data/csv/", params={f"{string_column}__exact": value}
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    rows = list(csv.reader(io.StringIO(response.text)))
    assert rows[0][0] == "__id"
    assert string_column in rows[0]
    assert len(rows) > 1


def test_resource_data_json_stream(api, string_column):
    value, count = most_frequent_value(api, string_column)
    response = api.get(
        f"/api/resources/{RESOURCE_ID}/data/json/", params={f"{string_column}__exact": value}
    )
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, list)
    assert len(body) == count


def test_resource_data_csv_too_long(api):
    """The big resource is above BATCH_SIZE, so the download is refused."""
    response = api.get(f"/api/resources/{BIG_RESOURCE_ID}/data/csv/")
    assert response.status_code == 403
    assert "too long" in response.json()["errors"][0]["title"].lower()


def test_sql_group_by(api, string_column):
    value, count = most_frequent_value(api, string_column)
    response = api.get(
        f"/api/resources/{RESOURCE_ID}/sql/",
        params={
            "q": f'SELECT "{string_column}" AS value, count(*) AS n FROM resource '
            "GROUP BY 1 ORDER BY n DESC LIMIT 3"
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["data"][0] == {"value": value, "n": count}
    assert body["meta"]["truncated"] is False
    assert body["meta"]["table"] == "resource"


def test_sql_row_limit(api):
    response = api.get(
        f"/api/resources/{BIG_RESOURCE_ID}/sql/", params={"q": "SELECT __id FROM resource"}
    )
    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["truncated"] is True
    assert body["meta"]["rows"] == body["meta"]["row_limit"]


def test_sql_cte_and_join_on_itself(api, string_column):
    response = api.get(
        f"/api/resources/{RESOURCE_ID}/sql/",
        params={
            "q": f'WITH counts AS (SELECT "{string_column}" AS value, count(*) AS n '
            "FROM resource GROUP BY 1) SELECT count(*) AS groups, sum(n) AS total FROM counts"
        },
    )
    assert response.status_code == 200
    assert response.json()["data"][0]["groups"] > 0


def test_sql_missing_query(api):
    response = api.get(f"/api/resources/{RESOURCE_ID}/sql/")
    assert response.status_code == 400


@pytest.mark.parametrize(
    "query",
    [
        "CREATE TABLE evil AS SELECT 1",
        "DELETE FROM resource",
        "COPY (SELECT 1) TO '/tmp/evil.csv'",
        "SELECT * FROM read_csv('/etc/passwd')",
        f"SELECT * FROM read_parquet('{OWN_OBJECT_URL}')",
        "SELECT * FROM duckdb_secrets()",
        "SELECT 1; DROP VIEW resource",
        "PRAGMA database_list",
        "SET disabled_filesystems=''",
        "SELECT * FROM other_table",
    ],
)
def test_sql_rejects_dangerous_queries(api, query):
    response = api.get(f"/api/resources/{RESOURCE_ID}/sql/", params={"q": query})
    assert response.status_code == 400
    assert response.json()["errors"][0]["title"] == "Invalid SQL query"


def test_sql_still_works_after_a_rejected_query(api, profile):
    api.get(f"/api/resources/{RESOURCE_ID}/sql/", params={"q": "DELETE FROM resource"})
    response = api.get(
        f"/api/resources/{RESOURCE_ID}/sql/", params={"q": "SELECT count(*) AS n FROM resource"}
    )
    assert response.status_code == 200
    assert response.json()["data"] == [{"n": profile["total_lines"]}]


def test_doc_is_served(api):
    assert api.get("/api/doc").status_code == 200
    assert "Tabular API" in api.get("/api/openapi.json").json()["info"]["title"]
