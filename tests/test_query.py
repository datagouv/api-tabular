from datetime import date

import pytest

from tabular_duckdb.query import build_query
from tabular_duckdb.resources import Resource

from .conftest import RESOURCE_ID, TOTAL_LINES

NAME = "Nom de l'élu"


def row(i: int) -> dict:
    """Python twin of RESOURCE_SQL, to compute the expected results."""
    return {
        "__id": i,
        "id": f"id-{i}",
        "score": (i % 10) / 10,
        "decompte": i % 7,
        "is_true": i % 2 == 0,
        "birth": date(1950 + i, 1, 1),
        NAME: None if i % 5 == 0 else f"Élu {i}",
    }


ROWS = [row(i) for i in range(1, TOTAL_LINES + 1)]


def expected_ids(predicate) -> list[int]:
    return [r["__id"] for r in ROWS if predicate(r)]


@pytest.fixture
def source(duckdb_connection, parquet_dir, monkeypatch):
    """The test resource, exposed exactly like the API does (`__id` included)."""
    monkeypatch.setattr("tabular_duckdb.resources.settings.PARQUET_BASE_URL", parquet_dir)
    resource = Resource(resource_id=RESOURCE_ID, created_at=None, total_lines=TOTAL_LINES)
    duckdb_connection.execute(f"CREATE VIEW source AS SELECT * FROM {resource.source_sql}")
    columns = {
        name: kind for name, kind, *_ in duckdb_connection.execute("DESCRIBE source").fetchall()
    }
    return duckdb_connection, columns


def run(source, query_string: list[str], page_size: int | None = None, offset: int = 0) -> list:
    conn, columns = source
    parsed = build_query(query_string, columns, page_size=page_size, offset=offset)
    sql, params = parsed.data_sql("source")
    cursor = conn.execute(sql, params)
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, values)) for values in cursor.fetchall()]


def ids(source, query_string: list[str], **kwargs) -> list[int]:
    return [r["__id"] for r in run(source, query_string, **kwargs)]


def test_query_limit(source):
    assert ids(source, [], page_size=12) == list(range(1, 13))


def test_query_offset(source):
    assert ids(source, [], page_size=12, offset=12) == list(range(13, 25))


def test_query_pagination_arguments_are_left_to_the_caller(source):
    assert ids(source, ["page=2", "page_size=20"]) == list(range(1, TOTAL_LINES + 1))


@pytest.mark.parametrize("direction", ["asc", "desc"])
def test_query_sort(source, direction):
    values = [r["decompte"] for r in run(source, [f"decompte__sort={direction}"])]
    assert values == sorted(values, reverse=direction == "desc")


def test_query_multiple_sorts(source):
    rows = run(source, ["decompte__sort=desc", "birth__sort=asc"])
    keys = [(-r["decompte"], r["birth"]) for r in rows]
    assert keys == sorted(keys)


def test_query_sort_unknown_direction(source):
    with pytest.raises(ValueError):
        run(source, ["decompte__sort=sideways"])


@pytest.mark.parametrize(
    "argument, predicate",
    [
        (f"{NAME}__exact=Élu 3", lambda r: r[NAME] == "Élu 3"),
        # IS DISTINCT FROM: NULL values do differ
        (f"{NAME}__differs=Élu 3", lambda r: r[NAME] != "Élu 3"),
        (f"{NAME}__isnull", lambda r: r[NAME] is None),
        (f"{NAME}__isnotnull", lambda r: r[NAME] is not None),
        (f"{NAME}__contains=élu 1", lambda r: r[NAME] is not None and "élu 1" in r[NAME].lower()),
        (
            f"{NAME}__notcontains=élu 1",
            lambda r: r[NAME] is not None and "élu 1" not in r[NAME].lower(),
        ),
        ("decompte__in=1,2", lambda r: r["decompte"] in (1, 2)),
        ("decompte__notin=1,2", lambda r: r["decompte"] not in (1, 2)),
        ("decompte__less=3", lambda r: r["decompte"] <= 3),
        ("decompte__greater=3", lambda r: r["decompte"] >= 3),
        ("decompte__strictly_less=3", lambda r: r["decompte"] < 3),
        ("decompte__strictly_greater=3", lambda r: r["decompte"] > 3),
        ("score__exact=0.5", lambda r: r["score"] == 0.5),
        ("is_true__exact=true", lambda r: r["is_true"]),
        ("birth__exact=1980-01-01", lambda r: r["birth"] == date(1980, 1, 1)),
        ("birth__strictly_less=1955-06-01", lambda r: r["birth"] < date(1955, 6, 1)),
    ],
)
def test_query_filters(source, argument, predicate):
    assert ids(source, [argument]) == expected_ids(predicate)


@pytest.mark.parametrize(
    "argument",
    [
        # a string comparison would silently return wrong rows ('9' >= '12.5')
        "decompte__greater=12.5",
        "decompte__exact=abc",
        "score__less=1,5",
        "birth__greater=1996",
        "is_true__exact=maybe",
        "decompte__in=1,abc",
    ],
)
def test_query_value_not_castable_to_the_column_type(source, argument):
    with pytest.raises(ValueError, match="invalid value"):
        run(source, [argument])


def test_query_multiple_filters_are_joined_with_and(source):
    assert ids(source, ["decompte__greater=3", "is_true__exact=false"]) == expected_ids(
        lambda r: r["decompte"] >= 3 and not r["is_true"]
    )


def test_query_value_is_url_decoded(source):
    assert ids(source, [f"{NAME}__exact=%C3%89lu%203"]) == [3]


def test_query_column_name_is_url_decoded(source):
    assert ids(source, ["Nom%20de%20l%27%C3%A9lu__isnull"]) == expected_ids(
        lambda r: r[NAME] is None
    )


def test_query_count_sql(source):
    conn, columns = source
    sql, params = build_query(["decompte__greater=3"], columns).count_sql("source")
    assert conn.execute(sql, params).fetchone()[0] == len(
        expected_ids(lambda r: r["decompte"] >= 3)
    )


def test_query_specify_columns(source):
    rows = run(source, ["columns=id,decompte"], page_size=1)
    assert rows == [{"id": "id-1", "decompte": 1}]


def test_query_specify_unknown_column(source):
    with pytest.raises(ValueError, match="unknown column"):
        run(source, ["columns=id,nope"])


def test_query_filter_on_unknown_column(source):
    with pytest.raises(ValueError, match="unknown column"):
        run(source, ["nope__exact=1"])


def test_query_forbidden_quote_in_columns(source):
    with pytest.raises(ValueError):
        run(source, ['columns=id,"decompte"'])


def test_query_unparseable_argument(source):
    with pytest.raises(ValueError):
        run(source, ["select=numnum"])


def test_query_operator_without_value(source):
    with pytest.raises(ValueError):
        run(source, ["decompte__exact"])


@pytest.mark.parametrize("aggregator", ["groupby", "count", "avg", "min", "max", "sum"])
def test_query_aggregators_are_rejected(source, aggregator):
    with pytest.raises(ValueError, match="sql"):
        run(source, [f"decompte__{aggregator}"])


@pytest.mark.parametrize(
    "query_string, predicate",
    [
        (
            ['or=(decompte__exact.1,score__greater."0.8")'],
            lambda r: r["decompte"] == 1 or r["score"] >= 0.8,
        ),
        (
            ["is_true__exact=true", "or=(decompte__exact.1,decompte__exact.2)"],
            lambda r: r["is_true"] and r["decompte"] in (1, 2),
        ),
        (
            [
                "or=(decompte__exact.1,decompte__exact.2)",
                'or=(score__less."0.2",score__greater."0.8")',
            ],
            lambda r: r["decompte"] in (1, 2) and (r["score"] <= 0.2 or r["score"] >= 0.8),
        ),
        (
            [f'or=(decompte__exact.0,and(score__greater."0.5",decompte__less.3),"{NAME}"__isnull)'],
            lambda r: (
                r["decompte"] == 0 or (r["score"] >= 0.5 and r["decompte"] <= 3) or r[NAME] is None
            ),
        ),
        (
            [f'or=("{NAME}"__isnotnull,or(decompte__exact.1,id__contains.-1))'],
            lambda r: r[NAME] is not None or r["decompte"] == 1 or "-1" in r["id"],
        ),
    ],
)
def test_query_or_group(source, query_string, predicate):
    assert ids(source, query_string) == expected_ids(predicate)


def test_query_or_group_with_encoded_structure(source):
    """Clients such as httpx percent-encode the parentheses, quotes and commas of the group."""
    assert ids(
        source, ["or=%28%22Nom+de+l%27%C3%A9lu%22__exact.%C3%89lu+3%2Cdecompte__exact.6%29"]
    ) == expected_ids(lambda r: r[NAME] == "Élu 3" or r["decompte"] == 6)


def test_query_or_group_with_encoded_column_name_on_isnull(source):
    """A column name still encoded (`+`, `%xx`) must be decoded, isnull terms included."""
    assert ids(
        source, ["or=(%22Nom+de+l%27%C3%A9lu%22__isnull,decompte__exact.6)"]
    ) == expected_ids(lambda r: r[NAME] is None or r["decompte"] == 6)


@pytest.mark.parametrize(
    "query_string",
    [
        # missing closing parenthesis
        ["or=(decompte__exact.1,decompte__greater.3"],
        # not the expected top level syntax
        ["or(decompte__exact.1,decompte__greater.3)"],
        # a value holding a dot must be quoted
        ["or=(decompte__exact.1,score__greater.0.8)"],
        # sort is not allowed within an operator
        ["or=(decompte__exact.1,score__sort.asc)"],
        # unknown column within an operator
        ["or=(nope__exact.1)"],
        # aggregation within an operator
        ["or=(decompte__count.1)"],
        # pagination and column selection are not allowed within an operator
        ["or=(decompte__exact.1,page.12)"],
        ["or=(decompte__exact.1,page_size.12)"],
        ["or=(decompte__exact.1,columns.12)"],
    ],
)
def test_query_with_malformed_or_group(source, query_string):
    with pytest.raises(ValueError):
        run(source, query_string)
