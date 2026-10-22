from dataclasses import dataclass
from datetime import date

import duckdb
import pytest

from tabular_duckdb.engine import fetch
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


@dataclass(frozen=True)
class Table:
    """A table of the test connection, queried with the columns DESCRIBE gives, like the API."""

    conn: duckdb.DuckDBPyConnection
    name: str

    @property
    def columns(self) -> dict[str, str]:
        described = self.conn.execute(f"DESCRIBE {self.name}").fetchall()
        return {name: kind for name, kind, *_ in described}


def run(
    table: Table, query_string: list[str], page_size: int | None = None, offset: int = 0
) -> list[dict]:
    parsed = build_query(query_string, table.columns, page_size=page_size, offset=offset)
    statement = parsed.data_sql(table.name)
    result = fetch(table.conn, statement.sql, statement.params)
    return [dict(zip(result.columns, values)) for values in result.rows]


def ids(table: Table, query_string: list[str], **kwargs) -> list[int]:
    return [r["__id"] for r in run(table, query_string, **kwargs)]


@pytest.fixture
def source(duckdb_connection, parquet_dir, monkeypatch) -> Table:
    """The test resource, exposed exactly like the API does (`__id` included)."""
    monkeypatch.setattr("tabular_duckdb.resources.settings.PARQUET_BASE_URL", parquet_dir)
    resource = Resource(resource_id=RESOURCE_ID, created_at=None)
    duckdb_connection.execute(f"CREATE VIEW source AS SELECT * FROM {resource.source_sql}")
    return Table(duckdb_connection, "source")


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


@pytest.mark.parametrize("value", ["sideways", "desc.sideways", "nullsfirst.desc"])
def test_query_sort_unknown_direction(source, value):
    with pytest.raises(ValueError):
        run(source, [f"decompte__sort={value}"])


@pytest.mark.parametrize(
    "value, nulls_first",
    [
        # PostgreSQL defaults: NULLs are the largest values
        ("asc", False),
        ("desc", True),
        # PostgREST modifiers
        ("asc.nullsfirst", True),
        ("desc.nullslast", False),
        ("nullsfirst", True),
        ("nullslast", False),
    ],
)
def test_query_sort_nulls_placement(source, value, nulls_first):
    names = [r[NAME] for r in run(source, [f"{NAME}__sort={value}"])]
    nulls = len(expected_ids(lambda r: r[NAME] is None))
    edge = names[:nulls] if nulls_first else names[-nulls:]
    assert edge == [None] * nulls
    non_null = [name for name in names if name is not None]
    assert non_null == sorted(non_null, reverse=value.startswith("desc"))


def test_query_sort_ties_are_ordered_by_id(source):
    """Rows with equal sort values must come in a stable order, or pagination skips rows."""
    conn, columns = source.conn, source.columns
    # stored in the reverse order of __id, so that only an explicit tiebreak gives __id order
    conn.execute("CREATE TABLE reversed AS SELECT * FROM source ORDER BY __id DESC")
    paginated_ids = []
    for offset in range(0, TOTAL_LINES, 7):
        parsed = build_query(["decompte__sort=asc", "columns=__id"], columns, 7, offset)
        statement = parsed.data_sql("reversed")
        rows = conn.execute(statement.sql, statement.params).fetchall()
        paginated_ids.extend(value for (value,) in rows)
    expected = sorted(ROWS, key=lambda r: (r["decompte"], r["__id"]))
    assert paginated_ids == [r["__id"] for r in expected]


# in the French order: accents and case only break ties, a space or a hyphen sorts before letters
FRENCH_ORDER = [
    "anne",
    "Anne",
    "Anne Marie",
    "Anne-Flore",
    "Élise",
    "Elm",
    "Hélène",
    "Henri",
    "Zoé",
]


@pytest.fixture
def names(duckdb_connection):
    """A text column holding the cases where the French order differs from the byte order."""
    shuffled = [
        "Henri",
        "Zoé",
        "Anne-Flore",
        "Élise",
        "anne",
        "Hélène",
        None,
        "Elm",
        "Anne Marie",
        "Anne",
    ]
    duckdb_connection.execute(
        "CREATE TABLE names AS SELECT * FROM (VALUES "
        + ", ".join(f"({i}, ?)" for i in range(1, len(shuffled) + 1))
        + ") AS t(__id, name)",
        shuffled,
    )
    return Table(duckdb_connection, "names")


@pytest.mark.parametrize("direction", ["asc", "desc"])
def test_query_text_sort_follows_the_french_order(names, direction):
    rows = run(names, [f"name__sort={direction}"])
    expected = FRENCH_ORDER if direction == "asc" else FRENCH_ORDER[::-1]
    # NULL is the largest value, like in PostgreSQL
    assert [r["name"] for r in rows] == (
        [*expected, None] if direction == "asc" else [None, *expected]
    )


@pytest.mark.parametrize(
    "argument, expected",
    [
        ("name__less=Elm", FRENCH_ORDER[:6]),
        # a lowercase letter sorts before its uppercase, so `Élise` is greater than `élise`
        ("name__less=élise", FRENCH_ORDER[:4]),
        ("name__strictly_less=Anne Marie", FRENCH_ORDER[:2]),
        ("name__greater=Hélène", FRENCH_ORDER[6:]),
        ("name__strictly_greater=Anne Marie", FRENCH_ORDER[3:]),
    ],
)
def test_query_text_comparisons_follow_the_french_order(names, argument, expected):
    rows = run(names, [argument, "name__sort=asc"])
    assert [r["name"] for r in rows] == expected


def test_query_text_min_and_max_follow_the_french_order(names):
    assert run(names, ["name__min", "name__max"]) == [
        {"name__min": FRENCH_ORDER[0], "name__max": FRENCH_ORDER[-1]}
    ]


def test_query_text_equality_stays_exact(names):
    assert [r["name"] for r in run(names, ["name__exact=anne"])] == ["anne"]
    assert [r["name"] for r in run(names, ["name__in=Anne,Elm"])] == ["Elm", "Anne"]


def test_query_in_list_keeps_the_commas_of_quoted_values():
    parsed = build_query(['name__in="a,b",c'], {"__id": "BIGINT", "name": "VARCHAR"})
    assert parsed.params == ["a,b", "c"]


FIRST_UUID = "6f1c1e9a-2b3c-4d5e-8f90-0a1b2c3d4e5f"
SECOND_UUID = "7a2d2f0b-3c4d-4e5f-9a01-1b2c3d4e5f60"


@pytest.fixture
def typed(duckdb_connection):
    """Columns of the types whose filter values need a conversion of their own."""
    duckdb_connection.execute(
        f"""
        CREATE TABLE typed AS SELECT * FROM (VALUES
            (1, TIMESTAMP '2024-01-01 05:00:00', TIMESTAMPTZ '2024-01-01 05:00:00+00',
             1.25::DECIMAL(10, 2), '{FIRST_UUID}'::UUID, TIME '08:30:00'),
            (2, TIMESTAMP '2024-01-02 12:00:00', TIMESTAMPTZ '2024-01-02 12:00:00+00',
             2.50::DECIMAL(10, 2), '{SECOND_UUID}'::UUID, TIME '17:00:00'),
            (3, TIMESTAMP '2024-01-03 00:00:00', TIMESTAMPTZ '2024-01-03 00:00:00+00',
             3.75::DECIMAL(10, 2), NULL, TIME '23:59:00')
        ) AS t(__id, ts, tstz, montant, uid, heure)
        """
    )
    return Table(duckdb_connection, "typed")


@pytest.mark.parametrize(
    "argument, expected",
    [
        ("ts__exact=2024-01-01T05:00:00", [1]),
        # a date compares with a timestamp as its midnight
        ("ts__greater=2024-01-02", [2, 3]),
        # `+` means a space in a query string
        ("tstz__strictly_less=2024-01-02T00:00:00%2B00:00", [1]),
        ("montant__less=2.5", [1, 2]),
        (f"uid__exact={SECOND_UUID}", [2]),
        (f"uid__in={FIRST_UUID},{SECOND_UUID}", [1, 2]),
        ("heure__greater=12:00", [2, 3]),
    ],
)
def test_query_filters_on_typed_columns(typed, argument, expected):
    assert ids(typed, [argument]) == expected


@pytest.mark.parametrize(
    "argument",
    [
        "ts__exact=hier",
        "tstz__less=2024-13-01",
        "montant__less=abc",
        "uid__exact=12",
        "heure__exact=25:00",
    ],
)
def test_query_value_not_castable_to_a_typed_column(typed, argument):
    with pytest.raises(ValueError, match="invalid value"):
        ids(typed, [argument])


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
        # an empty list, as built by a client from no selected value: like PostgREST's `in.()`
        (f"{NAME}__in=", lambda r: False),
        (f"{NAME}__notin=", lambda r: True),
        ("decompte__less=3", lambda r: r["decompte"] <= 3),
        ("decompte__greater=3", lambda r: r["decompte"] >= 3),
        ("decompte__strictly_less=3", lambda r: r["decompte"] < 3),
        ("decompte__strictly_greater=3", lambda r: r["decompte"] > 3),
        ("score__exact=0.5", lambda r: r["score"] == 0.5),
        ("is_true__exact=true", lambda r: r["is_true"]),
        ("birth__exact=1980-01-01", lambda r: r["birth"] == date(1980, 1, 1)),
        ("birth__strictly_less=1955-06-01", lambda r: r["birth"] < date(1955, 6, 1)),
        # contains on a non-text column, which the swagger offers for dates
        ("birth__contains=198", lambda r: "198" in r["birth"].isoformat()),
        # a nested value has no Python counterpart: it is compared through its text rendering
        ("liste__exact=[a, b]", lambda r: True),
        ("liste__differs=[a, b]", lambda r: False),
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
    conn, columns = source.conn, source.columns
    statement = build_query(["decompte__greater=3"], columns).count_sql("source")
    [(count,)] = conn.execute(statement.sql, statement.params).fetchall()
    assert count == len(expected_ids(lambda r: r["decompte"] >= 3))


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


AGGREGATES = {
    "count": len,
    "avg": lambda values: sum(values) / len(values),
    "min": min,
    "max": max,
    "sum": sum,
}


@pytest.mark.parametrize("operator", AGGREGATES)
def test_query_aggregate_by_group(source, operator):
    rows = run(source, ["decompte__groupby", f"score__{operator}"])
    groups = sorted({r["decompte"] for r in ROWS})
    assert [list(r) for r in rows] == [["decompte", f"score__{operator}"]] * len(groups)
    assert [r["decompte"] for r in rows] == groups
    assert [r[f"score__{operator}"] for r in rows] == pytest.approx(
        [
            AGGREGATES[operator]([r["score"] for r in ROWS if r["decompte"] == group])
            for group in groups
        ]
    )


def test_query_aggregate_without_group(source):
    # like SQL, count only counts the non NULL values
    assert run(source, [f"{NAME}__count", "decompte__max"]) == [
        {f"{NAME}__count": len(expected_ids(lambda r: r[NAME] is not None)), "decompte__max": 6}
    ]


def test_query_groupby_alone_selects_the_column(source):
    """Like PostgREST, rows are only grouped once an aggregate function is requested."""
    assert run(source, ["decompte__groupby"]) == [{"decompte": r["decompte"]} for r in ROWS]


def test_query_aggregated_columns_follow_the_query_string(source):
    rows = run(source, ["score__sum", "decompte__groupby", "is_true__groupby"], page_size=1)
    assert list(rows[0]) == ["score__sum", "decompte", "is_true"]


def test_query_filters_apply_before_aggregation(source):
    rows = run(source, ["decompte__groupby", "id__count", "is_true__exact=true"])
    assert {r["decompte"]: r["id__count"] for r in rows} == {
        group: sum(1 for r in ROWS if r["is_true"] and r["decompte"] == group) for group in range(7)
    }


def test_query_sort_on_a_grouped_column(source):
    rows = run(source, ["decompte__groupby", "id__count", "decompte__sort=desc"])
    assert [r["decompte"] for r in rows] == list(range(6, -1, -1))


def test_query_groups_are_paginated_in_a_stable_order(source):
    conn, columns = source.conn, source.columns
    conn.execute("CREATE TABLE reversed AS SELECT * FROM source ORDER BY __id DESC")
    groups = []
    for offset in range(0, 7, 3):
        parsed = build_query(["decompte__groupby", "id__count"], columns, 3, offset)
        statement = parsed.data_sql("reversed")
        rows = conn.execute(statement.sql, statement.params).fetchall()
        groups.extend(group for group, _ in rows)
    assert groups == list(range(7))


def test_query_count_sql_of_an_aggregation_counts_the_groups(source):
    conn, columns = source.conn, source.columns
    parsed = build_query(["decompte__groupby", "id__count", "decompte__less=2"], columns)
    statement = parsed.count_sql("source")
    assert conn.execute(statement.sql, statement.params).fetchall() == [(3,)]


@pytest.mark.parametrize(
    "query_string",
    [
        ["columns=id,decompte", "decompte__groupby"],
        ["decompte__count", "columns=id"],
    ],
)
def test_query_columns_alongside_aggregators(source, query_string):
    with pytest.raises(ValueError, match="`columns` cannot be set alongside aggregators"):
        run(source, query_string)


def test_query_aggregator_with_a_value(source):
    with pytest.raises(ValueError, match="could not be parsed"):
        run(source, ["decompte__count=1"])


def test_query_aggregator_on_unknown_column(source):
    with pytest.raises(ValueError, match="unknown column"):
        run(source, ["nope__groupby"])


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


@pytest.fixture
def dotted(duckdb_connection) -> Table:
    """A column whose name holds a dot, the separator of the terms of an `or=(...)` group."""
    duckdb_connection.execute(
        'CREATE TABLE dotted AS SELECT * FROM (VALUES (1, 1.5), (2, NULL), (3, 2.0)) t(__id, "a.b")'
    )
    duckdb_connection.execute('ALTER TABLE dotted ALTER "a.b" TYPE DECIMAL(2, 1)')
    return Table(duckdb_connection, "dotted")


@pytest.mark.parametrize(
    "query_string, expected",
    [
        (['or=("a.b"__exact."1.5","a.b"__isnull)'], [1, 2]),
        (['or=("a.b"__greater.2,__id__exact.1)'], [1, 3]),
    ],
)
def test_query_or_group_on_a_column_name_with_a_dot(dotted, query_string, expected):
    """Only the quotes tell the dot of the name from the dots separating column, operator and
    value."""
    assert ids(dotted, query_string) == expected


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
