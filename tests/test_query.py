from datetime import date

import pytest

from tabular_duckdb.query import build_query

from .conftest import SOURCE

SELECT_ALL = 'SELECT "__id", * EXCLUDE ("__id") FROM source'
ORDER_BY_ID = 'ORDER BY "__id" ASC'


def data_sql(query_string, columns, page_size=None, offset=0):
    return build_query(query_string, columns, page_size=page_size, offset=offset).data_sql(SOURCE)


def test_query_build_limit(columns):
    sql, params = data_sql([], columns, page_size=12)
    assert sql == f"{SELECT_ALL} {ORDER_BY_ID} LIMIT 12"
    assert params == []


def test_query_build_offset(columns):
    sql, _ = data_sql([], columns, page_size=12, offset=12)
    assert sql == f"{SELECT_ALL} {ORDER_BY_ID} LIMIT 12 OFFSET 12"


def test_query_build_sort_asc(columns):
    sql, _ = data_sql(["column_name__sort=asc"], columns, page_size=50)
    assert sql == f'{SELECT_ALL} ORDER BY "column_name" ASC LIMIT 50'


def test_query_build_sort_desc(columns):
    sql, _ = data_sql(["column_name__sort=desc"], columns, page_size=50)
    assert sql == f'{SELECT_ALL} ORDER BY "column_name" DESC LIMIT 50'


def test_query_build_sort_unknown_direction(columns):
    with pytest.raises(ValueError):
        data_sql(["column_name__sort=sideways"], columns)


def test_query_build_sort_with_page_in_query(columns):
    sql, _ = data_sql(["column_name__sort=asc", "page=2", "page_size=20"], columns)
    assert sql == f'{SELECT_ALL} ORDER BY "column_name" ASC'


def test_query_build_multiple_sorts(columns):
    sql, _ = data_sql(["score__sort=desc", "birth__sort=asc"], columns)
    assert sql == f'{SELECT_ALL} ORDER BY "score" DESC, "birth" ASC'


@pytest.mark.parametrize(
    "argument, expected_condition, expected_params",
    [
        ("column_name__exact=BIDULE", '("column_name" = ?)', ["BIDULE"]),
        ("column_name__differs=BIDULE", '("column_name" IS DISTINCT FROM ?)', ["BIDULE"]),
        ("column_name__isnull", '("column_name" IS NULL)', []),
        ("column_name__isnotnull", '("column_name" IS NOT NULL)', []),
        (
            "column_name__contains=BIDULE",
            "(CAST(\"column_name\" AS VARCHAR) ILIKE '%' || ? || '%')",
            ["BIDULE"],
        ),
        (
            "column_name__notcontains=BIDULE",
            "(NOT (CAST(\"column_name\" AS VARCHAR) ILIKE '%' || ? || '%'))",
            ["BIDULE"],
        ),
        (
            "column_name__in=value1,value2,value3",
            '("column_name" IN (?, ?, ?))',
            ["value1", "value2", "value3"],
        ),
        ("column_name__notin=value1,value2", '("column_name" NOT IN (?, ?))', ["value1", "value2"]),
        ("decompte__less=12", '("decompte" <= ?)', [12]),
        ("decompte__greater=12", '("decompte" >= ?)', [12]),
        ("decompte__strictly_less=12", '("decompte" < ?)', [12]),
        ("decompte__strictly_greater=12", '("decompte" > ?)', [12]),
        ("score__exact=0.5", '("score" = ?)', [0.5]),
        ("is_true__exact=true", '("is_true" = ?)', [True]),
        ("birth__exact=1980-03-23", '("birth" = ?)', [date(1980, 3, 23)]),
    ],
)
def test_query_build_filters(columns, argument, expected_condition, expected_params):
    sql, params = data_sql([argument], columns, page_size=50)
    assert sql == f"{SELECT_ALL} WHERE {expected_condition} {ORDER_BY_ID} LIMIT 50"
    assert params == expected_params


def test_query_build_multiple_filters(columns):
    sql, params = data_sql(
        ["column_name__exact=BIDULE", "decompte__greater=12", "column_name__exact=TRUC"],
        columns,
        page_size=50,
    )
    assert sql == (
        f'{SELECT_ALL} WHERE ("column_name" = ?) AND ("decompte" >= ?) AND ("column_name" = ?)'
        f" {ORDER_BY_ID} LIMIT 50"
    )
    assert params == ["BIDULE", 12, "TRUC"]


def test_query_value_is_url_decoded(columns):
    _, params = data_sql(["column_name__exact=rue%20de%20la%20paix"], columns)
    assert params == ["rue de la paix"]


def test_query_column_name_is_url_decoded(columns):
    sql, _ = data_sql(["third%20col__greater=3"], columns)
    assert '"third col" >= ?' in sql


def test_query_value_not_castable_falls_back_to_string_comparison(columns):
    """`birth__greater=1996` on a DATE column keeps working, as it did on PostgreSQL."""
    sql, params = data_sql(["birth__greater=1996"], columns)
    assert 'CAST("birth" AS VARCHAR) >= ?' in sql
    assert params == ["1996"]


def test_query_count_sql(columns):
    parsed = build_query(["decompte__greater=12"], columns, page_size=50)
    sql, params = parsed.count_sql(SOURCE)
    assert sql == 'SELECT count(*) FROM source WHERE ("decompte" >= ?)'
    assert params == [12]


def test_query_specify_columns(columns):
    sql, _ = data_sql(["columns=col1,col2"], columns)
    assert sql == f'SELECT "col1", "col2" FROM source {ORDER_BY_ID}'


def test_query_specify_unknown_column(columns):
    with pytest.raises(ValueError, match="unknown column"):
        data_sql(["columns=col1,nope"], columns)


def test_query_filter_on_unknown_column(columns):
    with pytest.raises(ValueError, match="unknown column"):
        data_sql(["nope__exact=1"], columns)


def test_query_forbidden_quote_in_columns(columns):
    with pytest.raises(ValueError):
        data_sql(['columns=col1,"col2"'], columns)


def test_query_unparseable_argument(columns):
    with pytest.raises(ValueError):
        data_sql(["select=numnum"], columns)


def test_query_operator_without_value(columns):
    with pytest.raises(ValueError):
        data_sql(["column_name__exact"], columns)


@pytest.mark.parametrize("aggregator", ["groupby", "count", "avg", "min", "max", "sum"])
def test_query_aggregators_are_rejected(columns, aggregator):
    """Aggregation now belongs to the /sql/ endpoint."""
    with pytest.raises(ValueError, match="sql"):
        data_sql([f"decompte__{aggregator}"], columns)


@pytest.mark.parametrize(
    "query_string, expected_condition, expected_params",
    [
        (
            ["or=(first_col__exact.BIDULE,second_col__greater.12)"],
            '(("first_col" = ?) OR ("second_col" >= ?))',
            ["BIDULE", 12],
        ),
        (
            ["first_col__in=BIDULE,TRUC", "or=(first_col__exact.BIDULE,second_col__greater.12)"],
            '("first_col" IN (?, ?)) AND (("first_col" = ?) OR ("second_col" >= ?))',
            ["BIDULE", "TRUC", "BIDULE", 12],
        ),
        (
            [
                "or=(first_col__exact.BIDULE,second_col__greater.12)",
                "or=(first_col__exact.TRUC,second_col__less.10)",
            ],
            '(("first_col" = ?) OR ("second_col" >= ?)) AND (("first_col" = ?) OR ("second_col" <= ?))',
            ["BIDULE", 12, "TRUC", 10],
        ),
        (
            [
                'or=(first_col__exact.TRUC,and("second.col"__greater."12.4","third col"__less.10),"fourth..col"__isnull)'
            ],
            (
                '(("first_col" = ?) OR (("second.col" >= ?) AND ("third col" <= ?))'
                ' OR ("fourth..col" IS NULL))'
            ),
            ["TRUC", 12.4, 10],
        ),
        (
            ["or=(first_col__isnotnull,or(second_col__exact.1,first_col__contains.x))"],
            (
                '(("first_col" IS NOT NULL) OR (("second_col" = ?)'
                " OR (CAST(\"first_col\" AS VARCHAR) ILIKE '%' || ? || '%')))"
            ),
            [1, "x"],
        ),
    ],
)
def test_query_with_or_group(columns, query_string, expected_condition, expected_params):
    sql, params = data_sql(query_string, columns, page_size=50)
    assert sql == f"{SELECT_ALL} WHERE {expected_condition} {ORDER_BY_ID} LIMIT 50"
    assert params == expected_params


def test_query_or_group_with_encoded_structure(columns):
    """Clients such as httpx percent-encode the parentheses, quotes and commas of the group."""
    sql, params = data_sql(
        ["or=%28%22third+col%22__greater.12%2Cfirst_col__exact.BIDULE%29"], columns
    )
    assert sql == f'{SELECT_ALL} WHERE (("third col" >= ?) OR ("first_col" = ?)) {ORDER_BY_ID}'
    assert params == [12, "BIDULE"]


def test_query_or_group_with_encoded_column_name_on_isnull(columns):
    """A column name still encoded (`+`, `%xx`) must be decoded, isnull terms included."""
    sql, params = data_sql(["or=(%22third+col%22__isnull,first_col__exact.BIDULE)"], columns)
    assert sql == f'{SELECT_ALL} WHERE (("third col" IS NULL) OR ("first_col" = ?)) {ORDER_BY_ID}'
    assert params == ["BIDULE"]


@pytest.mark.parametrize(
    "query_string",
    [
        # missing closing parenthesis
        ["or=(first_col__exact.BIDULE,second_col__greater.12"],
        # not the expected top level syntax
        ["or(first_col__exact.BIDULE,second_col__greater.12)"],
        # sort is not allowed within an operator
        ["or=(first_col__exact.BIDULE,second_col__sort.asc)"],
        # unknown column within an operator
        ["or=(nope__exact.BIDULE)"],
        # aggregation within an operator
        ["or=(first_col__count.1)"],
        # pagination and column selection are not allowed within an operator
        ["or=(first_col__exact.BIDULE,page.12)"],
        ["or=(first_col__exact.BIDULE,page_size.12)"],
        ["or=(first_col__exact.BIDULE,columns.12)"],
    ],
)
def test_query_with_malformed_or_group(columns, query_string):
    with pytest.raises(ValueError):
        data_sql(query_string, columns, page_size=50)
