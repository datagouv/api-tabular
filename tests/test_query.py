import pytest

from api_tabular.core.query import build_sql_query_string

from .conftest import RESOURCE_ID


def test_query_build_limit():
    query_str = []
    result = build_sql_query_string(query_str, page_size=12)
    assert result == "limit=12&order=__id.asc"


def test_query_build_offset():
    query_str = []
    result = build_sql_query_string(query_str, page_size=12, offset=12)
    assert result == "limit=12&offset=12&order=__id.asc"


def test_query_build_sort_asc():
    query_str = ["column_name__sort=asc"]
    result = build_sql_query_string(query_str, page_size=50)
    assert result == 'order="column_name".asc&limit=50'


def test_query_build_sort_asc_without_limit():
    query_str = ["column_name__sort=asc"]
    result = build_sql_query_string(query_str)
    assert result == 'order="column_name".asc'


def test_query_build_sort_asc_with_page_in_query():
    query_str = [
        "column_name__sort=asc",
        "page=2",
        "page_size=20",
    ]
    result = build_sql_query_string(query_str)
    assert result == 'order="column_name".asc'


def test_query_build_sort_desc():
    query_str = ["column_name__sort=desc"]
    result = build_sql_query_string(query_str, page_size=50)
    assert result == 'order="column_name".desc&limit=50'


def test_query_build_exact():
    query_str = ["column_name__exact=BIDULE"]
    result = build_sql_query_string(query_str, page_size=50)
    assert result == '"column_name"=eq.BIDULE&limit=50&order=__id.asc'


def test_query_build_differs():
    query_str = ["column_name__differs=BIDULE"]
    result = build_sql_query_string(query_str, page_size=50)
    assert result == '"column_name"=isdistinct.BIDULE&limit=50&order=__id.asc'


def test_query_build_isnull():
    query_str = ["column_name__isnull"]
    result = build_sql_query_string(query_str, page_size=50)
    assert result == '"column_name"=is.null&limit=50&order=__id.asc'


def test_query_build_isnotnull():
    query_str = ["column_name__isnotnull"]
    result = build_sql_query_string(query_str, page_size=50)
    assert result == '"column_name"=not.is.null&limit=50&order=__id.asc'


def test_query_build_contains():
    query_str = ["column_name__contains=BIDULE"]
    result = build_sql_query_string(query_str, page_size=50)
    assert result == '"column_name"=ilike.*BIDULE*&limit=50&order=__id.asc'


def test_query_build_in():
    query_str = ["column_name__in=value1,value2,value3"]
    result = build_sql_query_string(query_str, page_size=50)
    assert result == '"column_name"=in.(value1,value2,value3)&limit=50&order=__id.asc'


def test_query_build_less():
    query_str = ["column_name__less=12"]
    result = build_sql_query_string(query_str, page_size=50, offset=12)
    assert result == '"column_name"=lte.12&limit=50&offset=12&order=__id.asc'


def test_query_build_greater():
    query_str = ["column_name__greater=12"]
    result = build_sql_query_string(query_str, page_size=50)
    assert result == '"column_name"=gte.12&limit=50&order=__id.asc'


def test_query_build_multiple():
    query_str = [
        "column_name__exact=BIDULE",
        "column_name__greater=12",
        "column_name__exact=BIDULE",
    ]
    result = build_sql_query_string(query_str, page_size=50)
    assert (
        result
        == '"column_name"=eq.BIDULE&"column_name"=gte.12&"column_name"=eq.BIDULE&limit=50&order=__id.asc'
    )


@pytest.mark.parametrize(
    "query_str, expected, should_fail",
    [
        (
            # only one OR group
            [
                "or__1=column_name__exact=BIDULE",
                "or__1=second_col__less=12",
            ],
            'or=("column_name".eq.BIDULE,"second_col".lte.12)&limit=50&order=__id.asc',
            False,
        ),
        # one OR group + AND conditions
        (
            [
                "first_col__in=BIDULE,TRUC",
                "third_col__differs=1",
                "or__1=column_name__exact=BIDULE",
                "or__1=second_col__less=12",
            ],
            '"first_col"=in.(BIDULE,TRUC)&"third_col"=isdistinct.1&or=("column_name".eq.BIDULE,"second_col".lte.12)&limit=50&order=__id.asc',
            False,
        ),
        # two OR groups
        (
            [
                "first_col__exact=BIDULE",
                "or__1=column_name__exact=BIDULE",
                "or__1=second_col__less=12",
                "or__2=column_name__exact=TRUC",
                "or__2=second_col__greater=45",
            ],
            '"first_col"=eq.BIDULE&or=("column_name".eq.BIDULE,"second_col".lte.12)&or=("column_name".eq.TRUC,"second_col".gte.45)&limit=50&order=__id.asc',
            False,
        ),
        # one OR group + aggregation
        (
            [
                "first_col__exact=BIDULE",
                "or__1=column_name__exact=BIDULE",
                "or__1=second_col__less=12",
                "first_col__groupby",
                "column_name__count",
            ],
            '"first_col"=eq.BIDULE&or=("column_name".eq.BIDULE,"second_col".lte.12)&select="first_col","column_name__count":"column_name".count()&limit=50',
            False,
        ),
        # malformed OR group
        (
            [
                "or_1=column_name__exact=BIDULE",
                "or__1=second_col__sort=asc",
            ],
            None,
            True,
        ),
        (
            [
                "and__1=column_name__exact=BIDULE",
                "or__1=second_col__sort=asc",
            ],
            None,
            True,
        ),
        # forbidden params in OR group
        (
            [
                "or__1=column_name__exact=BIDULE",
                "or__1=second_col__sort=asc",
            ],
            None,
            True,
        ),
        *(
            (
                [
                    "or__1=column_name__exact=BIDULE",
                    f"or__1={forbidden}=12",
                ],
                None,
                True,
            )
            for forbidden in ["page", "page_size", "columns"]
        ),
    ],
)
def test_query_with_or_group(query_str, expected, should_fail):
    if should_fail:
        with pytest.raises(ValueError):
            build_sql_query_string(query_str, page_size=50)
    else:
        result = build_sql_query_string(query_str, page_size=50)
        assert result == expected


def test_query_build_multiple_with_unknown():
    query_str = ["select=numnum"]
    with pytest.raises(ValueError):
        build_sql_query_string(query_str, page_size=50)


@pytest.mark.parametrize(
    "allow_aggregation",
    [
        False,
        True,
    ],
)
def test_query_aggregators(allow_aggregation, mocker):
    if allow_aggregation:
        mocker.patch("api_tabular.config.ALLOW_AGGREGATION", [RESOURCE_ID])
    query_str = [
        "column_name__groupby",
        "column_name__min",
        "column_name__avg",
    ]
    if not allow_aggregation:
        with pytest.raises(PermissionError):
            build_sql_query_string(query_str, resource_id=RESOURCE_ID, page_size=50)
        return
    results = build_sql_query_string(query_str, resource_id=RESOURCE_ID, page_size=50).split("&")
    assert "limit=50" in results
    assert "order=__id.asc" not in results  # no sort if aggregators
    select = [_ for _ in results if "select" in _]
    assert len(select) == 1
    params = select[0].replace("select=", "").split(",")
    assert all(
        _ in params
        for _ in [
            '"column_name"',
            '"column_name__min":"column_name".min()',
            '"column_name__avg":"column_name".avg()',
        ]
    )


def test_query_specify_columns():
    query_str = ["columns=col1,col2"]
    result = build_sql_query_string(query_str)
    assert result == "select=col1,col2&order=__id.asc"


def test_query_specify_columns_and_aggregate():
    query_str = ["columns=col1,col2", "col1__groupby"]
    with pytest.raises(ValueError):
        build_sql_query_string(query_str)
