import pytest

from api_tabular import config
from api_tabular.utils import build_sql_query_string

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
    assert result == '"column_name"=neq.BIDULE&limit=50&order=__id.asc'


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
def test_query_aggregators(allow_aggregation):
    if allow_aggregation:
        config.override(ALLOW_AGGREGATION=[RESOURCE_ID])
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
