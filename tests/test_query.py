from udata_hydra_csvapi.utils import build_sql_query_string


def test_query_build_limit():
    query_str = []
    result = build_sql_query_string(query_str, 12)
    assert result == "limit=12"


def test_query_build_offset():
    query_str = []
    result = build_sql_query_string(query_str, 12, 12)
    assert result == "limit=12&offset=12"


def test_query_build_sort_asc():
    query_str = ["column_name__sort=asc"]
    result = build_sql_query_string(query_str, 50)
    assert result == "order=column_name.asc&limit=50"


def test_query_build_sort_desc():
    query_str = ["column_name__sort=desc"]
    result = build_sql_query_string(query_str, 50)
    assert result == "order=column_name.desc&limit=50"


def test_query_build_exact():
    query_str = ["column_name__exact=BIDULE"]
    result = build_sql_query_string(query_str, 50)
    assert result == "column_name=eq.BIDULE&limit=50"


def test_query_build_contains():
    query_str = ["column_name__contains=BIDULE"]
    result = build_sql_query_string(query_str, 50)
    assert result == "column_name=like.*BIDULE*&limit=50"


def test_query_build_less():
    query_str = ["column_name__less=12"]
    result = build_sql_query_string(query_str, 50, 12)
    assert result == "column_name=lte.12&limit=50&offset=12"


def test_query_build_greater():
    query_str = ["column_name__greater=12"]
    result = build_sql_query_string(query_str, 50)
    assert result == "column_name=gte.12&limit=50"


def test_query_build_multiple():
    query_str = [
        "column_name__exact=BIDULE",
        "column_name__greater=12",
        "column_name__exact=BIDULE",
    ]
    result = build_sql_query_string(query_str, 50)
    assert (
        result
        == "column_name=eq.BIDULE&column_name=gte.12&column_name=eq.BIDULE&limit=50"
    )


def test_query_build_multiple_with_unknown():
    query_str = ["select=numnum"]
    result = build_sql_query_string(query_str, 50)
    assert result == "limit=50"
