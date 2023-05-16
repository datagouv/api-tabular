from udata_hydra_csvapi.query import build_sql_query_string


def test_query_build_limit():
    query_str = ''
    result = build_sql_query_string(query_str, 1, 12)
    assert result == 'limit=12'


def test_query_build_offset():
    query_str = 'offset=12'
    result = build_sql_query_string(query_str)
    assert result == 'offset=12'


def test_query_build_sort_asc():
    query_str = 'column_name__sort=asc'
    result = build_sql_query_string(query_str)
    assert result == 'order=column_name.asc'


def test_query_build_sort_desc():
    query_str = 'column_name__sort=desc'
    result = build_sql_query_string(query_str)
    assert result == 'order=column_name.desc'


def test_query_build_exact():
    query_str = 'column_name__exact=BIDULE'
    result = build_sql_query_string(query_str)
    assert result == 'column_name=eq.BIDULE'


def test_query_build_contains():
    query_str = 'column_name__contains=BIDULE'
    result = build_sql_query_string(query_str)
    assert result == 'column_name=like.*BIDULE*'


def test_query_build_less():
    query_str = 'column_name__less=12'
    result = build_sql_query_string(query_str)
    assert result == 'column_name=lte.12'


def test_query_build_greater():
    query_str = 'column_name__greater=12'
    result = build_sql_query_string(query_str)
    assert result == 'column_name=gte.12'


def test_query_build_multiple():
    query_str = 'column_name__exact=BIDULE&limit=5&offset=12'
    result = build_sql_query_string(query_str)
    assert result == 'column_name=eq.BIDULE&limit=5&offset=12'


def test_query_build_multiple_with_unknown():
    query_str = 'limit=1&select=numnum'
    result = build_sql_query_string(query_str)
    assert result == 'limit=1'
