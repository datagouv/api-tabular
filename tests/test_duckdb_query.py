from api_tabular.tabular.duckdb_query import build_duckdb_query


def test_duckdb_query_limit_and_default_order():
    q = build_duckdb_query([], page_size=12)
    assert q.limit == 12
    assert q.offset == 0
    assert q.order_sql == 'ORDER BY "__id" ASC'
    assert q.select_sql == "*"
    assert not q.is_aggregation


def test_duckdb_query_filters():
    q = build_duckdb_query(
        ["score__greater=0.9", "decompte__exact=13"],
        page_size=20,
        offset=20,
    )
    assert 'WHERE "score" >= ? AND "decompte" = ?' == q.where_sql
    assert q.where_params == ["0.9", "13"]
    assert q.limit == 20
    assert q.offset == 20


def test_duckdb_query_contains_and_or():
    q = build_duckdb_query(
        ["or=(id__exact.abc,score__less.0.1)"],
        page_size=50,
    )
    assert q.where_sql.startswith("WHERE (")
    assert " OR " in q.where_sql
    assert q.where_params == ["abc", "0.1"]


def test_duckdb_query_aggregation():
    q = build_duckdb_query(
        ["decompte__groupby", "score__avg"],
        page_size=50,
    )
    assert q.is_aggregation
    assert '"decompte"' in q.select_sql
    assert 'AVG("score") AS "score__avg"' in q.select_sql
    assert q.group_by_sql == 'GROUP BY "decompte"'
    assert q.order_sql == ""


def test_duckdb_query_columns_conflict_with_agg():
    import pytest

    with pytest.raises(ValueError):
        build_duckdb_query(["columns=a,b", "a__groupby"])
