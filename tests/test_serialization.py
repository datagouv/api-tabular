import pytest

from tabular_duckdb.serialization import as_csv_value, jsonable, rows_to_json


@pytest.mark.parametrize(
    "expression, json_value, csv_value",
    [
        ("1.25::DECIMAL(10, 2)", 1.25, "1.25"),
        (
            "'b1a8c2e4-0f6d-4e51-9a3b-2c7d8e9f0a1b'::UUID",
            "b1a8c2e4-0f6d-4e51-9a3b-2c7d8e9f0a1b",
            "b1a8c2e4-0f6d-4e51-9a3b-2c7d8e9f0a1b",
        ),
        # not valid UTF-8: escaped rather than failing the whole response, or losing the byte
        ("'\\xAA'::BLOB", "\\xaa", "\\xaa"),
        ("'abc'::BLOB", "abc", "abc"),
        # map keys are converted like the values, and two invalid ones stay apart
        ("MAP {'\\xAA'::BLOB: 1, '\\xAB'::BLOB: 2}", {"\\xaa": 1, "\\xab": 2}, None),
        ("MAP {'abc'::BLOB: 1}", {"abc": 1}, '{"abc": 1}'),
        ("[1, 2]::INTEGER[2]", [1, 2], "[1, 2]"),
        ("INTERVAL 1 DAY", "1 day, 0:00:00", "1 day, 0:00:00"),
        ("{'a': 1}", {"a": 1}, '{"a": 1}'),
        ("[1, 2]", [1, 2], "[1, 2]"),
        ("TIME '12:30:00'", "12:30:00", "12:30:00"),
        ("true", True, "true"),
    ],
)
def test_duckdb_values_are_serialized(duckdb_connection, expression, json_value, csv_value):
    """Any of them can come out of an aggregation, whatever the parquet files hold."""
    [(value,)] = duckdb_connection.execute(f"SELECT {expression}").fetchall()
    assert jsonable(value) == json_value
    if csv_value is not None:
        assert as_csv_value(value) == csv_value


def test_timestamp_with_time_zone_is_serialized(duckdb_connection):
    """DuckDB converts TIMESTAMPTZ values to timezone-aware datetimes when fetching them, in the
    time zone of the engine."""
    duckdb_connection.execute("SET TimeZone='UTC'")
    cursor = duckdb_connection.execute("SELECT TIMESTAMPTZ '2024-01-01 12:00:00+00' AS t")
    rows = cursor.fetchall()
    [row] = rows_to_json(["t"], rows, json_columns=set())
    assert row["t"] == "2024-01-01T12:00:00+00:00"
    assert as_csv_value(rows[0][0]) == "2024-01-01T12:00:00+00:00"


def test_json_columns_are_parsed():
    rows = [('{"a": [1, null]}', '{"a": 1}'), ("[]", "[]"), (None, None)]
    assert rows_to_json(["parsed", "text"], rows, json_columns={"parsed"}) == [
        {"parsed": {"a": [1, None]}, "text": '{"a": 1}'},
        {"parsed": [], "text": "[]"},
        {"parsed": None, "text": None},
    ]


def test_a_json_column_holding_invalid_json_keeps_its_text():
    """The source file is checked by csv-detective only: nothing else validates the values."""
    assert rows_to_json(["l"], [("[1, 2",)], json_columns={"l"}) == [{"l": "[1, 2"}]
