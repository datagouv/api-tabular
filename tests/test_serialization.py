from tabular_duckdb.serialization import as_csv_value, rows_to_json


def test_timestamp_with_time_zone_is_serialized(duckdb_connection):
    """DuckDB converts TIMESTAMPTZ values to timezone-aware datetimes when fetching them."""
    cursor = duckdb_connection.execute("SELECT TIMESTAMPTZ '2024-01-01 12:00:00+00' AS t")
    rows = cursor.fetchall()
    [row] = rows_to_json(["t"], rows)
    assert row["t"].startswith("2024-01-01")
    assert as_csv_value(rows[0][0]).startswith("2024-01-01")
