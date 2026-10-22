from tabular_duckdb.config import settings
from tabular_duckdb.errors import safe_message


def test_safe_message_drops_the_quoted_sql():
    """DuckDB cuts the quoted SQL anywhere, a truncated storage location included."""
    message = (
        "Binder Error: x\n\nLINE 1: SELECT * FROM read_parquet('" + settings.PARQUET_BASE_URL[:12]
    )
    assert safe_message(message) == "Binder Error: x"


def test_safe_message_hides_the_storage_location():
    message = f"IO Error: cannot open {settings.object_url('abc')}"
    assert safe_message(message) == "IO Error: cannot open <resource>/abc.parquet"


def test_safe_message_hides_the_credentials(monkeypatch):
    monkeypatch.setattr(settings, "S3_ACCESS_KEY", "access-key-id")
    monkeypatch.setattr(settings, "S3_SECRET_KEY", "secret-key-value")
    message = "HTTP Error: denied for access-key-id with secret-key-value"
    assert safe_message(message) == "HTTP Error: denied for *** with ***"
