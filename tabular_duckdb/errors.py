import sentry_sdk

from tabular_duckdb.config import settings


class QueryException(Exception):
    """Error rendered with the same body as the PostgREST-based API."""

    def __init__(self, status: int, error_code: str | None, title: str, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.error_code = error_code
        self.title = title
        self.detail = detail

    def body(self) -> dict[str, list[dict[str, str | None]]]:
        return {"errors": [{"code": self.error_code, "title": self.title, "detail": self.detail}]}


def safe_message(message: str) -> str:
    """Do not leak the storage location nor credentials in DuckDB error messages."""
    # the `LINE 1: ...` context quotes the SQL that ran, which reads the parquet file by its
    # location and is cut at an arbitrary length: no replacement can catch a truncated prefix.
    # It is never what the client wrote anyway, even on /sql/ where the query is rebuilt.
    redacted = message.partition("\nLINE ")[0]
    redacted = redacted.replace(settings.PARQUET_BASE_URL.rstrip("/") + "/", "<resource>/")
    for secret in (settings.S3_SECRET_KEY, settings.S3_ACCESS_KEY):
        if secret:
            redacted = redacted.replace(secret, "***")
    return redacted.strip()


def invalid_query_string(detail: str) -> QueryException:
    return QueryException(400, None, "Invalid query string", detail)


def query_timeout(seconds: float) -> QueryException:
    return QueryException(
        400, None, "Query timeout", f"The query was interrupted after {seconds} seconds"
    )


def server_busy(seconds: float) -> QueryException:
    return QueryException(
        503,
        None,
        "Server busy",
        f"No query slot got available within {seconds} seconds, retry later",
    )


def handle_exception(
    status: int, title: str, detail: str, resource_id: str | None = None
) -> QueryException:
    """Report to Sentry (when configured) and build the exception to raise."""
    event_id = None
    if sentry_sdk.get_client().is_active():
        with sentry_sdk.new_scope() as scope:
            tags = {"status": status, "title": title, "detail": detail}
            if resource_id:
                tags["resource_id"] = resource_id
            scope.set_tags(tags)
            event_id = sentry_sdk.capture_exception(Exception(detail))
    return QueryException(status, event_id, title, detail)
