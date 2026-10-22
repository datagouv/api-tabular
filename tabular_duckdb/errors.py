import sentry_sdk

from tabular_duckdb.config import settings


class QueryException(Exception):
    """Error rendered with the same body as the PostgREST-based API."""

    def __init__(self, status: int, title: str, detail: str, error_code: str | None = None) -> None:
        super().__init__(detail)
        self.status = status
        self.error_code = error_code
        self.title = title
        self.detail = detail

    def body(self) -> dict[str, list[dict[str, str | None]]]:
        return {"errors": [{"code": self.error_code, "title": self.title, "detail": self.detail}]}


def safe_message(message: str) -> str:
    """Do not leak the storage location in DuckDB error messages."""
    # the `LINE 1: ...` context quotes the SQL that ran, which reads the parquet file by its
    # location and is cut at an arbitrary length: no replacement can catch a truncated prefix.
    # It is never exactly what the client wrote anyway: /sql/ runs the query wrapped in a LIMIT.
    redacted = message.partition("\nLINE ")[0]
    return redacted.replace(settings.PARQUET_BASE_URL.rstrip("/") + "/", "<resource>/").strip()


def invalid_query_string(detail: str) -> QueryException:
    return QueryException(400, "Invalid query string", detail)


def query_timeout(seconds: float) -> QueryException:
    return QueryException(
        400, "Query timeout", f"The query was interrupted after {seconds} seconds"
    )


def server_busy(seconds: float) -> QueryException:
    return QueryException(
        503,
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
    return QueryException(status, title, detail, event_id)
