import sentry_sdk


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
