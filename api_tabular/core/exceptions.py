"""
Exception handling for the core module.

This module contains exception classes and error handling utilities
extracted from the original error.py module.
"""

import json

import sentry_sdk
from aiohttp import web


class QueryException(web.HTTPException):
    """Re-raise an exception from postgrest as aiohttp exception"""

    def __init__(self, status, error_code, title, detail) -> None:
        self.status_code = status
        error_body = {"errors": [{"code": error_code, "title": title, "detail": detail}]}
        super().__init__(content_type="application/json", text=json.dumps(error_body))


def handle_exception(status: int, title: str, detail: str | dict, resource_id: str | None = None):
    """Handle exceptions with Sentry integration."""
    event_id = None
    e = Exception(detail)
    if sentry_sdk.Hub.current.client:
        with sentry_sdk.new_scope() as scope:
            sentry_tags: dict = {
                "status": status,
                "title": title,
                "detail": detail,
            }
            if resource_id:
                sentry_tags["resource_id"] = resource_id
            scope.set_tags(sentry_tags)
            event_id = sentry_sdk.capture_exception(e)
    raise QueryException(status, event_id, title, detail)
