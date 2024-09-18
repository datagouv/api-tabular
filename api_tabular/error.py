import json
from typing import Union

import sentry_sdk
from aiohttp import web

from api_tabular import config


class QueryException(web.HTTPException):
    """Re-raise an exception from postgrest as aiohttp exception"""

    def __init__(self, status, error_code, title, detail) -> None:
        self.status_code = status
        error_body = {"errors": [{"code": error_code, "title": title, "detail": detail}]}
        super().__init__(content_type="application/json", text=json.dumps(error_body))


def handle_exception(status: int, title: str, detail: Union[str, dict], resource_id: str = None):
    event_id = None
    e = Exception(detail)
    if config.SENTRY_DSN:
        with sentry_sdk.push_scope() as scope:
            scope.set_extra("status", status)
            scope.set_extra("title", title)
            scope.set_extra("detail", detail)
            if resource_id:
                scope.set_extra("resource_id", resource_id)
            event_id = sentry_sdk.capture_exception(e)
    raise QueryException(status, event_id, title, detail)
