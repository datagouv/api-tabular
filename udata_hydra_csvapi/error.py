import json
from aiohttp import web


class QueryException(web.HTTPException):
    """Re-raise an exception from postgrest as aiohttp exception"""
    def __init__(self, status, title, detail) -> None:
        error_body = {
            'errors': [{
                'title': title,
                'detail': detail
            }]
        }
        self.status_code = status
        super().__init__(content_type="application/json", text=json.dumps(error_body))
