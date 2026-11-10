import re
from collections.abc import Mapping

from aiohttp.client import ClientResponse
from aiohttp.web_response import Response

from api_tabular import config
from api_tabular.core.error import QueryException


def is_aggregation_allowed(resource_id: str) -> bool:
    return config.ALLOW_AGGREGATION or resource_id in config.ALLOW_AGGREGATION_EXCEPTIONS


def process_total(res: Response | ClientResponse) -> int:
    # the Content-Range looks like this: '0-49/21777'
    # see https://docs.postgrest.org/en/stable/references/api/pagination_count.html
    raw_total = res.headers.get("Content-Range")
    if raw_total is None:
        raise ValueError("Missing Content-Range header")
    _, str_total = raw_total.split("/")
    return int(str_total)


def _parse_int_param(query: Mapping[str, str], name: str, label: str, default: int) -> int:
    raw = query.get(name)
    if raw is None:
        return default
    # stricter than int(), which also accepts "1_0", " 1" or non-ASCII digits
    if not re.fullmatch(r"-?[0-9]+", raw):
        raise QueryException(400, None, "Invalid query string", f"{label} must be an integer")
    return int(raw)


def parse_pagination(query: Mapping[str, str]) -> tuple[int, int, int]:
    """Return (page, page_size, offset) from the query, raising a 400 on invalid values"""
    page = _parse_int_param(query, "page", "Page", 1)
    page_size = _parse_int_param(query, "page_size", "Page size", config.PAGE_SIZE_DEFAULT)
    if page < 1:
        raise QueryException(400, None, "Invalid query string", "Page must be at least 1")
    if page_size < 1:
        raise QueryException(400, None, "Invalid query string", "Page size must be at least 1")
    if page_size > config.PAGE_SIZE_MAX:
        raise QueryException(
            400,
            None,
            "Invalid query string",
            f"Page size exceeds allowed maximum: {config.PAGE_SIZE_MAX}",
        )
    return page, page_size, page_size * (page - 1)
