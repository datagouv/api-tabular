from aiohttp.client import ClientResponse
from aiohttp.web_response import Response

from api_tabular import config
from api_tabular.core.error import QueryException


def is_aggregation_allowed(resource_id: str) -> bool:
    return resource_id in config.ALLOW_AGGREGATION


def process_total(res: Response | ClientResponse) -> int:
    # the Content-Range looks like this: '0-49/21777'
    # see https://docs.postgrest.org/en/stable/references/api/pagination_count.html
    raw_total = res.headers.get("Content-Range")
    if raw_total is None:
        raise ValueError("Missing Content-Range header")
    _, str_total = raw_total.split("/")
    return int(str_total)


def build_offset(page: int, page_size: int) -> int:
    if page_size > config.PAGE_SIZE_MAX:
        raise QueryException(
            400,
            None,
            "Invalid query string",
            f"Page size exceeds allowed maximum: {config.PAGE_SIZE_MAX}",
        )
    return page_size * (page - 1) if page > 1 else 0
