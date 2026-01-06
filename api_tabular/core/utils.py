from aiohttp.web_response import Response

from api_tabular import config


def is_aggregation_allowed(resource_id: str):
    return resource_id in config.ALLOW_AGGREGATION


def process_total(res: Response) -> int:
    # the Content-Range looks like this: '0-49/21777'
    # see https://docs.postgrest.org/en/stable/references/api/pagination_count.html
    raw_total = res.headers.get("Content-Range")
    _, str_total = raw_total.split("/")
    return int(str_total)
