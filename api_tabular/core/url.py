from aiohttp.web_request import Request

from api_tabular import config


def external_url(url) -> str:
    return f"{config.SCHEME}://{config.SERVER_NAME}{url}"


def build_link_with_page(
    request: Request, query_string: list[str], page: int, page_size: int
) -> str:
    q = [string for string in query_string if not string.startswith("page")]
    q.extend([f"page={page}", f"page_size={page_size}"])
    rebuilt_q = "&".join(q)
    return external_url(f"{request.path}?{rebuilt_q}")


def url_for(request: Request, route: str, *args, **kwargs) -> str:
    router = request.app.router
    if kwargs.pop("_external", None):
        return external_url(router[route].url_for(**kwargs))
    return str(router[route].url_for(**kwargs))
