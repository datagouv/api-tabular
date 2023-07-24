
from aiohttp.test_utils import make_mocked_request

from api_tabular.utils import build_link_with_page


def test_build_link_with_page():
    request = make_mocked_request("GET", "/api/test?foo=bar")
    link = build_link_with_page(request, query_string=["foo=1", "bar=3"], page=2, page_size=10)
    assert link == f"{request.scheme}://{request.host}/api/test?foo=1&bar=3&page=2&page_size=10"
