
from aiohttp.test_utils import make_mocked_request

from api_tabular.utils import build_link_with_page, url_for, external_url


def test_build_link_with_page():
    request = make_mocked_request("GET", "/api/test?foo=bar")
    link = build_link_with_page(request, query_string=["foo=1", "bar=3"], page=2, page_size=10)
    assert link == external_url("/api/test?foo=1&bar=3&page=2&page_size=10")


def test_url_for(client):
    request = make_mocked_request("GET", "/api/test?foo=bar")
    request.app.router = client.app.router
    url = url_for(request, 'profile', rid='rid')
    assert str(url) == '/api/resources/rid/profile/'


def test_url_for_external(client):
    request = make_mocked_request("GET", "/api/test?foo=bar")
    request.app.router = client.app.router
    url = url_for(request, 'profile', rid='rid', _external=True)
    assert str(url) == external_url("/api/resources/rid/profile/")
