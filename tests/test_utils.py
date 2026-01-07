from aiohttp.test_utils import make_mocked_request

from api_tabular.core.url import build_link_with_page, external_url, url_for


def test_build_link_with_page():
    request = make_mocked_request("GET", "/api/test?foo=bar")
    link = build_link_with_page(request, query_string=["foo=1", "bar=3"], page=2, page_size=10)
    assert link == external_url("/api/test?foo=1&bar=3&page=2&page_size=10")


def test_url_for(fake_client):
    request = make_mocked_request("GET", "/api/test?foo=bar")
    request.app.router = fake_client.app.router
    url = url_for(request, "profile", rid="rid")
    assert str(url) == "/api/resources/rid/profile/"


def test_url_for_external(fake_client):
    request = make_mocked_request("GET", "/api/test?foo=bar")
    request.app.router = fake_client.app.router
    url = url_for(request, "profile", rid="rid", _external=True)
    assert str(url) == external_url("/api/resources/rid/profile/")
