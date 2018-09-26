"""Response Unit tests

The unit tests for the response module.
As it is a unit test all external dependencies are mocked

"""
import importlib
import datetime

class MockRequest:
    args = {}
    path = 'path'


def before_each_response_test(monkeypatch):
    import flask
    importlib.reload(flask)

    mockRequest = MockRequest()
    monkeypatch.setattr(flask, 'request', mockRequest)

    import gobapi.response
    importlib.reload(gobapi.response)


def test_not_found(monkeypatch):
    before_each_response_test(monkeypatch)

    from gobapi.response import not_found

    assert(not_found('msg') == ('{"error": 404, "text": "msg"}', 404))


def test_page_ref(monkeypatch):
    before_each_response_test(monkeypatch)

    from gobapi.response import get_page_ref
    assert(get_page_ref(1, 10) == 'path?page=1')
    assert(get_page_ref(0, 10) == None)
    assert(get_page_ref(10, 10) == 'path?page=10')
    assert(get_page_ref(11, 10) == None)

    MockRequest.args = {'arg': 'value'}
    assert(get_page_ref(1, 10) == 'path?arg=value&page=1')


def test_hal_response(monkeypatch):
    before_each_response_test(monkeypatch)

    from gobapi.response import hal_response
    assert(hal_response({}) == '{"_links": {"self": {"href": "path?arg=value"}}}')
    assert(hal_response({}, {'link': 'href'}) == '{"_links": {"link": {"href": "href"}, "self": {"href": "path?arg=value"}}}')
