"""Response Unit tests

The unit tests for the response module.
As it is a unit test all external dependencies are mocked

"""
import importlib
import json

from unittest import TestCase
from unittest.mock import patch

from gobapi.response import stream_response, ndjson_entities, stream_entities

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

    assert(not_found('msg') == ('{"error": 404, "text": "msg"}', 404, {'Content-Type': 'application/json'}))


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
    assert(hal_response({}) == ('{"_links": {"self": {"href": "path?arg=value"}}}', 200, {'Content-Type': 'application/json'}))
    assert(hal_response({}, {'link': 'href'}) == ('{"_links": {"link": {"href": "href"}, "self": {"href": "path?arg=value"}}}', 200, {'Content-Type': 'application/json'}))


def test_camelcase_converter(monkeypatch):
    before_each_response_test(monkeypatch)

    from gobapi.response import _to_camelcase, _dict_to_camelcase

    assert(_to_camelcase("") == "")
    assert(_to_camelcase("_") == "_")
    assert(_to_camelcase("_____") == "_____")
    assert(_to_camelcase("_camel_case_case") == "_camelCaseCase")
    assert(_to_camelcase("camel_case_case") == "camelCaseCase")
    assert(_to_camelcase("_camel_case_case_") == "_camelCaseCase_")

    assert(_dict_to_camelcase({}) == {})
    adict = {
        "camel_case1" : "camel_case"
    }
    assert(_dict_to_camelcase(adict) == {
        'camelCase1': 'camel_case'
    })

    adict = {
        "camel_case1" : {
            "camel_case2" : "camel_case"
        }
    }
    assert(_dict_to_camelcase(adict) == {
        'camelCase1': {'camelCase2': 'camel_case'}
    })

    adict = {
        "camel_case1" : {
            "k": "v",
            "camel_case2" : {
                "camel_case3" : "camel_case",
                "x": "y"
            },
            "a": "b",
            "c": {"camel_case4": "e"}
        }
    }
    assert(_dict_to_camelcase(adict) == {
        'camelCase1': {
            'k': 'v',
            'camelCase2': {
                'camelCase3': 'camel_case',
                'x': 'y'},
            'a': 'b',
            'c': {'camelCase4': 'e'},
            }
    } )

    adict = {
        "camel_case1" : [{
            "camel_case2" : "camel_case"
        }]
    }
    assert(_dict_to_camelcase(adict) == {
        'camelCase1': [{'camelCase2': 'camel_case'}]
    })

    adict = {
        "camel_case1" : [[{
            "camel_case2" : "camel_case"
        }]]
    }
    assert(_dict_to_camelcase(adict) == {
        'camelCase1': [[{'camelCase2': 'camel_case'}]]
    })


class TestStream(TestCase):

    def test_stream(self):
        result = stream_response({'some_key': 'some data'})
        self.assertEqual(result, '{"someKey": "some data"}')

    @patch('gobapi.response.stream_response')
    def test_stream_entities(self, mock_response):
        mock_response.side_effect = lambda r: str(r)
        entities = [{'a': 'b'}, 5, "s"]
        convert = lambda e: json.dumps(e)
        result = stream_entities(entities, convert)
        json_result = ' '.join([r for r in result])
        self.assertEqual(json.loads(json_result) , entities)

    @patch('gobapi.response.stream_response')
    def test_ndjson_entities(self, mock_response):
        mock_response.side_effect = lambda r: str(r)
        entities = [{'a': 'b'}, 5, "s"]
        convert = lambda e: json.dumps(e)
        result = ndjson_entities(entities, convert)
        json_result = [json.loads(r) for r in result]
        self.assertEqual(json_result, entities)

