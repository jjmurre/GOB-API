"""Response Unit tests

The unit tests for the response module.
As it is a unit test all external dependencies are mocked

"""
import importlib
import json

from unittest import TestCase
from unittest.mock import patch

from gobcore.typesystem.gob_types import JSON

from gobapi.response import stream_response, ndjson_entities, stream_entities
from gobapi.utils import to_snake, to_camelcase, dict_to_camelcase, object_to_camelcase


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


def test_to_snake():
    assert(to_snake("") == "")
    assert(to_snake("a") == "a")
    assert(to_snake("A") == "_a")
    assert(to_snake("ABC") == "_a_b_c")
    assert(to_snake("firstSecond") == "first_second")


def test_to_camelcase():
    assert(to_camelcase("") == "")
    assert(to_camelcase("a") == "a")
    assert(to_camelcase("_a") == "_a")
    assert(to_camelcase("_a_b_c") == "_aBC")
    assert(to_camelcase("first_second") == "firstSecond")

    assert(to_camelcase("") == "")
    assert(to_camelcase("_") == "_")
    assert(to_camelcase("_____") == "_____")
    assert(to_camelcase("_camel_case_case") == "_camelCaseCase")
    assert(to_camelcase("camel_case_case") == "camelCaseCase")
    assert(to_camelcase("_camel_case_case_") == "_camelCaseCase_")


def test_dict_to_camelcase(monkeypatch):
    before_each_response_test(monkeypatch)

    assert(dict_to_camelcase({}) == {})

    adict = {
        "camel_case1" : "camel_case"
    }
    assert(dict_to_camelcase(adict) == {
        'camelCase1': 'camel_case'
    })

    adict = {
        "camel_case1" : {
            "camel_case2" : "camel_case"
        }
    }
    assert(dict_to_camelcase(adict) == {
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
    assert(dict_to_camelcase(adict) == {
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
    assert(dict_to_camelcase(adict) == {
        'camelCase1': [{'camelCase2': 'camel_case'}]
    })

    adict = {
        "camel_case1" : [[{
            "camel_case2" : "camel_case"
        }]]
    }
    assert(dict_to_camelcase(adict) == {
        'camelCase1': [[{'camelCase2': 'camel_case'}]]
    })


def test_object_to_camelcase(monkeypatch):
    before_each_response_test(monkeypatch)

    assert(object_to_camelcase({}) == {})

    obj = "camel_case1"
    assert(object_to_camelcase(obj) == "camel_case1")

    obj = ["camel_case1", "camel_case2"]
    assert(object_to_camelcase(obj) == ["camel_case1", "camel_case2"])

    obj = {"camel_case1" : "camel_case1", "camel_case2" : "camel_case2"}
    assert(object_to_camelcase(obj) == {"camelCase1": "camel_case1", "camelCase2": "camel_case2"})

    obj = [{"camel_case1" : "camel_case1"}, {"camel_case2" : "camel_case2"}]
    assert(object_to_camelcase(obj) == [{"camelCase1": "camel_case1"}, {"camelCase2": "camel_case2"}])

    obj = [{"camel_case1" : {"camel_case2" : "camel_case"}}]
    assert(object_to_camelcase(obj) == [{"camelCase1": {"camelCase2" : "camel_case"}}])

    obj = [{"camel_case1" : [{"camel_case2" : "camel_case"}]}]
    assert(object_to_camelcase(obj) == [{"camelCase1": [{"camelCase2" : "camel_case"}]}])


class TestStream(TestCase):

    @patch('gobapi.auth.auth_query.request')
    def test_stream(self, mock_request):
        result = stream_response({'some_key': 'some_data'})
        self.assertEqual(result, '{"someKey": "some_data"}')

        result = stream_response({"some_key1": "some_data1", "some_key2": {"some_key3": "some_data3"}})
        self.assertEqual(result, '{"someKey1": "some_data1", "someKey2": {"someKey3": "some_data3"}}')

        result = stream_response({"some_key1": "some_data1", "some_key2": JSON('{"some_key3": "some_data3"}')})
        self.assertEqual(result, '{"someKey1": "some_data1", "someKey2": {"someKey3": "some_data3"}}')

        result = stream_response({"some_key1": "some_data1", "some_key2": JSON('[{"some_key3": "some_data3"}]')})
        self.assertEqual(result, '{"someKey1": "some_data1", "someKey2": [{"someKey3": "some_data3"}]}')

        result = stream_response({"some_key1": "some_data1", "some_key2": [JSON('{"some_key3": "some_data3"}')]})
        self.assertEqual(result, '{"someKey1": "some_data1", "someKey2": [{"someKey3": "some_data3"}]}')

    @patch('gobapi.response.stream_response')
    def test_stream_entities(self, mock_response):
        mock_response.side_effect = lambda r: str(r)
        entities = [{'a': 'b'}, 5, "s"]
        convert = lambda e: json.dumps(e)
        result = list(stream_entities(entities, convert))

        # Trailing "\n" should be added to signal successful response
        expected_result = ['['] + [json.dumps(entities[0])] + [',' + json.dumps(ent) for ent in entities[1:]] + [']\n', '\n']
        self.assertEqual(expected_result, result)

    @patch('gobapi.response.stream_response')
    def test_ndjson_entities(self, mock_response):
        mock_response.side_effect = lambda r: str(r)
        entities = [{'a': 'b'}, 5, "s"]
        convert = lambda e: json.dumps(e)
        result = list(ndjson_entities(entities, convert))

        # Trailing "\n" should be added to signal successful response
        expected_result = [json.dumps(ent) + "\n" for ent in entities] + ["\n"]
        self.assertEqual(expected_result, result)

