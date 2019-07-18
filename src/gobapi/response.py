"""Response

This module contains API response utility methods

API responses are in HAL JSON format (http://stateless.co/hal_specification.html).
hal_response is a method that converts an API response to HAL JSON

Paged output contains links to any next or previous page
get_page_ref contains logic to format a page link

When a requested item can not be found, a 404 not found is returned
The not_found method provides for logic to generate 404 responses

"""
import re
import json
import urllib

from flask import request
from gobapi.json import APIGobTypeJSONEncoder


def _to_camelcase(s):
    """Converts a snake_case string to camelCase

    Example:
        _to_camelcase(snake_case) => snakeCase

    :param s: string to convert to camelCase
    :return:
    """
    def _camelcase_converter(m):
        return m.group(1).upper()

    _RE_TO_CAMELCASE = re.compile(r'(?!^)_([a-zA-Z])')
    return re.sub(_RE_TO_CAMELCASE, _camelcase_converter, s)


def _dict_to_camelcase(d):
    """Converts a dict with snake_case key names to a dict with camelCase key names

    Recursive function to convert dictionaries with arbitrary depth to camelCase dictionaries

    Example:
        _dict_to_camelcase({"snake_case": "value}) => {"snakeCase": "value}

    :param d:
    :return:
    """

    def item_to_camelcase(value):
        if isinstance(value, list):
            return [item_to_camelcase(v) for v in value]
        elif isinstance(value, dict):
            return _dict_to_camelcase(value)
        else:
            return value

    obj = {}
    for key, value in d.items():
        obj[_to_camelcase(key)] = item_to_camelcase(value)
    return obj


def _error_response(error, msg):
    """JSON error response

    This method is used to generate uniformly formatted error response

    :param error: the error code, eg 404
    :param msg: the message that describes the error
    :return:
    """
    return json.dumps({
        'error': error,
        'text': str(msg)
    }), error, {'Content-Type': 'application/json'}


def hal_response(data, links={}):
    """HAL JSON response

    Converts the specified data and links to a HAL JSON document

    :param data: any data object
    :param links: any links to related data, eg next and previosu page
    :return:
    """
    self = request.path
    if len(request.args):
        self += f'?{urllib.parse.urlencode(request.args)}'
    links['self'] = self

    response = _dict_to_camelcase({
        '_links': {key: {'href': href} for key, href in links.items()},
        **data
    })

    return json.dumps(response, cls=APIGobTypeJSONEncoder), 200, {'Content-Type': 'application/json'}


def stream_response(data):
    response = _dict_to_camelcase(data)
    return json.dumps(response, cls=APIGobTypeJSONEncoder)


def not_found(msg):
    """Not found

    Provides for a standard not found response

    :param msg: the message that describes what was not found
    :return:
    """
    return _error_response(404, msg)


def get_page_ref(page, num_pages):
    """Page reference

    Returns a page reference link to a page.
    The page has to be within the range [1, num_pages]
    Otherwise None is returned

    :param page:
    :param num_pages:
    :return:
    """
    if 1 <= page and page <= num_pages:
        args = request.args.copy()
        args['page'] = page
        return f'{request.path}?{urllib.parse.urlencode(args)}'


def stream_entities(entities, convert):
    yield("[")
    empty = True
    for entity in entities:
        yield ("" if empty else ",") + stream_response(convert(entity))
        empty = False
    yield("]")


def ndjson_entities(entities, convert):
    for entity in entities:
        yield stream_response(convert(entity)) + "\n"
