"""Response

This module contains API response utility methods

API responses are in HAL JSON format (http://stateless.co/hal_specification.html).
hal_response is a method that converts an API response to HAL JSON

Paged output contains links to any next or previous page
get_page_ref contains logic to format a page link

When a requested item can not be found, a 404 not found is returned
The not_found method provides for logic to generate 404 responses

"""
import json
import urllib

from flask import request
from gobapi.json import APIGobTypeJSONEncoder
from gobapi.utils import dict_to_camelcase


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


def hal_response(data, links=None):
    """HAL JSON response

    Converts the specified data and links to a HAL JSON document

    :param data: any data object
    :param links: any links to related data, eg next and previous page
    :return:
    """
    self = request.path
    if len(request.args):
        self += f'?{urllib.parse.urlencode(request.args)}'
    links = links or {}
    links['self'] = self

    response = dict_to_camelcase({
        '_links': {key: {'href': href} for key, href in links.items()},
        **data
    })

    return json.dumps(response, cls=APIGobTypeJSONEncoder), 200, {'Content-Type': 'application/json'}


def stream_response(data):
    response = dict_to_camelcase(data)
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
