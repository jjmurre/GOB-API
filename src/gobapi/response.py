"""Response

This module contains API response utility methods

API responses are in HAL JSON format (http://stateless.co/hal_specification.html).
hal_response is a method that converts an API response to HAL JSON

Paged output contains links to any next or previous page
get_page_ref contains logic to format a page link

When a requested item can not be found, a 404 not found is returned
The not_found method provides for logic to generate 404 responses

"""
import datetime
import simplejson as json
import urllib

from flask import request


def _as_json(data):
    """Converts data to json

    :param data: any data object
    :return: the data in json format
    """
    def custom_encode(value):
        """Encoder for any types that are not supported by teh json library

        :param value: any value
        :return: the encoded value
        """
        if isinstance(value, datetime.date):
            return datetime.date.isoformat(value)

    return json.dumps(data,  default=custom_encode)


def _error_response(error, msg):
    """JSON error response

    This method is used to generate uniformly formatted error response

    :param error: the error code, eg 404
    :param msg: the message that describes the error
    :return:
    """
    return _as_json({
        'error': error,
        'text': str(msg)
    }), error


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

    response = {
        '_links': {key: {'href': href} for key, href in links.items()}
    }
    response.update(data)

    return _as_json(response)


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
