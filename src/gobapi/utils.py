import re


def to_snake(camel: str):
    """
    Convert a camelCase string to snake

    Example:
        _to_snake(snakeCase) => snake_case

    :param camel:
    :return:
    """
    return re.sub('([A-Z])', r'_\1', camel).lower()


def to_camelcase(s):
    """Converts a snake_case string to camelCase

    Example:
        to_camelcase(snake_case) => snakeCase

    :param s: string to convert to camelCase
    :return:
    """
    def _camelcase_converter(m):
        return m.group(1).upper()

    _RE_TO_CAMELCASE = re.compile(r'(?!^)_([a-zA-Z])')
    return re.sub(_RE_TO_CAMELCASE, _camelcase_converter, s)


def dict_to_camelcase(d):
    """Converts a dict with snake_case key names to a dict with camelCase key names

    Recursive function to convert dictionaries with arbitrary depth to camelCase dictionaries

    Example:
        dict_to_camelcase({"snake_case": "value}) => {"snakeCase": "value}

    :param d:
    :return:
    """
    return d if d is None else {to_camelcase(key): object_to_camelcase(value) for key, value in d.items()}


def object_to_camelcase(value):
    """Converts an object with snake_case key names to an object with camelCase key names."""
    if isinstance(value, list):
        return [object_to_camelcase(v) for v in value]
    elif isinstance(value, dict):
        return dict_to_camelcase(value)
    else:
        return value


def streaming_gob_response(func):
    """Decorator for a function or method that returns a generator that serves as streaming response.

    Decorator performs two actions:
    - If Exception occurs, adds an "GOB_API_ERROR.... " line as last line.
    - Adds an empty line as last line when all items are generated successfully. Client is expected to check for this
    line to verify a successful response.

    This decorator solves the problem that if an error occurs during generation of the response, or the process is
    killed while generating the response, the client receives an incomplete response, but with code 200. The client
    has no idea that an incomplete response is received. By checking the last line the client can always verify the
    response.

    :param func:
    :return:
    """
    def wrapper(*args, **kwargs):
        try:
            yield from func(*args, *kwargs)
            # Add new line to signal successful response
            yield "\n"
        except Exception as e:
            yield f"GOB_API_ERROR. Caught Exception. Response aborted. See logs.\n"

            # Re-raise so that this Exception is logged
            raise e

    return wrapper
