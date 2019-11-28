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
        _to_camelcase(snake_case) => snakeCase

    :param s: string to convert to camelCase
    :return:
    """
    def _camelcase_converter(m):
        return m.group(1).upper()

    _RE_TO_CAMELCASE = re.compile(r'(?!^)_([a-zA-Z])')
    return re.sub(_RE_TO_CAMELCASE, _camelcase_converter, s)
