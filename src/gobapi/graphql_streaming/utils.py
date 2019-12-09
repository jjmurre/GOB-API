import re

from gobcore.model import GOBModel


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


def resolve_schema_collection_name(schema_collection_name: str):
    """
    Resolve catalog and collection from schema collection name

    :param schema_collection_name:
    :return:
    """
    model = GOBModel()
    names = to_snake(schema_collection_name).split('_')
    for n in range(1, len(names)):
        catalog_name = '_'.join(names[:-n])
        collection_name = '_'.join(names[-n:])
        catalog = model.get_catalog(catalog_name)
        collection = model.get_collection(catalog_name, collection_name)
        if catalog and collection:
            return catalog_name, collection_name
    return None, None
