from gobcore.model import GOBModel

from gobapi.utils import to_snake


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
