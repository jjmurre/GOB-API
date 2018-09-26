from gobcore.model import GOBModel


def get_catalogs():
    """Catalogs

    Returns a list of catalog names

    :return:
    """

    # todo: this should be read from configuration file(s)

    return {
        'meetbouten': {
            'description': 'De meetgegevens van bouten in de Amsterdamse panden',
            'collections': GOBModel().get_model_names()
        }
    }


def get_catalog_names():
    """Catalog Names

    Returns the names in the list of catalogs

    :return:
    """
    return get_catalogs().keys()


def get_catalog(catalog_name):
    """Catalog

    Returns the catalog in the list of catalogs with the specified name
    If the name does not exist, None is returned

    :param catalog_name:
    :return:
    """
    catalogs = get_catalogs()
    return catalogs[catalog_name] if catalog_name in catalogs else None


def get_collections(catalog_name):
    """Collections

    Returns the list of collections within the specified catalog

    :param catalog_name:
    :return:
    """
    catalog = get_catalog(catalog_name)
    return catalog['collections'] if catalog is not None else None


def get_collection(catalog_name, collection_name):
    """ Collection

    Returns the collection with the specified name within the specified catalog
    If any of the catalog and collection does not exist, None is returned

    :param catalog_name:
    :param collection_name:
    :return:
    """
    collections = get_collections(catalog_name)
    return collection_name if collections is not None and collection_name in collections else None
