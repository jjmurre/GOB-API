from gobcore.views import GOBViews


def get_view(catalog_name, collection_name, view):
    """ View

    Returns the view with the specified name within the specified catalog
    and collection. If any of the catalog, collection or view does not exist,
    None is returned

    :param catalog_name:
    :param collection_name:
    :param view:
    :return:
    """
    views = GOBViews().get_views(catalog_name, collection_name)
    return view if view in views else None
