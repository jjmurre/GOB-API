"""API

This module contains the API endpoints.
Endpoints can use storage methods to retrieve data from the GOB Storage.
Responses are created in a uniform way by using the response module.

The endpoints are defined in the ROUTES variable.
The only public method is get_app() which returns a Flask application object.
The API can be started by get_app().run()

"""
from flask import Flask, request
from flask_cors import CORS

from api.storage import connect, get_catalogs, get_catalog, get_collections, get_collection, get_entities, get_entity
from api.response import hal_response, not_found, get_page_ref


def _catalogs():
    """Returns the GOB catalogs

    :return: a list of catalogs (name, href)
    """
    catalogs = [{'name': catalog, 'href': f'/{catalog}/'} for catalog in get_catalogs()]
    return hal_response({'catalogs': catalogs})


def _catalog(catalog_name):
    """Return the details of a specific GOB catalog

    :param catalog_name: e.g. meetbouten
    :return: the details of the specified catalog {name, href}
    """
    catalog = get_catalog(catalog_name)
    if catalog:
        result = {
            'description': catalog['description'],
            'collections': [
                {
                    'name': collection,
                    'href': f'/{catalog_name}/{collection}/'
                } for collection in get_collections(catalog_name)
            ]
        }
        return hal_response(result)
    else:
        return not_found(f"Catalog {catalog_name} not found")


def _entities(catalog_name, collection_name, page, page_size):
    """Returns the entities in the specified catalog collection

    The page and page_size are used to calculate the offset and number of entities to return

    A result, links tuple is returned.
    Result is an object containing relevant metadata about the result
    Links contain the references to any next or previous page

    :param catalog_name: e.g. meetbouten
    :param collection_name: e.g. meting
    :param page: any page number, page numbering starts at 1
    :param page_size: the number of entities per page
    :return: (result, links)
    """
    assert(get_collection(catalog_name, collection_name))
    assert(page >= 1)
    assert(page_size >= 1)

    offset = (page - 1) * page_size

    entities, total_count = get_entities(collection_name, offset=offset, limit=page_size)

    num_pages = (total_count + page_size - 1) // page_size

    return {
        'total_count': total_count,
        'page_size': page_size,
        'pages': num_pages,
        'results': entities
    }, {
        'next': get_page_ref(page + 1, num_pages),
        'previous': get_page_ref(page - 1, num_pages)
    }


def _entity(catalog_name, collection_name, id):
    """Returns a specific identity, identified by it's id, in the specified catalog collection

    :param catalog_name: e.g. meetbouten
    :param collection_name: e.g. meting
    :param id: unique identifier of the entity
    :return: the entity or None if not exists
    """
    assert(get_collection(catalog_name, collection_name))
    assert(id)

    return get_entity(collection_name, id)


def _collection(catalog_name, collection_name):
    """Returns the list of enitites within the specified collection or an individual entity

    When an id parameter is specified an individual entity is returned.
    Otherwise a list of entities is returned. This output is paged, default page 1 page size 100

    :param catalog_name: e.g. meetbouten
    :param collection_name: e.g. meting
    :return:
    """
    if get_collection(catalog_name, collection_name):
        id = request.args.get('id')
        if id:
            result = _entity(catalog_name, collection_name, id)
            return hal_response(result) if result else not_found(f'{catalog_name}.{collection_name}:{id} not found')
        else:
            page = int(request.args.get('page', 1))
            page_size = int(request.args.get('page_size', 100))
            result, links = _entities(catalog_name, collection_name, page, page_size)
            return hal_response(data=result, links=links)
    else:
        return not_found(f'{catalog_name}.{collection_name} not found')


def get_app():
    """Returns a Flask application object

    The rules are maintained in the ROUTES variable (Note: By default a rule just listens for GET)

    CORS is used to allow CORS for all domains on all routes

    :return: a Flask application object
    """
    connect()

    ROUTES = [
        ('/', _catalogs),
        ('/<catalog_name>/', _catalog),
        ('/<catalog_name>/<collection_name>/', _collection)
    ]

    app = Flask(__name__)
    CORS(app)

    for route, view_func in ROUTES:
        app.route(rule=route)(view_func)

    return app
