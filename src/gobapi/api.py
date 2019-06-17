"""API

This module contains the API endpoints.
Endpoints can use storage methods to retrieve data from the GOB Storage.
Responses are created in a uniform way by using the response module.

The endpoints are defined in the ROUTES variable.
The only public method is get_app() which returns a Flask application object.
The API can be started by get_app().run()

"""
from flask_graphql import GraphQLView
from flask import Flask, request
from flask_cors import CORS

from gobcore.model import GOBModel
from gobcore.views import GOBViews

from gobapi.config import API_BASE_PATH, API_INFRA_SERVICES
from gobapi.response import hal_response, not_found, get_page_ref
from gobapi.states import get_states
from gobapi.storage import connect, get_entities, get_entity, shutdown_session

from gobapi.graphql.schema import schema
from gobapi import infra


def _catalogs():
    """Returns the GOB catalogs

    :return: a list of catalogs (name, href)
    """
    result = {
        '_embedded': {
            'catalogs': [
                {
                    'name': catalog_name,
                    'description': catalog['description'],
                    '_links': {
                        'self': {
                            'href': f'{API_BASE_PATH}/{catalog_name}/'
                        }
                    }
                } for catalog_name, catalog in GOBModel().get_catalogs().items()
            ]
        }
    }
    return hal_response(result)


def _catalog(catalog_name):
    """Return the details of a specific GOB catalog

    :param catalog_name: e.g. meetbouten
    :return: the details of the specified catalog {name, href}
    """
    catalog = GOBModel().get_catalog(catalog_name)
    if catalog:
        result = {
            'description': catalog['description'],
            '_embedded': {
                'collections': [
                    {
                        'name': collection_name,
                        '_links': {
                            'self': {
                                'href': f'/gob/{catalog_name}/{collection_name}/'
                            }
                        }
                    } for collection_name in GOBModel().get_collection_names(catalog_name)
                ]
            }
        }
        return hal_response(result)
    else:
        return not_found(f"Catalog {catalog_name} not found")


def _entities(catalog_name, collection_name, page, page_size, view=None):
    """Returns the entities in the specified catalog collection

    The page and page_size are used to calculate the offset and number of entities to return

    A result, links tuple is returned.
    Result is an object containing relevant metadata about the result
    Links contain the references to any next or previous page

    :param catalog_name: e.g. meetbouten
    :param collection_name: e.g. meting
    :param page: any page number, page numbering starts at 1
    :param page_size: the number of entities per page
    :param view: the database view that's being used to get the entities, defaults to the entity table
    :return: (result, links)
    """
    assert (GOBModel().get_collection(catalog_name, collection_name))
    assert (page >= 1)
    assert (page_size >= 1)

    offset = (page - 1) * page_size

    entities, total_count = get_entities(catalog_name, collection_name, offset=offset, limit=page_size, view=view)

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


def _collection(catalog_name, collection_name):
    """Returns the list of entities within the specified collection

    A list of entities is returned. This output is paged, default page 1 page size 100

    :param catalog_name: e.g. meetbouten
    :param collection_name: e.g. meting
    :return:
    """

    if GOBModel().get_collection(catalog_name, collection_name):
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('page_size', 100))

        view = request.args.get('view', None)

        # If a view is requested and doesn't exist return a 404
        if view and view not in GOBViews().get_views(catalog_name, collection_name):
            return not_found(f'{catalog_name}.{collection_name}?view={view} not found')

        view_name = f"{catalog_name}_{collection_name}_{view}" if view else None

        result, links = _entities(catalog_name, collection_name, page, page_size, view_name)
        return hal_response(data=result, links=links)
    else:
        return not_found(f'{catalog_name}.{collection_name} not found')


def _entity(catalog_name, collection_name, entity_id, view=None):
    """Returns the entity within the specified collection with the specified id

    An individual entity is returned.

    :param catalog_name: e.g. meetbouten
    :param collection_name: e.g. meting
    :param entity_id: unique identifier of the entity
    :param view: the database view that's being used to get the entity, defaults to the entity table
    :return:
    """
    if GOBModel().get_collection(catalog_name, collection_name):
        view = request.args.get('view', None)

        # If a view is requested and doesn't exist return a 404
        if view and view not in GOBViews().get_views(catalog_name, collection_name):
            return not_found(f'{catalog_name}.{collection_name}?view={view} not found')

        view_name = f"{catalog_name}_{collection_name}_{view}" if view else None

        result = get_entity(catalog_name, collection_name, entity_id, view_name)
        return hal_response(result) if result is not None else not_found(
            f'{catalog_name}.{collection_name}:{entity_id} not found')
    else:
        return not_found(f'{catalog_name}.{collection_name} not found')


def _states():
    """Returns the states for the supplied list of collections

    All states for a collection with the related collections are returned.
    The list of collections can be passed as an URL parameter:

    ?collections=gebieden:wijken,gebieden:stadsdelen

    :return:
    """
    collection_names = request.args.get('collections')
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', 100))
    offset = (page - 1) * page_size

    if collection_names:
        collections = []
        for c in collection_names.split(','):
            collections.append(c.split(':'))

        entities, total_count = get_states(collections, offset=offset, limit=page_size)

        num_pages = (total_count + page_size - 1) // page_size

        result = {
            'total_count': total_count,
            'page_size': page_size,
            'pages': num_pages,
            'results': entities
        }
        links = {
            'next': get_page_ref(page + 1, num_pages),
            'previous': get_page_ref(page - 1, num_pages)
            }
        return hal_response(result, links)
    else:
        return not_found(f'No collections requested')


def _health():
    return 'Connectivity OK'


class GOBFlask(Flask):
    _infra_threads = None

    def run(self, *args, **kwargs):
        self._infra_threads = infra.start_all_services(
            API_INFRA_SERVICES
        )
        super().run(*args, **kwargs)


def get_app():
    """Returns a Flask application object

    The rules are maintained in the ROUTES variable (Note: By default a rule just listens for GET)

    CORS is used to allow CORS for all domains on all routes

    :return: a Flask application object
    """
    connect()

    graphql = GraphQLView.as_view(
        'graphql',
        schema=schema,
        graphiql=True  # for having the GraphiQL interface
    )

    ROUTES = [
        # Health check URL
        ('/status/health/', _health),

        (f'{API_BASE_PATH}/', _catalogs),
        (f'{API_BASE_PATH}/<catalog_name>/', _catalog),
        (f'{API_BASE_PATH}/<catalog_name>/<collection_name>/', _collection),
        (f'{API_BASE_PATH}/<catalog_name>/<collection_name>/<entity_id>/', _entity),

        # Get states with history for a list of collections
        (f'{API_BASE_PATH}/toestanden/', _states),

        (f'{API_BASE_PATH}/graphql/', graphql)
    ]

    # app = GOBFlask(__name__)
    app = Flask(__name__)
    CORS(app)

    # app.teardown_appcontext(shutdown_session)

    for route, view_func in ROUTES:
        app.route(rule=route)(view_func)

    return app
