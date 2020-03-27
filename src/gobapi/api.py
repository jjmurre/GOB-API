"""API

This module contains the API endpoints.
Endpoints can use storage methods to retrieve data from the GOB Storage.
Responses are created in a uniform way by using the response module.

The endpoints are defined in the ROUTES variable.
The only public method is get_app() which returns a Flask application object.
The API can be started by get_app().run()

"""
import json

from flask_graphql import GraphQLView
from flask import Flask, request, Response
from flask_cors import CORS

from gobcore.model import GOBModel
from gobcore.views import GOBViews

from gobapi.config import API_BASE_PATH, API_SECURE_BASE_PATH
from gobapi.fat_file import fat_file
from gobapi.response import hal_response, not_found, get_page_ref, ndjson_entities, stream_entities
from gobapi.dump.csv import csv_entities
from gobapi.dump.sql import sql_entities
from gobapi.dump.to_db import dump_to_db
from gobapi.auth.routes import secure_route, public_route

from gobapi.worker.response import WorkerResponse
from gobapi.worker.api import worker_result, worker_status, worker_end

from gobapi.states import get_states
from gobapi.storage import connect, get_entities, get_entity, query_entities, dump_entities, query_reference_entities,\
    clear_test_dbs
from gobapi.dbinfo.api import get_db_info

from gobapi.graphql.schema import schema
from gobapi.session import shutdown_session
from gobapi.graphql_streaming.api import GraphQLStreamingApi


def _catalogs():
    """Returns the GOB catalogs

    :return: a list of catalogs (name, href)
    """
    result = {
        '_embedded': {
            'catalogs': [
                {
                    'name': catalog_name,
                    'abbreviation': catalog['abbreviation'],
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
            'name': catalog_name,
            'abbreviation': catalog['abbreviation'],
            'description': catalog['description'],
            'version': catalog['version'],
            'collections': [a for a in catalog['collections'].keys()],
            '_embedded': {
                'collections': [
                    {
                        'name': collection_name,
                        'abbreviation': collection['abbreviation'],
                        '_links': {
                            'self': {
                                'href': f'/gob/{catalog_name}/{collection_name}/'
                            }
                        }
                    } for collection_name, collection in GOBModel().get_collections(catalog_name).items()
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

    if view:
        # For views always show next page unless no results are returned. Count is slow on large views
        num_pages = page + 1 if len(entities) > 0 else page
    else:
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


def _clear_tests():
    clear_test_dbs()
    return "", 200


def _reference_entities(src_catalog_name, src_collection_name, reference_name, src_id, page, page_size):
    """Returns the entities for a reference with specified source entity

    The page and page_size are used to calculate the offset and number of entities to return

    A result, links tuple is returned.
    Result is an object containing relevant metadata about the result
    Links contain the references to any next or previous page

    :param src_catalog_name: e.g. meetbouten
    :param src_collection_name: e.g. metingen
    :param reference_name: e.g. ligt_in_buurt
    :param src_id: e.g. 1234
    :param page: any page number, page numbering starts at 1
    :param page_size: the number of entities per page
    :return: (result, links)
    """
    assert (GOBModel().get_collection(src_catalog_name, src_collection_name)['references'].get(reference_name))
    assert (page >= 1)
    assert (page_size >= 1)

    offset = (page - 1) * page_size

    entities, total_count = get_entities(src_catalog_name, src_collection_name, offset=offset, limit=page_size,
                                         view=None, reference_name=reference_name, src_id=src_id)

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


def _dump(catalog_name, collection_name):
    """
    Dump all entities in the requested format. Currently only csv

    :param catalog_name:
    :param collection_name:
    :return: Streaming response of all entities in csv format with header
    """
    method = request.method

    if method == 'GET':
        format = request.args.get('format')
        entities, model = dump_entities(catalog_name, collection_name)

        if format == "csv":
            result = csv_entities(entities, model)
            return WorkerResponse.stream_with_context(result, mimetype='text/csv')
        elif format == "sql":
            return Response(sql_entities(catalog_name, collection_name, model), mimetype='application/sql')
        else:
            return f"Unrecognised format parameter '{format}'" if format else "Format parameter not set", 400
    elif method == 'POST':
        content_type = request.content_type
        if content_type == 'application/json':
            config = json.loads(request.data)
            result = dump_to_db(catalog_name, collection_name, config)
            return WorkerResponse.stream_with_context(result, mimetype='text/plain')
        else:
            return f"Unrecognised content type '{content_type}'", 400


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

        stream = request.args.get('stream', None) == "true"
        ndjson = request.args.get('ndjson', None) == "true"

        # If a view is requested and doesn't exist return a 404
        if view and not GOBViews().get_view(catalog_name, collection_name, view):
            return not_found(f'{catalog_name}.{collection_name}?view={view} not found')

        view_name = GOBViews().get_view(catalog_name, collection_name, view)['name'] if view else None

        if stream:
            entities, convert = query_entities(catalog_name, collection_name, view_name)
            result = stream_entities(entities, convert)
            return WorkerResponse.stream_with_context(result, mimetype='application/json')
        elif ndjson:
            entities, convert = query_entities(catalog_name, collection_name, view_name)
            result = ndjson_entities(entities, convert)
            return WorkerResponse.stream_with_context(result, mimetype='application/x-ndjson')
        else:
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
        if view and not GOBViews().get_view(catalog_name, collection_name, view):
            return not_found(f'{catalog_name}.{collection_name}?view={view} not found')

        view_name = GOBViews().get_view(catalog_name, collection_name, view)['name'] if view else None

        result = get_entity(catalog_name, collection_name, entity_id, view_name)
        return hal_response(result) if result is not None else not_found(
            f'{catalog_name}.{collection_name}:{entity_id} not found')
    else:
        return not_found(f'{catalog_name}.{collection_name} not found')


def _reference_collection(catalog_name, collection_name, entity_id, reference_path):
    """Returns the (very many) references from an entity within the specified collection
    with the specified id

    An list of references is returned.

    :param catalog_name: e.g. meetbouten
    :param collection_name: e.g. meting
    :param entity_id: unique identifier of the entity
    :param reference: unique identifier of the reference attribute e.g. ligt_in_buurt
    :param view: the database view that's being used to get the entity, defaults to the entity table
    :return:
    """
    model = GOBModel()
    entity_collection = model.get_collection(catalog_name, collection_name)

    if entity_collection:
        # Get the reference
        reference_name = reference_path.replace('-', '_')
        reference = model.get_collection(catalog_name, collection_name)['references'].get(reference_name)
        # Check if the source entity exists
        entity = get_entity(catalog_name, collection_name, entity_id)

        if entity and reference:
            page = int(request.args.get('page', 1))
            page_size = int(request.args.get('page_size', 100))

            stream = request.args.get('stream', None) == "true"
            ndjson = request.args.get('ndjson', None) == "true"

            if stream:
                entities, convert = query_reference_entities(catalog_name, collection_name, reference_name, entity_id)
                return Response(stream_entities(entities, convert), mimetype='application/json')
            elif ndjson:
                entities, convert = query_reference_entities(catalog_name, collection_name, reference_name, entity_id)
                return Response(ndjson_entities(entities, convert), mimetype='application/x-ndjson')
            else:
                result, links = _reference_entities(catalog_name, collection_name, reference_name, entity_id,
                                                    page, page_size)
                return hal_response(data=result, links=links)

        response = not_found(f'{catalog_name}.{collection_name}:{entity_id} not found') \
            if not entity else not_found(f'{catalog_name}.{collection_name}:{entity_id}:{reference_name} not found')
        return response
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


def _add_route(app, paths, rule, view_func, methods):
    """
    For every rule add a public and a secure endpoint

    Both the public and the secure endpoints are protected.
    The secure endpoint expects the keycloak headers to be present and the endpoint is protected by gatekeeper
    The public endpoint assures that none of the keycloak headers is present

    :param app:
    :param rule:
    :param view_func:
    :param methods:
    :return:
    """
    wrappers = {
        API_BASE_PATH: public_route,
        API_SECURE_BASE_PATH: secure_route
    }
    for path in paths:
        wrapper = wrappers[path]
        wrapped_rule = f"{path}{rule}"
        app.add_url_rule(rule=wrapped_rule, methods=methods, view_func=wrapper(wrapped_rule, view_func))


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
    graphql_streaming = GraphQLStreamingApi()

    app = Flask(__name__)
    CORS(app)

    # Health check route
    app.route(rule='/status/health/')(_health)

    # Application routes
    PUBLIC = [API_BASE_PATH, API_SECURE_BASE_PATH]
    # SECURE = [API_SECURE_BASE_PATH]
    ROUTES = [
        (PUBLIC, '/', _catalogs, ['GET']),
        (PUBLIC, '/<catalog_name>/', _catalog, ['GET']),
        (PUBLIC, '/<catalog_name>/<collection_name>/', _collection, ['GET']),
        (PUBLIC, '/<catalog_name>/<collection_name>/<entity_id>/', _entity, ['GET']),
        (PUBLIC, '/<catalog_name>/<collection_name>/<entity_id>/<reference_path>/', _reference_collection, ['GET']),
        (PUBLIC, '/alltests/', _clear_tests, ['DELETE']),
        (PUBLIC, '/toestanden/', _states, ['GET']),
        (PUBLIC, '/graphql/', graphql, ['GET', 'POST']),
        (PUBLIC, '/graphql/streaming/', graphql_streaming.entrypoint, ['POST']),
        (PUBLIC, '/dump/<catalog_name>/<collection_name>/', _dump, ['GET', 'POST']),
        (PUBLIC, '/worker/<worker_id>', worker_result, ['GET']),
        (PUBLIC, '/worker/end/<worker_id>', worker_end, ['DELETE']),
        (PUBLIC, '/worker/status/<worker_id>', worker_status, ['GET']),
        (PUBLIC, '/fat_file/<gbs>', fat_file, ['GET']),
        (PUBLIC, '/info/<info_type>/', get_db_info, ['GET'])
    ]
    for paths, rule, view_func, methods in ROUTES:
        _add_route(app, paths, rule, view_func, methods)

    app.teardown_appcontext(shutdown_session)

    return app
