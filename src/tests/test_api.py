""""API Unit tests

The unit tests for the API module.
As it is a unit test all external dependencies are mocked

"""
import importlib


def noop(*args):
    pass


class MockFlask:
    def __init__(self, name):
        pass

    def route(self, rule):
        return noop

class MockCORS:
    def __init__(self, app):
        pass

class MockRequest:
    args = {}
    path = 'path'

mockRequest = MockRequest()

catalogs = []
catalog = None
collections = []
collection = None
entities = []
entity = None

def mock_entities(collection, offset, limit):
    global entities

    return entities, len(entities)


def before_each_api_test(monkeypatch):
    import flask
    importlib.reload(flask)

    import flask_cors
    importlib.reload(flask_cors)

    import api.storage
    importlib.reload(api.storage)

    import api.response
    importlib.reload(api.response)

    global catalogs, catalog
    global collections, collection
    global entities, entity

    catalogs = []
    catalog = None
    collections = []
    collection = None
    entities = []
    entity = None

    monkeypatch.setattr(flask, 'Flask', MockFlask)
    monkeypatch.setattr(flask_cors, 'CORS', MockCORS)

    monkeypatch.setattr(flask, 'request', mockRequest)

    monkeypatch.setattr(api.response, 'hal_response', lambda data, links=None: (data, links))
    monkeypatch.setattr(api.response, 'not_found', lambda msg: msg)

    monkeypatch.setattr(api.storage, 'connect', noop)
    monkeypatch.setattr(api.storage, 'get_catalogs', lambda: catalogs)
    monkeypatch.setattr(api.storage, 'get_catalog', lambda name: catalog)
    monkeypatch.setattr(api.storage, 'get_collections', lambda name: collections)
    monkeypatch.setattr(api.storage, 'get_collection', lambda name1, name2: collection)
    monkeypatch.setattr(api.storage, 'get_entities', mock_entities)
    monkeypatch.setattr(api.storage, 'get_entity', lambda name, id: entity)

    import api.api
    importlib.reload(api.api)


def test_app(monkeypatch):
    before_each_api_test(monkeypatch)


    from api.api import get_app
    assert(not get_app() == None)


def test_catalogs(monkeypatch):
    global catalogs

    before_each_api_test(monkeypatch)

    from api.api import _catalogs
    assert(_catalogs() == ({'catalogs': []}, None))

    catalogs = ['catalog']
    assert(_catalogs() == ({'catalogs': [{'href': '/catalog/', 'name': 'catalog'}]}, None))

def test_catalog(monkeypatch):
    global catalog

    before_each_api_test(monkeypatch)

    from api.api import _catalog
    assert(_catalog('catalog_name') == 'Catalog catalog_name not found')

    catalog = {
        'description': 'description',
        'collections': []
    }
    assert(_catalog('catalog_name') == ({'collections': [], 'description': 'description'}, None))

def test_entities(monkeypatch):
    global collection

    before_each_api_test(monkeypatch)

    from api.api import _entities
    collection = 'collection'
    assert(_entities('catalog', 'collection', 1, 1) == ({'page_size': 1, 'pages': 0, 'results': [], 'total_count': 0}, {'next': None, 'previous': None}))

def test_entity(monkeypatch):
    global collection
    global entity

    before_each_api_test(monkeypatch)

    from api.api import _entity
    collection = 'collection'
    assert(_entity('catalog', 'collection', 1) == None)

    entity = {}
    assert(_entity('catalog', 'collection', 1) == {})

def test_collection(monkeypatch):
    global mockRequest
    global catalog, collection
    global entities, entity

    before_each_api_test(monkeypatch)

    from api.api import _collection
    assert(_collection('catalog', 'collection') == 'catalog.collection not found')

    catalog = 'catalog'
    collection = 'collection'
    mockRequest.args = {'id': 1}
    assert(_collection('catalog', 'collection') == 'catalog.collection:1 not found')

    entity = {'id': 1}
    assert(_collection('catalog', 'collection') == ({'id': 1}, None))

    mockRequest.args = {}
    assert(_collection('catalog', 'collection') == (
        ({
             'page_size': 100,
             'pages': 0,
             'results': [],
             'total_count': 0
         },{
             'next': None,
            'previous': None}
        )))

    mockRequest.args = {
        'page': 5,
        'page_size': 10
    }
    assert(_collection('catalog', 'collection') == (
        ({
             'page_size': 10,
             'pages': 0,
             'results': [],
             'total_count': 0
         },{
             'next': None,
             'previous': None}
        )))

    entities = [{'id': 1}]
    assert(_collection('catalog', 'collection') == (
        ({
             'page_size': 10,
             'pages': 1,
             'results': [{'id': 1}],
             'total_count': 1
         },{
             'next': None,
             'previous': None}
        )))
