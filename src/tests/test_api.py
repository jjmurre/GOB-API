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


class MockGOBViews:
    views = {}

    def get_views(self, catalog, collection):
        return self.views

mockRequest = MockRequest()

catalogs = []
catalog = None
collections = []
collection = None
entities = []
entity = None
views = {}


def mock_entities(collection, offset, limit, view=None):
    global entities

    return entities, len(entities)


def before_each_api_test(monkeypatch):
    import flask
    importlib.reload(flask)

    import flask_cors
    importlib.reload(flask_cors)

    import gobapi.storage
    importlib.reload(gobapi.storage)

    import gobapi.core.model
    importlib.reload(gobapi.core.model)

    import gobapi.response
    importlib.reload(gobapi.response)

    global catalogs, catalog
    global collections, collection
    global entities, entity

    catalogs = {}
    catalog = None
    collections = []
    collection = None
    entities = []
    entity = None
    view = None

    monkeypatch.setattr(flask, 'Flask', MockFlask)
    monkeypatch.setattr(flask_cors, 'CORS', MockCORS)

    monkeypatch.setattr(flask, 'request', mockRequest)

    monkeypatch.setattr(gobapi.response, 'hal_response', lambda data, links=None: (data, links))
    monkeypatch.setattr(gobapi.response, 'not_found', lambda msg: msg)

    monkeypatch.setattr(gobapi.core.model, 'get_catalogs', lambda: catalogs)
    monkeypatch.setattr(gobapi.core.model, 'get_catalog', lambda name: catalog)
    monkeypatch.setattr(gobapi.core.model, 'get_collections', lambda name: collections)
    monkeypatch.setattr(gobapi.core.model, 'get_collection', lambda name1, name2: collection)

    monkeypatch.setattr(gobapi.core.views, 'GOBViews', MockGOBViews)

    monkeypatch.setattr(gobapi.storage, 'connect', noop)
    monkeypatch.setattr(gobapi.storage, 'get_entities', mock_entities)
    monkeypatch.setattr(gobapi.storage, 'get_entity', lambda name, id, view: entity)

    import gobapi.api
    importlib.reload(gobapi.api)


def test_app(monkeypatch):
    before_each_api_test(monkeypatch)


    from gobapi.api import get_app
    assert(not get_app() == None)


def test_catalogs(monkeypatch):
    global catalogs

    before_each_api_test(monkeypatch)

    from gobapi.api import _catalogs
    assert(_catalogs() == (({'catalogs': []}, None), 200, {'Content-Type': 'application/json'}))

    catalogs = {'catalog': {}}
    assert(_catalogs() == (({'catalogs': [{'href': '/gob/catalog/', 'name': 'catalog'}]}, None), 200, {'Content-Type': 'application/json'}))


def test_catalog(monkeypatch):
    global catalog

    before_each_api_test(monkeypatch)

    from gobapi.api import _catalog
    assert(_catalog('catalog_name') == 'Catalog catalog_name not found')

    catalog = {
        'description': 'description',
        'collections': []
    }
    assert(_catalog('catalog_name') == (({'collections': [], 'description': 'description'}, None), 200, {'Content-Type': 'application/json'}))


def test_entities(monkeypatch):
    global collection

    before_each_api_test(monkeypatch)

    from gobapi.api import _entities
    collection = 'collection'
    assert(_entities('catalog', 'collection', 1, 1) == ({'page_size': 1, 'pages': 0, 'results': [], 'total_count': 0}, {'next': None, 'previous': None}))


def test_entities_with_view(monkeypatch):
    global collection, views

    before_each_api_test(monkeypatch)

    from gobapi.api import _entities
    collection = 'collection'
    assert(_entities('catalog', 'collection', 1, 1, 'enhanced') == ({'page_size': 1, 'pages': 0, 'results': [], 'total_count': 0}, {'next': None, 'previous': None}))


def test_entity(monkeypatch):
    global catalog, collection
    global entity

    before_each_api_test(monkeypatch)

    from gobapi.api import _entity
    assert(_entity('catalog', 'collection', '1') == 'catalog.collection not found')

    catalog = 'catalog'
    assert(_entity('catalog', 'collection', '1') == 'catalog.collection not found')

    collection = 'collection'
    entity = None
    assert(_entity('catalog', 'collection', '1') == 'catalog.collection:1 not found')

    entity = {'id': 1}
    assert(_entity('catalog', 'collection', 1) == ((entity, None), 200, {'Content-Type': 'application/json'}))


def test_entity_with_view(monkeypatch):
    global mockRequest
    global catalog, collection
    global entity

    before_each_api_test(monkeypatch)

    from gobapi.api import _entity
    mockRequest.args = {
        'view': 'enhanced'
    }
    assert(_entity('catalog', 'collection', '1') == 'catalog.collection not found')

    catalog = 'catalog'
    assert(_entity('catalog', 'collection', '1') == 'catalog.collection not found')

    collection = 'collection'
    entity = None
    assert(_entity('catalog', 'collection', '1') == 'catalog.collection?view=enhanced not found')

    MockGOBViews.views = {
        'enhanced': {}
    }
    assert(_entity('catalog', 'collection', '1') == 'catalog.collection:1 not found')

    entity = {'id': 1}
    assert(_entity('catalog', 'collection', 1) == ((entity, None), 200, {'Content-Type': 'application/json'}))


def test_collection(monkeypatch):
    global mockRequest
    global catalog, collection
    global entities, entity

    before_each_api_test(monkeypatch)

    from gobapi.api import _collection
    assert(_collection('catalog', 'collection') == 'catalog.collection not found')

    catalog = 'catalog'
    collection = 'collection'

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
        ), 200, {'Content-Type': 'application/json'}))

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
        ), 200, {'Content-Type': 'application/json'}))


def test_collection_with_view(monkeypatch):
    global mockRequest
    global catalog, collection
    global entities, entity

    before_each_api_test(monkeypatch)

    from gobapi.api import _collection
    assert(_collection('catalog', 'collection') == 'catalog.collection not found')

    catalog = 'catalog'
    collection = 'collection'

    mockRequest.args = {
        'view': 'enhanced'
    }
    MockGOBViews.views = {}
    assert(_collection('catalog', 'collection') == 'catalog.collection?view=enhanced not found')

    MockGOBViews.views = {
        'enhanced': {}
    }
    assert(_collection('catalog', 'collection') == (
        ({
             'page_size': 100,
             'pages': 0,
             'results': [],
             'total_count': 0
         },{
             'next': None,
             'previous': None}
        ), 200, {'Content-Type': 'application/json'}))



def test_health(monkeypatch):
    before_each_api_test(monkeypatch)

    from gobapi.api import _health
    assert(_health() == 'Connectivity OK')


def test_wsgi(monkeypatch):
    before_each_api_test(monkeypatch)

    from gobapi.wsgi import application
    assert(not application == None)
