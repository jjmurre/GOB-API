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

    def teardown_appcontext(self, func):
        return None


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


class MockGOBModel:
    def __init__(self):
        pass

    def get_catalogs(self):
        global catalogs
        return catalogs

    def get_catalog(self, catalog_name):
        global catalog
        return catalog

    def get_collection_names(self, catalog_name):
        global catalog
        return catalog['collections'].keys()

    def get_collection(self, catalog_name, collection_name):
        global collection
        return collection


def mock_entities(catalog, collection, offset, limit, view=None):
    global entities

    return entities, len(entities)


def before_each_api_test(monkeypatch):
    import flask
    importlib.reload(flask)

    import flask_cors
    importlib.reload(flask_cors)

    import gobcore.model
    importlib.reload(gobcore.model)

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

    monkeypatch.setattr(gobapi.response, 'hal_response', lambda data, links=None: ((data, links), 200, {'Content-Type': 'application/json'}))
    monkeypatch.setattr(gobapi.response, 'not_found', lambda msg: msg)

    monkeypatch.setattr(gobcore.views, 'GOBViews', MockGOBViews)

    monkeypatch.setattr(gobapi.storage, 'connect', noop)
    monkeypatch.setattr(gobapi.storage, 'get_entities', mock_entities)
    monkeypatch.setattr(gobapi.storage, 'get_entity', lambda catalog, collection, id, view: entity)

    monkeypatch.setattr(gobapi.states, 'get_states', lambda collections, offset, limit: ([{'id': '1', 'attribute': 'attribute'}], 1))

    monkeypatch.setattr(gobcore.model, 'GOBModel', MockGOBModel)

    import gobapi.api
    importlib.reload(gobapi.api)

    import gobapi.storage
    importlib.reload(gobapi.storage)

    import gobapi.states
    importlib.reload(gobapi.states)


def test_app(monkeypatch):
    before_each_api_test(monkeypatch)


    from gobapi.api import get_app
    assert(not get_app() == None)


def test_catalogs(monkeypatch):
    global catalogs

    before_each_api_test(monkeypatch)

    from gobapi.api import _catalogs
    assert(_catalogs() == (({'_embedded': {'catalogs':[] }}, None), 200, {'Content-Type': 'application/json'}))

    catalogs = {'catalog': {'description': 'catalog'}}
    assert(_catalogs() == (({'_embedded': {'catalogs': [{'_links': {'self': {'href': '/gob/catalog/'}}, 'name': 'catalog', 'description': 'catalog'}]}}, None), 200, {'Content-Type': 'application/json'}))


def test_catalog(monkeypatch):
    global catalog

    before_each_api_test(monkeypatch)

    from gobapi.api import _catalog
    assert(_catalog('catalog_name') == 'Catalog catalog_name not found')

    catalog = {
        'description': 'description',
        'collections': {}
    }
    assert(_catalog('catalog_name') == (({'_embedded': {'collections': []}, 'description': 'description'}, None), 200, {'Content-Type': 'application/json'}))


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


def test_states(monkeypatch):
    global mockRequest
    global catalog, collection
    global entities, entity

    before_each_api_test(monkeypatch)

    from gobapi.api import _states
    assert(_states() == 'No collections requested')

    catalog = 'catalog'
    collection = {'references': {}}

    mockRequest.args = {
        'collections': 'catalog:collection'
    }
    assert(_states() == (
        ({
             'page_size': 100,
             'pages': 1,
             'results': [{'id': '1', 'attribute': 'attribute'}],
             'total_count': 1
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
