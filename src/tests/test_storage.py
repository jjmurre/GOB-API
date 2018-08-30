"""Storage Unit tests

The unit tests for the storage module.
As it is a unit test all external dependencies are mocked

"""
import importlib

class MockClasses:
    def __init__(self):
        self.collection1 = 'collection1'

class MockBase:
    def prepare(self, engine, reflect):
        return None

    classes = MockClasses()

class MockEntity:
    def __init__(self, *args):
        for key in args:
            setattr(self, key, key)

class MockEntities:
    all_entities = []
    one_entity = {}

    def count(self):
        return len(self.all_entities)

    def offset(self, offset):
        return self

    def limit(self, limit):
        return self

    def all(self):
        return self.all_entities

    def filter_by(self, **kwargs):
        return self

    def one_or_none(self):
        return self.one_entity

class MockSession:
    def __init__(self, engine):
        pass

    def query(self, table):
        return MockEntities()


def mock_create_engine(url):
    return 'engine'


def mock_automap_base():
    return MockBase()


def mock_get_gobmodel():
    return {
        'collection1': {
            'entity_id': 'id',
            'attributes': {
                'id': {
                    'type': 'GOB.String',
                    'description': 'Unique id of the collection'
                },
                'attribute': {
                    'type': 'GOB.String',
                    'description': 'Some attribute'
                }
            }
        }
    }

mock_CATALOGS = {
    'catalog1': {
        'description': 'Catalog1',
        'collections': [
            'collection1',
            'collection2'
        ]
    },
    'catalog2': {
        'description': 'Catalog2',
        'collections': [
        ]
    },
}


def before_each_storage_test(monkeypatch):
    import sqlalchemy
    importlib.reload(sqlalchemy)
    import sqlalchemy.ext.automap
    importlib.reload(sqlalchemy.ext.automap)
    import sqlalchemy.orm
    importlib.reload(sqlalchemy.orm)

    import api.config
    importlib.reload(api.config)

    monkeypatch.setattr(sqlalchemy, 'create_engine', mock_create_engine)
    monkeypatch.setattr(sqlalchemy.ext.automap, 'automap_base', mock_automap_base)
    monkeypatch.setattr(sqlalchemy.orm, 'Session', MockSession)

    monkeypatch.setattr(api.config, 'CATALOGS', mock_CATALOGS)
    monkeypatch.setattr(api.config, 'get_gobmodel', mock_get_gobmodel)

    import api.storage
    importlib.reload(api.storage)

    from api.storage import connect
    connect()


def test_catalogs(monkeypatch):
    before_each_storage_test(monkeypatch)

    from api.storage import get_catalogs
    assert(get_catalogs() == ['catalog1', 'catalog2'])


def test_catalog(monkeypatch):
    before_each_storage_test(monkeypatch)

    from api.storage import get_catalog
    assert(get_catalog('catalog1') == {'collections': ['collection1', 'collection2'], 'description': 'Catalog1'})
    assert(get_catalog('catalog2') == {'collections': [], 'description': 'Catalog2'})
    assert(get_catalog('non existing catalog') == None)


def test_collections(monkeypatch):
    before_each_storage_test(monkeypatch)

    from api.storage import get_collections
    assert(get_collections('catalog1') == ['collection1', 'collection2'])
    assert(get_collections('catalog2') == [])
    assert(get_collections('non existing catalog') == None)


def test_collection(monkeypatch):
    before_each_storage_test(monkeypatch)

    from api.storage import get_collection
    assert(get_collection('catalog1', 'collection1') == 'collection1')
    assert(get_collection('catalog1', 'collection2') == 'collection2')
    assert(get_collection('catalog1', 'non existing collection') == None)
    assert(get_collection('catalog2', 'non existing collection') == None)
    assert(get_collection('non existing catalog', 'non existing collection') == None)


def test_entities(monkeypatch):
    before_each_storage_test(monkeypatch)

    from api.storage import get_entities
    MockEntities.all_entities = []
    assert(get_entities('collection1', 0, 1) == ([], 0))

    mockEntity = MockEntity('id', 'attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('collection1', 0, 1) == ([{'attribute': 'attribute', 'id': 'id'}], 1))

    mockEntity = MockEntity('id', 'attribute', 'non_existing_attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('collection1', 0, 1) == ([{'attribute': 'attribute', 'id': 'id'}], 1))


def test_entity(monkeypatch):
    before_each_storage_test(monkeypatch)

    from api.storage import get_entity
    assert(get_entity('collection1', 'id') == None)

    mockEntity = MockEntity('id', 'attribute')
    MockEntities.one_entity = mockEntity
    assert(get_entity('collection1', 'id') == {'attribute': 'attribute', 'id': 'id'})
