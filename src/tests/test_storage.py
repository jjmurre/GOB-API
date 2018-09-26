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
    class model:
        def get_model(self, name):
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
            }[name]

    return model()

def before_each_storage_test(monkeypatch):
    import sqlalchemy
    importlib.reload(sqlalchemy)
    import sqlalchemy.ext.automap
    importlib.reload(sqlalchemy.ext.automap)
    import sqlalchemy.orm
    importlib.reload(sqlalchemy.orm)

    import gobapi.config
    importlib.reload(gobapi.config)

    import gobcore.model
    importlib.reload(gobapi.config)

    monkeypatch.setattr(sqlalchemy, 'create_engine', mock_create_engine)
    monkeypatch.setattr(sqlalchemy.ext.automap, 'automap_base', mock_automap_base)
    monkeypatch.setattr(sqlalchemy.orm, 'Session', MockSession)

    monkeypatch.setattr(gobcore.model, 'GOBModel', mock_get_gobmodel)

    import gobapi.storage
    importlib.reload(gobapi.storage)

    from gobapi.storage import connect
    connect()


def test_entities(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import get_entities
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

    from gobapi.storage import get_entity
    assert(get_entity('collection1', 'id') == None)

    mockEntity = MockEntity('id', 'attribute')
    MockEntities.one_entity = mockEntity
    assert(get_entity('collection1', 'id') == {'attribute': 'attribute', 'id': 'id'})
