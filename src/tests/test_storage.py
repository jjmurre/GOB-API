"""Storage Unit tests

The unit tests for the storage module.
As it is a unit test all external dependencies are mocked

"""
import importlib
import sqlalchemy
import sqlalchemy_filters


class MockClasses:
    def __init__(self):
        self.catalog_collection1 = 'catalog_collection1'
        self.catalog_collection2 = 'catalog_collection2'


class MockBase:
    def prepare(self, engine, reflect):
        return None

    classes = MockClasses()


class MockEntity:
    def __init__(self, *args):
        self._id = 1
        self.reference = {
            'id': 1,
            'bronwaarde': 1
        }
        self.manyreference = [
            {
                'id': 1,
                'bronwaarde': 1
            },
            {
                'id': 2,
                'bronwaarde': 2
            }
        ]
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


class MockColumn:

    def __init__(self, name):
        self.name = name
    type = sqlalchemy.types.VARCHAR()

class MockTable():

    def __init__(self, *args, **kwargs):
        pass

    columns = [MockColumn('id'), MockColumn('attribute'), MockColumn('meta')]


class MockSession:
    def __init__(self, engine):
        self._remove = False
        pass

    def query(self, table):
        return MockEntities()

    def query_property(self):
        pass

    def remove(self):
        self._remove = True

def mock_create_engine(url):
    return 'engine'


def mock_scoped_session(func):
    engine = mock_create_engine('test')
    return MockSession(engine)


def mock_automap_base():
    return MockBase()


mock_PUBLIC_META_FIELDS = {
    "meta": {
        "type": "GOB.String",
        "description": "metadescription"
    }
}


def mock_get_gobmodel():
    class model:
        def get_collection(self, catalog_name, collection_name):
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
                    },
                    'api': {
                        "filters": [
                            {
                                "field": "attribute",
                                "op": "==",
                                "value": "attribute"
                            }
                        ]
                    },
                    'fields': {
                        'id': {
                            'type': 'GOB.String',
                            'description': 'Unique id of the collection'
                        },
                        'attribute': {
                            'type': 'GOB.String',
                            'description': 'Some attribute'
                        }
                    },
                    'references': {}
                },
                'collection2': {
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
                    },
                    'fields': {
                        'id': {
                            'type': 'GOB.String',
                            'description': 'Unique id of the collection'
                        },
                        'attribute': {
                            'type': 'GOB.String',
                            'description': 'Some attribute'
                        }
                    },
                    'references': {
                        'reference': {
                            'type': 'GOB.Reference',
                            'description': 'Reference to another entity',
                            'ref': 'catalog:collection'
                        },
                        'manyreference': {
                            'type': 'GOB.ManyReference',
                            'description': 'Reference array to another entity',
                            'ref': 'catalog:collection2'
                        }
                    }
                }
            }[collection_name]
        def get_table_name(self, catalog_name, collection_name):
            return f'{catalog_name}_{collection_name}'
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
    monkeypatch.setattr(sqlalchemy, 'Table', MockTable)
    monkeypatch.setattr(sqlalchemy.ext.automap, 'automap_base', mock_automap_base)
    monkeypatch.setattr(sqlalchemy.orm, 'scoped_session', mock_scoped_session)
    monkeypatch.setattr(sqlalchemy_filters, 'apply_filters', lambda q, f: q)

    monkeypatch.setattr(gobcore.model, 'GOBModel', mock_get_gobmodel)
    monkeypatch.setattr(gobcore.model.metadata, 'PUBLIC_META_FIELDS', mock_PUBLIC_META_FIELDS)

    import gobapi.storage
    importlib.reload(gobapi.storage)

    from gobapi.storage import connect
    connect()


def test_entities(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import get_entities
    MockEntities.all_entities = []
    assert(get_entities('catalog', 'collection1', 0, 1) == ([], 0))

    mockEntity = MockEntity('id', 'attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection1', 0, 1) == ([{'attribute': 'attribute', 'id': 'id', '_links': {'self': {'href': '/gob/catalog/collection1/1/'}}}], 1))

    mockEntity = MockEntity('id', 'attribute', 'non_existing_attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection1', 0, 1) == ([{'attribute': 'attribute', 'id': 'id', '_links': {'self': {'href': '/gob/catalog/collection1/1/'}}}], 1))


def test_entities_with_references(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import get_entities

    mockEntity = MockEntity('id', 'attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection2', 0, 1) == ([{
        'attribute': 'attribute',
        'id': 'id',
        '_links': {
            'self': {'href': '/gob/catalog/collection2/1/'}
        },
        '_embedded': {
            'reference': {'bronwaarde': 1, 'id': 1, '_links': {'self': {'href': '/gob/catalog/collection/1/'}}},
            'manyreference': [
                {'bronwaarde': 1, 'id': 1, '_links': {'self': {'href': '/gob/catalog/collection2/1/'}}},
                {'bronwaarde': 2, 'id': 2, '_links': {'self': {'href': '/gob/catalog/collection2/2/'}}}
            ]
        }
    }], 1))


def test_entities_without_reference_id(monkeypatch):
    before_each_storage_test(monkeypatch)
    from gobapi.storage import get_entities

    mockEntity = MockEntity('id', 'attribute')
    mockEntity.reference['id'] = None
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection2', 0, 1) == ([{
        'attribute': 'attribute',
        'id': 'id',
        '_links': {
            'self': {'href': '/gob/catalog/collection2/1/'}
        },
        '_embedded': {
            'reference': {'bronwaarde': 1, 'id': None},
            'manyreference': [
                {'bronwaarde': 1, 'id': 1, '_links': {'self': {'href': '/gob/catalog/collection2/1/'}}},
                {'bronwaarde': 2, 'id': 2, '_links': {'self': {'href': '/gob/catalog/collection2/2/'}}}
            ]
        }
    }], 1))


def test_entities_with_view(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import get_entities
    MockEntities.all_entities = []
    assert(get_entities('catalog', 'collection1', 0, 1, 'enhanced') == ([], 0))

    mockEntity = MockEntity('id', 'attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection1', 0, 1, 'enhanced') == ([{'attribute': 'attribute', 'id': 'id'}], 1))

    mockEntity = MockEntity('id', 'attribute', 'non_existing_attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection1', 0, 1, 'enhanced') == ([{'attribute': 'attribute', 'id': 'id'}], 1))


def test_entity(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import get_entity
    assert(get_entity('catalog', 'collection1', 'id') == None)

    mockEntity = MockEntity('id', 'attribute', 'meta')
    MockEntities.one_entity = mockEntity
    assert(get_entity('catalog', 'catalog', 'collection1', 'id') == {'attribute': 'attribute', 'id': 'id', 'meta': 'meta'})


def test_entity_with_view(monkeypatch):
    before_each_storage_test(monkeypatch)

    MockEntities.one_entity = None
    from gobapi.storage import get_entity
    assert(get_entity('catalog', 'collection1', 'id', 'enhanced') == None)

    mockEntity = MockEntity('id', 'attribute', 'meta')
    MockEntities.one_entity = mockEntity
    assert(get_entity('catalog', 'collection1', 'id', 'enhanced') == {'attribute': 'attribute', 'id': 'id', 'meta': 'meta'})

def test_teardown_session(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import shutdown_session, session

    assert(session._remove == False)
    shutdown_session()
    assert(session._remove == True)
