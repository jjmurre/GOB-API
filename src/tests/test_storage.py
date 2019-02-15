"""Storage Unit tests

The unit tests for the storage module.
As it is a unit test all external dependencies are mocked

"""
import datetime
import importlib
import sqlalchemy
import sqlalchemy_filters

from unittest import mock

from gobapi.storage import _get_convert_for_state

class MockEntity:
    def __init__(self, *args):
        self._id = '1'
        self.reference = {
            'identificatie': '1',
            'bronwaarde': '1'
        }
        self.manyreference = [
            {
                'identificatie': '1',
                'bronwaarde': '1'
            },
            {
                'identificatie': '2',
                'bronwaarde': '2'
            }
        ]
        self.datum_begin_geldigheid = datetime.date.today() - datetime.timedelta(days=365)
        self.begin_geldigheid = datetime.date.today()
        self.eind_geldigheid = datetime.date.today()
        self.volgnummer = 1
        self.max_volgnummer = 1
        for key in args:
            setattr(self, key, key)


class MockClasses:
    def __init__(self):
        self.catalog_collection1 = MockEntity()
        self.catalog_collection2 = MockEntity()


class MockBase:
    def prepare(self, engine, reflect):
        return None

    classes = MockClasses()


class MockEntities:
    all_entities = []
    one_entity = {}

    def __init__(self):
        self.c = MockEntity()

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

    def group_by(self, *args):
        return self

    def subquery(self, *args):
        return self

    def join(self, *args):
        return self


class MockColumn:

    def __init__(self, name):
        self.name = name
    type = sqlalchemy.types.VARCHAR()

class MockTable():

    def __init__(self, *args, **kwargs):
        pass

    columns = [MockColumn('identificatie'), MockColumn('attribute'), MockColumn('meta')]


class MockSession:
    def __init__(self, engine):
        self._remove = False
        self.c = MockEntity()
        pass

    def query(self, table, *args):
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
                    'entity_id': 'identificatie',
                    'attributes': {
                        'identificatie': {
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
                        'identificatie': {
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
                    'entity_id': 'identificatie',
                    'attributes': {
                        'identificatie': {
                            'type': 'GOB.String',
                            'description': 'Unique id of the collection'
                        },
                        'attribute': {
                            'type': 'GOB.String',
                            'description': 'Some attribute'
                        }
                    },
                    'fields': {
                        'identificatie': {
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

        def get_reference_by_abbreviations(self, catalog_abbreviation, collection_abbreviation):
            return 'catalog:collection'
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

    mockEntity = MockEntity('identificatie', 'attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection1', 0, 1) == ([{'attribute': 'attribute', 'identificatie': 'identificatie', '_links': {'self': {'href': '/gob/catalog/collection1/1/'}}}], 1))

    mockEntity = MockEntity('identificatie', 'attribute', 'non_existing_attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection1', 0, 1) == ([{'attribute': 'attribute', 'identificatie': 'identificatie', '_links': {'self': {'href': '/gob/catalog/collection1/1/'}}}], 1))


def test_entities_with_references(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import get_entities

    mockEntity = MockEntity('identificatie', 'attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection2', 0, 1) == ([{
        'attribute': 'attribute',
        'identificatie': 'identificatie',
        '_links': {
            'self': {'href': '/gob/catalog/collection2/1/'}
        },
        '_embedded': {
            'reference': {'bronwaarde': '1', 'identificatie': '1', '_links': {'self': {'href': '/gob/catalog/collection/1/'}}},
            'manyreference': [
                {'bronwaarde': '1', 'identificatie': '1', '_links': {'self': {'href': '/gob/catalog/collection2/1/'}}},
                {'bronwaarde': '2', 'identificatie': '2', '_links': {'self': {'href': '/gob/catalog/collection2/2/'}}}
            ]
        }
    }], 1))


def test_entities_without_reference_id(monkeypatch):
    before_each_storage_test(monkeypatch)
    from gobapi.storage import get_entities

    mockEntity = MockEntity('identificatie', 'attribute')
    mockEntity.reference['identificatie'] = None
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection2', 0, 1) == ([{
        'attribute': 'attribute',
        'identificatie': 'identificatie',
        '_links': {
            'self': {'href': '/gob/catalog/collection2/1/'}
        },
        '_embedded': {
            'reference': {'bronwaarde': '1', 'identificatie': None},
            'manyreference': [
                {'bronwaarde': '1', 'identificatie': '1', '_links': {'self': {'href': '/gob/catalog/collection2/1/'}}},
                {'bronwaarde': '2', 'identificatie': '2', '_links': {'self': {'href': '/gob/catalog/collection2/2/'}}}
            ]
        }
    }], 1))


def test_entities_with_view(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import get_entities
    MockEntities.all_entities = []
    assert(get_entities('catalog', 'collection1', 0, 1, 'enhanced') == ([], 0))

    mockEntity = MockEntity('identificatie', 'attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection1', 0, 1, 'enhanced') == ([{'attribute': 'attribute', 'identificatie': 'identificatie'}], 1))

    mockEntity = MockEntity('identificatie', 'attribute', 'non_existing_attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection1', 0, 1, 'enhanced') == ([{'attribute': 'attribute', 'identificatie': 'identificatie'}], 1))

    # Add a reference to the table columns
    MockTable.columns = [MockColumn('identificatie'), MockColumn('attribute'), MockColumn('_ref_is_test_tse_tst')]
    mockEntity = MockEntity('identificatie', 'attribute', '_ref_is_test_tse_tst')
    mockEntity._ref_is_test_tse_tst = {'identificatie': '1234'}
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection1', 0, 1, 'enhanced') == ([{'attribute': 'attribute', 'identificatie': 'identificatie', '_embedded': {'is_test': {'identificatie': '1234', '_links': {'self': {'href': '/gob/catalog/collection/1234/'}}}}}], 1))

    # Reset the table columns
    MockTable.columns = [MockColumn('identificatie'), MockColumn('attribute'), MockColumn('meta')]


@mock.patch("gobapi.storage.cast", mock.MagicMock())
def test_collection_states(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import get_collection_states
    MockEntities.all_entities = []
    assert(get_collection_states('catalog', 'collection1') == {})

    mockEntity = MockEntity('identificatie', 'attribute')
    MockEntities.all_entities = [
        mockEntity
    ]

    assert(get_collection_states('catalog', 'collection1') == {'1': [mockEntity]})


def test_entity(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import get_entity
    assert(get_entity('catalog', 'collection1', 'identificatie') == None)

    mockEntity = MockEntity('identificatie', 'attribute', 'meta')
    MockEntities.one_entity = mockEntity
    assert(get_entity('catalog', 'catalog', 'collection1', 'identificatie') == {'attribute': 'attribute', 'identificatie': 'identificatie', 'meta': 'meta'})


def test_entity_with_view(monkeypatch):
    before_each_storage_test(monkeypatch)

    MockEntities.one_entity = None
    from gobapi.storage import get_entity
    assert(get_entity('catalog', 'collection1', 'identificatie', 'enhanced') == None)

    mockEntity = MockEntity('identificatie', 'attribute', 'meta')
    MockEntities.one_entity = mockEntity
    assert(get_entity('catalog', 'collection1', 'identificatie', 'enhanced') == {'attribute': 'attribute', 'identificatie': 'identificatie', 'meta': 'meta'})

def test_teardown_session(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import shutdown_session, session

    assert(session._remove == False)
    shutdown_session()
    assert(session._remove == True)


def test_get_convert_for_state(monkeypatch):
    before_each_storage_test(monkeypatch)

    MockGOBModel = mock_get_gobmodel()
    model = MockGOBModel.get_collection('catalog', 'collection1')
    convert = _get_convert_for_state(model)
    mockEntity = MockEntity('identificatie', 'attribute')
    result = convert(mockEntity)

    assert(result == {'identificatie': 'identificatie', 'attribute': 'attribute'})
