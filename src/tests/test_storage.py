"""Storage Unit tests

The unit tests for the storage module.
As it is a unit test all external dependencies are mocked

"""
import datetime
import importlib
import sqlalchemy
import sqlalchemy_filters

from unittest import mock, TestCase

from gobapi.storage import _get_convert_for_state, filter_deleted, connect, _format_reference, _get_table
from gobapi.auth.auth_query import AuthorizedQuery
from gobcore.model.metadata import FIELD

class MockEntity:
    def __init__(self, *args):
        self._id = '1'
        self.identificatie = '1'
        self.reference = {
            FIELD.REFERENCE_ID: '1',
            'bronwaarde': '1'
        }
        self.manyreference = [
            {
                FIELD.REFERENCE_ID: '1',
                'bronwaarde': '1'
            },
            {
                FIELD.REFERENCE_ID: '2',
                'bronwaarde': '2'
            }
        ]
        self._private_reference = {
            FIELD.REFERENCE_ID: '1',
            'bronwaarde': '1'
        }
        self.datum_begin_geldigheid = datetime.date.today() - datetime.timedelta(days=365)
        self.begin_geldigheid = datetime.date.today()
        self.eind_geldigheid = datetime.date.today()
        self._expiration_date = datetime.datetime.now()
        self._date_deleted = None
        self.volgnummer = 1
        self.max_seqnr = 1
        self.src_id = 1
        self.dst_id = 1
        for key in args:
            setattr(self, key, key)


class MockClasses:
    def __init__(self):
        self.catalog_collection1 = MockEntity()
        self.catalog_collection2 = MockEntity()
        self.catalog_collection3 = MockEntity()
        self.rel_relation_name = MockEntity()


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

    def filter(self, _, **kwargs):
        return self

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

    def yield_per(self, *args):
        return self

    def set_catalog_collection(self, *args):
        return self


class MockColumn:

    def __init__(self, name):
        self.name = name
    type = sqlalchemy.types.VARCHAR()

class MockTable():

    _date_deleted = None

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


def mock_scoped_session(func=None):
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
        def get_catalog(self, catalog_name):
            return catalog_name

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
                        },
                        '_private_attribute': {
                            'type': 'GOB.String',
                            'description': 'Some private attribute'
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
                        },
                        '_private_attribute': {
                            'type': 'GOB.String',
                            'description': 'Some private attribute'
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
                        },
                        'reference': {
                            'type': 'GOB.Reference',
                            'description': 'Reference to another entity',
                            'ref': 'catalog:collection'
                        },
                        'manyreference': {
                            'type': 'GOB.ManyReference',
                            'description': 'Reference array to another entity',
                            'ref': 'catalog:collection2'
                        },
                        '_private_reference': {
                            'type': 'GOB.Reference',
                            'description': 'Private reference to another entity',
                            'ref': 'catalog:collection'
                        },
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
                        },
                        '_private_reference': {
                            'type': 'GOB.Reference',
                            'description': 'Private reference to another entity',
                            'ref': 'catalog:collection'
                        },
                    }
                },
                'collection3': {
                    'entity_id': 'identificatie',
                    'attributes': {
                        'identificatie': {
                            'type': 'GOB.String',
                            'description': 'Unique id of the collection'
                        },
                        'attribute': {
                            'type': 'GOB.String',
                            'description': 'Some attribute'
                        },
                        'verymanyreference': {
                            'type': 'GOB.VeryManyReference',
                            'description': 'Reference array to another entity',
                            'ref': 'catalog:collection2'
                        },
                    },
                    'fields': {
                        'identificatie': {
                            'type': 'GOB.String',
                            'description': 'Unique id of the collection'
                        },
                        'attribute': {
                            'type': 'GOB.String',
                            'description': 'Some attribute'
                        },
                        'verymanyreference': {
                            'type': 'GOB.VeryManyReference',
                            'description': 'Reference array to another entity',
                            'ref': 'catalog:collection2'
                        },
                    },
                    'references': {
                        'verymanyreference': {
                            'type': 'GOB.ManyReference',
                            'description': 'Reference array to another entity',
                            'ref': 'catalog:collection2'
                        },
                    },
                    'very_many_references': {
                        'verymanyreference': {
                            'type': 'GOB.ManyReference',
                            'description': 'Reference array to another entity',
                            'ref': 'catalog:collection2'
                        },
                    }
                },
                'relation_name': {
                    'entity_id': 'identificatie',
                    'attributes': {
                        'identificatie': {
                            'type': 'GOB.String',
                            'description': 'Unique id of the collection'
                        },
                    },
                    'fields': {
                        'identificatie': {
                            'type': 'GOB.String',
                            'description': 'Unique id of the collection'
                        },
                    },
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
    monkeypatch.setattr(gobapi.session, 'get_session', mock_scoped_session)

    monkeypatch.setattr(gobcore.model, 'GOBModel', mock_get_gobmodel)
    monkeypatch.setattr(gobcore.model.metadata, 'PUBLIC_META_FIELDS', mock_PUBLIC_META_FIELDS)

    monkeypatch.setattr(gobcore.model.relations, 'get_relation_name', lambda m, a, o, r: 'relation_name')

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
    # The private reference should't be visible on the entities list
    assert(get_entities('catalog', 'collection2', 0, 1) == ([{
        'attribute': 'attribute',
        'identificatie': 'identificatie',
        '_links': {
            'self': {'href': '/gob/catalog/collection2/1/'}
        },
        '_embedded': {
            'reference': {'bronwaarde': '1', FIELD.REFERENCE_ID: '1', '_links': {'self': {'href': '/gob/catalog/collection/1/'}}},
            'manyreference': [
                {'bronwaarde': '1', FIELD.REFERENCE_ID: '1', '_links': {'self': {'href': '/gob/catalog/collection2/1/'}}},
                {'bronwaarde': '2', FIELD.REFERENCE_ID: '2', '_links': {'self': {'href': '/gob/catalog/collection2/2/'}}}
            ]
        }
    }], 1))


def test_entities_without_reference_id(monkeypatch):
    before_each_storage_test(monkeypatch)
    from gobapi.storage import get_entities

    mockEntity = MockEntity('identificatie', 'attribute')
    mockEntity.reference[FIELD.REFERENCE_ID] = None
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
            'reference': {FIELD.REFERENCE_ID: None, 'bronwaarde': '1'},
            'manyreference': [
                {FIELD.REFERENCE_ID: '1', 'bronwaarde': '1' , '_links': {'self': {'href': '/gob/catalog/collection2/1/'}}},
                {FIELD.REFERENCE_ID: '2', 'bronwaarde': '2', '_links': {'self': {'href': '/gob/catalog/collection2/2/'}}}
            ]
        }
    }], 1))


def test_entities_with_verymanyreferences(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import get_entities

    mockEntity = MockEntity('identificatie', 'attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    # It should have a reference link to the verymanyreference
    assert(get_entities('catalog', 'collection3', 0, 1) == ([{
        'attribute': 'attribute',
        'identificatie': 'identificatie',
        '_links': {
            'self': {'href': '/gob/catalog/collection3/1/'},
            'verymanyreference': {'href': '/gob/catalog/collection3/1/verymanyreference/'}
        }
    }], 1))


def test_reference_entities(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import get_entities

    mockEntity = MockEntity('identificatie', 'attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    # A list of entities of catalog:collection2 should be returned
    assert(get_entities('catalog', 'collection3', 0, 1, None, 'verymanyreference', '1') == ([{
        'attribute': 'attribute',
        'identificatie': 'identificatie',
        '_links': {
            'self': {'href': '/gob/catalog/collection2/1/'}
        },
        '_embedded': {
            'reference': {'bronwaarde': '1', FIELD.REFERENCE_ID: '1', '_links': {'self': {'href': '/gob/catalog/collection/1/'}}},
            'manyreference': [
                {'bronwaarde': '1', FIELD.REFERENCE_ID: '1', '_links': {'self': {'href': '/gob/catalog/collection2/1/'}}},
                {'bronwaarde': '2', FIELD.REFERENCE_ID: '2', '_links': {'self': {'href': '/gob/catalog/collection2/2/'}}}
            ]
        }
    }], 1))


def test_entities_with_view(monkeypatch):
    before_each_storage_test(monkeypatch)

    from gobapi.storage import get_entities
    MockEntities.all_entities = []
    # Views return total_count None to prevent slow count on large tables
    assert(get_entities('catalog', 'collection1', 0, 1, 'enhanced') == ([], None))

    mockEntity = MockEntity('identificatie', 'attribute', '_private_attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection1', 0, 1, 'enhanced') == ([{'attribute': 'attribute', 'identificatie': 'identificatie'}], None))

    mockEntity = MockEntity('identificatie', 'attribute', 'non_existing_attribute')
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection1', 0, 1, 'enhanced') == ([{'attribute': 'attribute', 'identificatie': 'identificatie'}], None))

    # Add a reference to the table columns
    MockTable.columns = [MockColumn('identificatie'), MockColumn('attribute'), MockColumn('_ref_is_test_tse_tst')]
    mockEntity = MockEntity('identificatie', 'attribute', '_ref_is_test_tse_tst')
    mockEntity._ref_is_test_tse_tst = {FIELD.REFERENCE_ID: '1234'}
    MockEntities.all_entities = [
        mockEntity
    ]
    assert(get_entities('catalog', 'collection1', 0, 1, 'enhanced') ==
           ([{'attribute': 'attribute', 'identificatie': 'identificatie',
              '_embedded': {'is_test': {FIELD.REFERENCE_ID: '1234',
                                        '_links': {'self': {'href': '/gob/catalog/collection/1234/'}}}}}], None))

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

    mockEntity = MockEntity('identificatie', 'attribute', '_private_attribute', 'meta')
    MockEntities.one_entity = mockEntity

    # Expect the private attribute to be visible
    expected = {
        'identificatie': 'identificatie',
        'attribute': 'attribute',
        'meta': 'meta',
        '_private_attribute': '_private_attribute',
        '_links': {'self': {'href': '/gob/catalog/collection1/1/'}}
    }

    assert(get_entity('catalog', 'collection1', 'identificatie') == expected)


def test_entity_with_view(monkeypatch):
    before_each_storage_test(monkeypatch)

    MockEntities.one_entity = None
    from gobapi.storage import get_entity
    assert(get_entity('catalog', 'collection1', 'identificatie', 'enhanced') == None)

    mockEntity = MockEntity('identificatie', 'attribute', 'meta')
    MockEntities.one_entity = mockEntity
    assert(get_entity('catalog', 'collection1', 'identificatie', 'enhanced') == {'attribute': 'attribute', 'identificatie': 'identificatie', 'meta': 'meta'})


def test_get_convert_for_state(monkeypatch):
    before_each_storage_test(monkeypatch)

    MockGOBModel = mock_get_gobmodel()
    model = MockGOBModel.get_collection('catalog', 'collection1')
    convert = _get_convert_for_state(model)
    mockEntity = MockEntity('identificatie', 'attribute')
    result = convert(mockEntity)

    assert(result == {'identificatie': 'identificatie', 'attribute': 'attribute'})

def test_filter_deleted(monkeypatch):
    # Assert query is returned unchanged, when date_deleted is absent
    table = {}
    assert('query' == filter_deleted('query', table))


class TestStorage(TestCase):

    @mock.patch("gobapi.storage.create_engine")
    @mock.patch("gobapi.storage.URL", mock.MagicMock())
    @mock.patch("gobapi.storage.scoped_session", mock.MagicMock())
    @mock.patch("gobapi.storage.sessionmaker")
    @mock.patch("gobapi.storage.automap_base", mock.MagicMock())
    @mock.patch("gobapi.storage.MetaData", mock.MagicMock())
    @mock.patch("gobapi.storage.set_session", mock.MagicMock())
    def test_connect_autocommit(self, mock_sessionmaker, mock_create_engine):
        connect()

        # Autocommit should always be set to True, to avoid problems with auto-creation of transactions that block
        # other processes.
        mock_sessionmaker.assert_called_with(autocommit=True, autoflush=False, bind=mock_create_engine.return_value, query_cls=AuthorizedQuery)

    def test_format_reference(self):
        reference = {
            'bronwaarde': 'bronwaarde_val',
            'volgnummer': 'volgnummer_val',
            'id': 'id_val',
            'otherfield': 'otherfield_val',
        }

        res = _format_reference(reference, 'catalog', 'collection')

        self.assertEqual({
            'bronwaarde': 'bronwaarde_val',
            'volgnummer': 'volgnummer_val',
            'id': 'id_val',
            'otherfield': 'otherfield_val',
            '_links': {
                'self': {
                    'href': '/gob/catalog/collection/id_val/'
                }
            }
        }, res)

    def test_get_table(self):
        names = ["any"]
        self.assertEqual(_get_table(names, names[0]), names[0])

        names = ["any", "any1", "any123", "any", "any"]
        for name in names:
            self.assertEqual(_get_table(names, name), name)

        names = ["any"]
        self.assertEqual(_get_table(names, "any123"), "any")
