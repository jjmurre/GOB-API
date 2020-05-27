"""Storage Unit tests

The unit tests for the storage module.
As it is a unit test all external dependencies are mocked

"""
import datetime
import importlib
import sqlalchemy

from unittest import mock, TestCase
from unittest.mock import MagicMock

from gobapi.storage import _get_convert_for_state, filter_deleted, connect, _format_reference, _get_table, \
    _to_gob_value, _add_resolve_attrs_to_columns, _get_convert_for_table, _add_relation_dates_to_manyreference, \
    _flatten_join_result, get_entity_refs_after, dump_entities, get_max_eventid, exec_statement, \
    _create_reference_link, _create_reference_view, _create_reference, _add_relations, _apply_filters, \
    get_id_columns, clear_test_dbs, get_count
from gobapi.auth.auth_query import AuthorizedQuery
from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD


class MockEntity:
    __has_states__ = False
    __tablename__ = 'tablename'

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
        self.src_volgnummer = 1
        self.dst_id = 1
        self.bronwaarde = 'bronwaarde'
        for key in args:
            setattr(self, key, key)

mock_models = {
    'catalog_collection1': MockEntity(),
    'catalog_collection2': MockEntity(),
    'catalog_collection3': MockEntity(),
    'rel_relation_name': MockEntity(),
}


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

    def outerjoin(self, *args):
        return self

    def yield_per(self, *args):
        return self

    def set_catalog_collection(self, *args):
        return self

    def add_columns(self, *args):
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

class SecureType:

    @classmethod
    def from_value_secure(cls, *args, **kwargs):
        return SecureType()

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

        def _extract_references(self, attributes):
            return {field_name: spec for field_name, spec in attributes.items()
                    if spec['type'] in ['GOB.Reference', 'GOB.ManyReference', 'GOB.VeryManyReference']}
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
    monkeypatch.setattr(gobapi.session, 'get_session', mock_scoped_session)
    monkeypatch.setattr(gobapi.config, 'current_api_base_path', lambda: '/gob')

    monkeypatch.setattr(gobcore.model, 'GOBModel', mock_get_gobmodel)
    monkeypatch.setattr(gobcore.model.metadata, 'PUBLIC_META_FIELDS', mock_PUBLIC_META_FIELDS)

    monkeypatch.setattr(gobcore.model.relations, 'get_relation_name', lambda m, a, o, r: 'relation_name')

    import gobapi.storage
    importlib.reload(gobapi.storage)

    monkeypatch.setattr(gobapi.storage, 'models', mock_models)
    monkeypatch.setattr(gobapi.storage, '_apply_filters', lambda e, f, t: e)
    monkeypatch.setattr(gobapi.storage, '_format_reference', lambda ref, cat, col, spec: {'reference': ref})
    monkeypatch.setattr(gobapi.storage, '_add_relations', lambda q, cat, col: q)

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
            'reference': [{'reference': {'bronwaarde': '1', 'id': '1'}}],
            'manyreference': [{'reference': {'id': '1', 'bronwaarde': '1'}},
                              {'reference': {'id': '2', 'bronwaarde': '2'}}]
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
            'reference': [{'reference': {'bronwaarde': '1', 'id': None}}],
            'manyreference': [{'reference': {'id': '1', 'bronwaarde': '1'}},
                              {'reference': {'id': '2', 'bronwaarde': '2'}}]
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
            'reference': [{'reference': {'bronwaarde': '1', 'id': '1'}}],
            'manyreference': [{'reference': {'id': '1', 'bronwaarde': '1'}}, {'reference': {'id': '2', 'bronwaarde': '2'}}]
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
    expect_ref = MagicMock()
    expect_ref._string = '{"reference": {"%s": "1234"}}' % FIELD.REFERENCE_ID
    assert(get_entities('catalog', 'collection1', 0, 1, 'enhanced') ==
           ([{'attribute': 'attribute', 'identificatie': 'identificatie',
              '_embedded': {'is_test': expect_ref}}], None))

    # Reset the table columns
    MockTable.columns = [MockColumn('identificatie'), MockColumn('attribute'), MockColumn('meta')]


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
    assert(get_entity('catalog', 'collection1', 'identificatie', 'enhanced') ==
           {'attribute': 'attribute', 'identificatie': 'identificatie', 'meta': 'meta'})


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


@mock.patch("gobapi.storage.current_api_base_path", lambda: '/gob')
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

        res = _format_reference(reference, 'catalog', 'collection', {})

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

    @mock.patch("gobapi.storage.get_gob_type_from_info")
    def test_format_reference_secure_bronwaarde(self, mock_get_gob_type):
        reference = {
            'bronwaarde': 'bronwaarde_val',
            'volgnummer': 'volgnummer_val',
            'id': 'id_val',
            'otherfield': 'otherfield_val',
        }
        spec = {
            'secure': {
                'bronwaarde': {
                    'type': 'GOB.SecureString',
                    'level': 4,
                }
            }
        }

        mock_get_gob_type.return_value.from_value.return_value.to_value = {'decrypted': 'reference'}
        res = _format_reference(reference, 'catalog', 'collection', spec)

        mock_get_gob_type.assert_called_with(spec)
        mock_get_gob_type.return_value.from_value.assert_called_with(reference, secure=spec['secure'])

        self.assertEqual({
            'decrypted': 'reference',
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

    def test_add_resolve_attrs_to_columns(self):
        class MockColumn:
            def __init__(self, name):
                self.name = name

        model = GOBModel()
        model._data = {
            'the_catalog': {
                'abbreviation': 'cat',
                'name': 'the_catalog',
                'collections': {
                    'the_collection': {
                        'name': 'the_collection',
                        'abbreviation': 'col',
                        'attributes': {
                            'ignored_column': {
                                'column': 'spec',
                                'of': 'ignored_column',
                            },
                            'resolved_column': {
                                'column': 'spec',
                                'of': 'resolved_column',
                            }
                        }
                    },
                    'the_collection2': {
                        'name': 'the_collection2',
                        'abbreviation': 'col2',
                        'attributes': {
                            'other_resolved_column': {
                                'column': 'spec',
                                'of': 'other_resolved_column'
                            }
                        }
                    }
                }
            }
        }
        columns = [
            MockColumn('ignored_column'),
            MockColumn('cat:col:resolved_column'),
            MockColumn('cat:col2:other_resolved_column'),
            MockColumn('cat:col2:nonexisting'),
        ]

        with mock.patch("gobapi.storage.GOBModel", return_value=model):
            _add_resolve_attrs_to_columns(columns)

        self.assertFalse(hasattr(columns[0], 'attribute'))
        self.assertFalse(hasattr(columns[0], 'public_name'))

        self.assertEqual({
            'column': 'spec',
            'of': 'resolved_column',
        }, getattr(columns[1], 'attribute'))

        self.assertEqual('resolved_column', getattr(columns[1], 'public_name'))

        self.assertEqual({
            'column': 'spec',
            'of': 'other_resolved_column',
        }, getattr(columns[2], 'attribute'))

        self.assertEqual('other_resolved_column', getattr(columns[2], 'public_name'))

        self.assertFalse(hasattr(columns[3], 'attribute'))
        self.assertFalse(hasattr(columns[3], 'public_name'))

    @mock.patch("gobapi.storage._add_resolve_attrs_to_columns")
    @mock.patch("gobapi.storage._to_gob_value")
    def test_get_convert_for_table(self, mock_to_gob_value, mock_resolve_attrs):
        """Tests the resolve_column method and result when using _add_resolve_attrs_to_columns.
        Tests if _to_gob_value is called with the right parameters, and if the result matches.

        Does not check references and filter.

        """
        class MockColumn:
            def __init__(self, name, with_attr=False):
                self.name = name
                self.type = 'the type'

                if with_attr:
                    self.attribute = 'the attribute'
                    self.public_name = 'public_' + name

        class MockTable:
            def __init__(self, columns):
                self.columns = columns

        table = MockTable([
            MockColumn('columna', False),
            MockColumn('columnb', True),
        ])

        convert = _get_convert_for_table(table)

        entity = {
            'columna': 'col_a_value',
            'columnb': 'col_b_value',
        }

        mock_to_gob_value.side_effect = lambda e, n, t, resolve_secure: 'gobval(' + e.get(n) + ')'
        mock_resolve_attrs.side_effect = lambda x: x

        result = convert(entity)
        self.assertEqual({
            'columna': 'gobval(col_a_value)',
            'public_columnb': 'gobval(col_b_value)',
        }, result)

        mock_to_gob_value.assert_has_calls([
            mock.call(entity, 'columna', type('the type'), resolve_secure=True),
            mock.call(entity, 'columnb', 'the attribute', resolve_secure=True),
        ])
        mock_resolve_attrs.assert_called_with(table.columns)

    def test_add_relation_dates_to_manyreference(self):
        entity_reference = [
            {'id': 1, 'bronwaarde': 'bronwaarde1'},
            {'id': 2, 'bronwaarde': 'bronwaarde2'},
            {'id': 3, 'bronwaarde': 'bronwaarde3'},
            {'id': 4, 'bronwaarde': 'bronwaarde4'},
        ]

        relation_dates = [
            {'bronwaarde': 'bronwaarde1', 'begin_geldigheid_relatie': datetime.datetime(2000,1,1), 'eind_geldigheid_relatie': datetime.datetime(2010,1,1)},
            {'bronwaarde': 'bronwaarde2', 'begin_geldigheid_relatie': datetime.datetime(2000,1,1), 'eind_geldigheid_relatie': None},
            {'bronwaarde': 'bronwaarde3'},
        ]

        result = _add_relation_dates_to_manyreference(entity_reference, relation_dates)

        expected_result = [
            {'id': 1, 'bronwaarde': 'bronwaarde1', 'begin_geldigheid_relatie': datetime.datetime(2000,1,1), 'eind_geldigheid_relatie': datetime.datetime(2010,1,1)},
            {'id': 2, 'bronwaarde': 'bronwaarde2', 'begin_geldigheid_relatie': datetime.datetime(2000,1,1), 'eind_geldigheid_relatie': None},
            {'id': 3, 'bronwaarde': 'bronwaarde3', 'begin_geldigheid_relatie': None, 'eind_geldigheid_relatie': None},
            {'id': 4, 'bronwaarde': 'bronwaarde4'},
        ]

        self.assertEqual(result, expected_result)

    @mock.patch("gobapi.storage.Base", MockEntity)
    def test_flatten_join_result(self):
        mock_entity = MockEntity()
        mock_entity.some_attr = 'some value'

        result_dict = {
            'catalog_collection1': mock_entity,
            'ref:relation_attr_name1': 'the bronwaardes list1',
            'ref:relation_attr_name2': 'the bronwaardes list2',
        }

        class MockResult():

            def __getitem__(self, key):
                return mock_entity

            def _asdict(self):
                return result_dict

        mock_result = MockResult()

        result = _flatten_join_result(mock_result)

        self.assertEqual('the bronwaardes list1', getattr(result, 'relation_attr_name1'))
        self.assertEqual('the bronwaardes list2', getattr(result, 'relation_attr_name2'))
        self.assertEqual('some value', getattr(result, 'some_attr'))

    @mock.patch("gobapi.storage._Base", mock.MagicMock())
    @mock.patch("gobapi.storage.get_table_and_model")
    @mock.patch("gobapi.storage.get_session")
    @mock.patch("gobapi.storage.functions.concat", lambda *args: "".join(args))
    def test_get_entity_refs_after(self, mock_get_session, mock_get_table_and_model):

        table_no_seqnr = type('MockTableNoSeqnr', (object,), {'_id': '230', '_last_event': 2000})
        table_seqnr = type('MockTableWithSeqnr', (object,), {'_id': '230', '_last_event': 2000, 'volgnummer': '2'})
        mock_get_session.return_value.query.return_value.filter.return_value.all.return_value = [
            ('id1',),
            ('id2',),
            ('id3',),
        ]

        # First check query selection without volgnummer
        mock_get_table_and_model.return_value = table_no_seqnr, 'model'
        get_entity_refs_after('catalog', 'collection', 2920)

        mock_get_session.return_value.query.assert_called_with('230')

        # Test query selection with volgnummer
        mock_get_table_and_model.return_value = table_seqnr, 'model'
        result = get_entity_refs_after('catalog', 'collection', 2920)

        mock_get_session.return_value.query.assert_called_with('230_2')
        self.assertEqual(['id1', 'id2', 'id3'], result)

        # Test filter
        mock_get_session.return_value.query.return_value.filter.assert_called_with(False)

        get_entity_refs_after('catalog', 'collection', 1900)
        mock_get_session.return_value.query.return_value.filter.assert_called_with(True)

        get_entity_refs_after('catalog', 'collection', 2000)
        mock_get_session.return_value.query.return_value.filter.assert_called_with(False)

        mock_get_table_and_model.assert_called_with('catalog', 'collection')

    @mock.patch("gobapi.storage._Base", mock.MagicMock())
    @mock.patch("gobapi.storage.get_table_and_model")
    @mock.patch("gobapi.storage.get_session")
    @mock.patch("gobapi.storage.func.max", lambda x: 'max(' + str(x) + ')')
    def test_get_max_eventid(self, mock_get_session, mock_get_table_and_model):
        table = type('MockTable', (object,), {'_last_event': 82404})

        mock_get_table_and_model.return_value = table, 'model'

        result = get_max_eventid('catalog', 'collection')
        mock_get_table_and_model.assert_called_with('catalog', 'collection')

        # Max of _last_event is queried
        mock_get_session.return_value.query.assert_called_with('max(82404)')

        # Scalar value is returned
        self.assertEqual(mock_get_session.return_value.query.return_value.scalar.return_value, result)

    @mock.patch("gobapi.storage._Base", mock.MagicMock())
    @mock.patch("gobapi.storage.get_table_and_model")
    @mock.patch("gobapi.storage.get_session")
    def test_get_count(self, mock_get_session, mock_get_table_and_model):
        table = type('MockTable', (object,), {'_last_event': 82404})

        mock_get_table_and_model.return_value = table, 'model'

        result = get_count('catalog', 'collection')
        mock_get_table_and_model.assert_called_with('catalog', 'collection')

        mock_get_session.return_value.query.assert_called_with(table)

        # Scalar value is returned
        self.assertEqual(mock_get_session.return_value.query.return_value.count.return_value, result)

    @mock.patch("gobapi.storage._Base", mock.MagicMock())
    @mock.patch("gobapi.storage.get_table_and_model")
    @mock.patch("gobapi.storage.get_session")
    def test_dump_entities_with_filter(self, mock_get_session, mock_get_table_and_model):
        mock_table = type('MockTable', (object,), {'_id': '9204940'})
        mock_model = {}
        mock_filter = mock.MagicMock()

        mock_get_table_and_model.return_value = mock_table, mock_model

        result = dump_entities('catalog_name', 'collection_name', filter=mock_filter)
        mock_get_session.return_value.query.assert_called_with(mock_table)

        # Assert filter is called, and used on entities
        mock_filter.assert_called_with(mock_table)
        mock_get_session.return_value.query.return_value.filter.assert_called_with(mock_filter.return_value)
        entities = mock_get_session.return_value.query.return_value.filter.return_value

        # Assert catalog/collection are set on entities
        entities.set_catalog_collection.assert_called_with('catalog_name', 'collection_name')

        mock_model['catalog'] = 'catalog_name'
        mock_model['collection'] = 'collection_name'
        self.assertEqual((entities.yield_per.return_value, mock_model), result)

    @mock.patch("gobapi.storage._Base", mock.MagicMock())
    @mock.patch("gobapi.storage.get_table_and_model")
    @mock.patch("gobapi.storage.get_session")
    def test_dump_entities_order_by(self, mock_get_session, mock_get_table_and_model):
        mock_table = type('MockTable', (object,), {'_id': '9204940', '_order_by_column': 'orderval'})
        mock_model = {}
        order_by_column = '_order_by_column'

        mock_get_table_and_model.return_value = mock_table, mock_model
        dump_entities('catalog_name', 'collection_name', order_by=order_by_column)

        mock_get_session.return_value.query.assert_called_with(mock_table)

        # Assert order_by is added and used
        mock_get_session.return_value.query.return_value.order_by.assert_called_with('orderval')

    @mock.patch("gobapi.storage.get_gob_type_from_info")
    @mock.patch("gobapi.storage.Authority")
    def test_to_gob_value(self, mock_Authority, mock_get_type):
        # Assume that the spec refers to a secure type
        mock_get_type.return_value = "secure type"

        # Assume that the Authority confirms the type as secure
        mock_Authority.is_secure_type.return_value = True

        # Then get_secure_type and get_secured_value should be called
        mock_Authority.get_secure_type.return_value = "secure GOB type"
        mock_Authority.get_secured_value.return_value = "secure value"

        # spec should be a dict to denote a GOB model spec
        spec = {}
        result = _to_gob_value(None, 'field', spec, resolve_secure=True)
        self.assertEqual(result, "secure value")

        mock_Authority.get_secure_type.assert_called_with("secure type", spec, None)
        mock_Authority.get_secured_value.assert_called_with("secure GOB type")

    @mock.patch("gobapi.storage.session")
    def test_exec_statement(self, mock_session):
        mock_engine = mock.MagicMock()
        mock_session.get_bind.return_value = mock_engine
        result = exec_statement("any statement")
        mock_engine.execute.assert_called_with("any statement")
        self.assertEqual(result, mock_engine.execute.return_value)

    def test_create_reference_link_empty(self):
        self.assertEqual({}, _create_reference_link({}, 'cat', 'col'))

    @mock.patch("gobapi.storage._to_gob_value")
    @mock.patch("gobapi.storage._format_reference")
    def test_create_reference_view_many(self, mock_format_reference, mock_to_gob_value):
        mock_to_gob_value.return_value = type('', (), {
            'to_db': ['a', 'b', 'c']
        })
        mock_format_reference.side_effect = lambda ref, cat, col, spec: ref

        self.assertEqual(['a', 'b', 'c'], _create_reference_view('entity', 'field', {'ref': 'cat:col', 'type': 'GOB.ManyReference'}))
        mock_to_gob_value.assert_called_with('entity', 'field', {'ref': 'cat:col', 'type': 'GOB.ManyReference'})
        mock_format_reference.assert_called_with('c', 'cat', 'col', {})

    def test_create_reference_nonref(self):
        self.assertEqual({}, _create_reference({}, 'field', {'ref': None}))

    @mock.patch("gobapi.storage.GOBModel")
    @mock.patch("gobapi.storage.get_table_and_model")
    @mock.patch("gobapi.storage.func.json_agg")
    @mock.patch("gobapi.storage.func.json_build_object")
    @mock.patch("gobapi.storage.session")
    @mock.patch("gobapi.storage.and_")
    @mock.patch("gobapi.storage.get_relation_name", lambda m, cat, col, ref: None if ref is None else f'{cat}_{col}_{ref}')
    def test_add_relations(self, mock_and, mock_session, mock_json_build_object, mock_json_agg, mock_get_table_and_model, mock_model):
        mock_src_table = type('MockSrcTable', (), {
            '_id': 'the src id',
            'volgnummer': 'the src volgnummer',
        })
        mock_rel_table = type('MockRelTable', (), {
            'src_id': 'rel table src id',
            'src_volgnummer': 'rel table src volgnummer',
            'bronwaarde': 'rel table bronwaarde',
            'dst_id': 'rel table dst id',
            '_date_deleted': type('MockIs', (), {
                'is_': lambda x: 'is_date_deleted_' + str(x)
            })
        })
        mock_model.return_value.get_collection.return_value = {
            'has_states': False,
            'references': ['reference1', None],  # None triggers None relation name in mocked function
        }

        mock_get_table_and_model.side_effect = lambda cat, col: {
            'rel': {
                'cat_col_reference1': (mock_rel_table, ''),
            },
            'cat': {
                'col': (mock_src_table, '')
            }
        }[cat][col]

        mock_query = MagicMock()

        mocked_subquery = type('MockSubQuery', (), {
            'c': type('MockC', (), {
                'src_id': 'subquery src_id',
                'src_volgnummer': 'subquery src_volgnummer',
                'source_values': MagicMock(),
            })
        })()
        mock_session.query.return_value.filter.return_value.group_by.return_value.subquery.return_value = mocked_subquery

        result = _add_relations(mock_query, 'cat', 'col')
        self.assertEqual(mock_query.join.return_value.add_columns.return_value, result)

        # Check build of subquery: json_agg of json_build_object
        mock_session.query.assert_called_with(
            'rel table src id',
            mock_json_agg.return_value.label.return_value,
        )
        mock_json_agg.assert_called_with(mock_json_build_object.return_value)
        mock_json_build_object.assert_called_with(
            'bronwaarde', 'rel table bronwaarde',
            'id', 'rel table dst id',
        )

        # Filtered by _date_deleted
        mock_session.query.return_value.filter.assert_called_with('is_date_deleted_None')

        # Grouped by rel table src id
        mock_session.query.return_value.filter.return_value.group_by.assert_called_with('rel table src id')

        # Check subquery is LEFT OUTER joined
        mock_query.join.assert_called_with(
            mocked_subquery,
            mock_and.return_value,
            isouter=True
        )

        # Check the correct label is assigned for further processing in calling function
        mocked_subquery.c.source_values.label.assert_called_with('ref:reference1')

    def test_apply_filters(self):
        query = MagicMock()
        model = type('MockModel', (), {
            'attribute_to_filter': 'matching_value',
        })
        filters = [{'op': '==', 'field': 'attribute_to_filter', 'value': 'nonmatching_value'}]

        result = _apply_filters(query, filters, model)
        query.filter.assert_called_with(False)
        self.assertEqual(query.filter.return_value, result)

        filters = [{'op': '==', 'field': 'attribute_to_filter', 'value': 'matching_value'}]
        result = _apply_filters(query, filters, model)
        query.filter.assert_called_with(True)
        self.assertEqual(query.filter.return_value, result)

        filters = [{}]
        with self.assertRaises(NotImplementedError):
            _apply_filters(query, filters, model)

    @mock.patch("gobapi.storage.get_table_and_model")
    def test_get_id_columns(self, mock_table_and_model):
        mock_table_and_model.return_value = MagicMock(), None
        result = get_id_columns("rel", "any collection")
        self.assertEqual(len(result), 4)
        result = get_id_columns("any catalogue", "any collection")
        self.assertEqual(len(result), 2)

    @mock.patch("gobapi.storage.get_relation_name", lambda model, cat, col, ref: f"{col}_{ref}")
    @mock.patch("gobapi.storage.GOBModel")
    @mock.patch("gobapi.storage.exec_statement")
    def test_clear_test_dbs(self, mock_exec, mock_gob_model):
        mock_model = MagicMock()
        mock_gob_model.return_value = mock_model
        mock_model.get_collections.return_value = ["col1", "col2"]
        mock_model.get_collection.return_value = {
            'references': {
                'ref1': None,
            },
            'very_many_references': {
                'ref2': None
            }
        }
        mock_model.get_table_name.side_effect = lambda cat, col: f"{cat}_{col}"
        clear_test_dbs()
        mock_exec.assert_called_with("""
-- Truncate test tables
TRUNCATE TABLE test_catalogue_col1 CASCADE;
TRUNCATE TABLE rel_col1_ref1       CASCADE;
TRUNCATE TABLE rel_col1_ref2       CASCADE;
TRUNCATE TABLE test_catalogue_col2 CASCADE;
TRUNCATE TABLE rel_col2_ref1       CASCADE;
TRUNCATE TABLE rel_col2_ref2       CASCADE;

-- Delete test entity events
DELETE
FROM events
WHERE catalogue = 'test_catalogue'
  AND entity IN ('col1',
                 'col2');

-- Delete test relation events
DELETE FROM events
WHERE catalogue = 'rel'
  AND entity IN ('col1_ref1',
                 'col1_ref2',
                 'col2_ref1',
                 'col2_ref2');

-- Commit all changes
COMMIT;
""")
