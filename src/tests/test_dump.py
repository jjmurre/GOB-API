from unittest import TestCase
from unittest.mock import MagicMock, patch, ANY

from flask import Response

from gobapi import dump, api, storage
from gobapi.dump.csv_stream import CSVStream
from gobapi.dump.to_db import dump_to_db, _dump_to_db


class MockEntity:

    def __init__(self):
        self.a = "a"
        self.b = 5

    @classmethod
    def specs(self):
        return {
            'a': {
                'type': 'GOB.String',
                'entity_id': 'a'
            },
            'b': {
                'type': 'GOB.Integer'
            }
        }


class MockSession:

    def __init__(self):
        pass

    def query(self, any_query):
        return self

    def yield_per(self, n):
        return "any table"


class MockModel:
    cat_col_has_states = None
    collection = {'cat': 'col'}

    def has_states(self, cat, col):
        return self.cat_col_has_states

    def get_collection(self, cat, col):
        return self.collection


from gobapi.dump.config import get_unique_reference, add_unique_reference
from gobapi.dump.config import get_field_specifications, get_field_value, joined_names


class TestConfig(TestCase):

    @patch('gobapi.dump.config.GOBModel', MockModel)
    def test_reference_fields(self):
        MockModel.cat_col_has_states = True
        self.assertEqual(dump.config.get_reference_fields({'ref': 'a:b'}), dump.config.REFERENCE_FIELDS)
        self.assertTrue(dump.config.FIELD.SEQNR in dump.config.get_reference_fields({'ref': 'a:b'}))

        MockModel.cat_col_has_states = False
        self.assertTrue(dump.config.FIELD.SEQNR not in dump.config.get_reference_fields({'ref': 'a:b'}))

        MockModel.collection = None
        self.assertEqual([dump.config.FIELD.SOURCE_VALUE], dump.config.get_reference_fields({'ref': 'a:b'}))

    def test_joined_names(self):
        result = joined_names()
        self.assertEqual(result, "")

        result = joined_names("a")
        self.assertEqual(result, "a")

        result = joined_names("a", "b", "c")
        self.assertEqual(result, "a_b_c")

        result = joined_names(1, "2", 3.5)
        self.assertEqual(result, "1_2_3.5")

    def test_add_unique_reference(self):
        result = add_unique_reference({})
        self.assertEqual(result, {'ref': None})

        result = add_unique_reference({'any': 'value'})
        self.assertEqual(result, {'any': 'value', 'ref': None})

        result = add_unique_reference({'id': 'any id'})
        self.assertEqual(result, {'id': 'any id', 'ref': 'any id'})

        result = add_unique_reference({'id': 'any id', 'volgnummer': 'any volgnummer'})
        self.assertEqual(result, {'id': 'any id', 'ref': 'any id_any volgnummer', 'volgnummer': 'any volgnummer'})

    @patch('gobapi.dump.config.UNIQUE_ID', 'a')
    @patch('gobapi.dump.config.FIELD.SEQNR', 'b')
    def test_get_unique_reference_with_volgnummer(self):
        result = get_unique_reference(MockEntity(), 'a', MockEntity.specs())
        self.assertEqual(result, 'a_5')

    @patch('gobapi.dump.config.UNIQUE_ID', 'a')
    @patch('gobapi.dump.config.FIELD.SEQNR', 'c')
    def test_get_unique_reference_without_volgnummer(self):
        result = get_unique_reference(MockEntity(), 'a', MockEntity.specs())
        self.assertEqual(result, 'a')

    @patch('gobapi.dump.config.UNIQUE_ID', 'a')
    @patch('gobapi.dump.config.REL_UNIQUE_IDS', ['src_a'])
    @patch('gobapi.dump.config.FIELD.SEQNR', 'c')
    @patch('gobapi.dump.config.get_field_value', lambda e, f, s: f)
    def test_get_unique_reference_relation(self):
        specs = {
            'src_id': {
                'type': 'GOB.String',
                'entity_id': 'a'
            },
            'src_c': {
                'type': 'GOB.Integer'
            }
        }
        result = get_unique_reference(MockEntity(), 'src_a', specs)
        self.assertEqual(result, 'src_id_src_c')

    @patch('gobapi.dump.config.is_relation', lambda m: True)
    def test_get_field_specifications_relations(self):
        model = {
            'catalog': 'any catalog',
            'entity_id': 'any entity id name',
            'all_fields': {
                'skip_on_type': {'type': 'GOB.VeryManyReference'},
                '_hash': "skip on field name",
                'src_id': {'type': 'whatever type'}
            }
        }
        result = get_field_specifications(model)
        expect = {
            'src_id': {'type': 'whatever type'},
            'src_ref': {
                'description': 'src identificatie_volgnummer or identificatie',
                'type': 'GOB.String'
            },
            'dst_ref': {
                'description': 'dst identificatie_volgnummer or identificatie',
                'type': 'GOB.String'
            }
        }
        self.assertEqual(result, expect)

    def test_get_field_specifications(self):
        model = {
            'catalog': 'any catalog',
            'entity_id': 'any entity id name',
            'all_fields': {
                'skip_on_type': {'type': 'GOB.VeryManyReference'},
                '_hash': "skip on field name",
                'any_attr': {'type': 'whatever type'}
            }
        }
        result = get_field_specifications(model)
        expect = {
            'any_attr': {'type': 'whatever type'},
            'ref': {
                'description': 'identificatie_volgnummer or identificatie',
                'entity_id': 'any entity id name',
                'type': 'GOB.String'
            }
        }
        self.assertEqual(result, expect)

    def test_get_field_value(self):
        entity = MockEntity()
        result = get_field_value(entity, 'a', MockEntity.specs()['a'])
        self.assertEqual(result, 'a')

        result = get_field_value(entity, 'c', MockEntity.specs()['a'])
        self.assertEqual(result, None)


class TestSQL(TestCase):

    def test_create_schema(self):
        result = dump.sql._create_schema('any_name')
        self.assertTrue("CREATE SCHEMA IF NOT EXISTS \"any_name\"" in result)

    @patch('gobapi.dump.sql.GOBModel', MagicMock())
    @patch('gobapi.dump.sql.get_reference_fields', lambda x: dump.config.REFERENCE_FIELDS)
    @patch('gobapi.dump.sql.get_field_order')
    @patch('gobapi.dump.sql.get_field_specifications')
    def test_create_table(self, mock_specs, mock_order):
        catalogue = {
            'description': "Any 'catalogue' description"
        }
        result = dump.sql._create_table('any_schema', catalogue, 'any_table', {})
        self.assertTrue("CREATE TABLE IF NOT EXISTS \"any_schema\".\"any_table\"" in result)
        self.assertTrue("COMMENT ON TABLE  \"any_schema\".\"any_table\"" in result)

        mock_specs.return_value = {
            'a': {
                'type': 'GOB.String',
                'description': "Any 'a' description"
            }
        }
        mock_order.return_value = ['a']
        result = dump.sql._create_table(catalogue, 'any_schema', 'any_table', {})
        self.assertTrue("\"a\" character varying" in result)
        self.assertTrue("Any ''a'' description" in result)

        mock_specs.return_value = {
            'a': {
                'type': 'GOB.Reference',
                'description': 'Any description'
            }
        }
        result = dump.sql._create_table(catalogue, 'any_schema', 'any_table', {})
        for s in ['ref', 'id', 'volgnummer', 'bronwaarde']:
            self.assertTrue(f"\"a_{s}\"" in result)
            self.assertTrue(f"character varying" in result)

        mock_specs.return_value = {
            'a': {
                'type': 'GOB.JSON',
                'attributes': {'a': {'type': 'GOB.String'}, 'b': {'type': 'GOB.String'}},
                'description': 'Any description'
            }
        }
        result = dump.sql._create_table(catalogue, 'any_schema', 'any_table', {})
        for s in ['a', 'b']:
            self.assertTrue(f"\"a_{s}\"" in result)
            self.assertTrue(f"character varying" in result)

    def test_import_csv(self):
        result = dump.sql._import_csv('any_schema', 'any_collection', 'any_collection.csv')
        self.assertTrue("\COPY \"any_schema\".\"any_collection\" FROM 'any_collection.csv'" in result)

    @patch('gobapi.dump.sql.GOBModel', MagicMock())
    @patch('gobapi.dump.sql.get_field_order', MagicMock())
    @patch('gobapi.dump.sql.get_field_specifications')
    def test_sql_entities(self, mock_specs):
        mock_specs.return_value = {
            'entity_id': 'any_entity_id',
            'all_fields': {}
        }
        result = dump.sql.sql_entities('any catalog', 'any collection', {})
        self.assertTrue("CREATE SCHEMA IF NOT EXISTS" in result)
        self.assertTrue("CREATE TABLE IF NOT EXISTS" in result)
        self.assertTrue("\COPY" in result)


class TestCSV(TestCase):

    def test_csv_line(self):
        result = dump.csv._csv_line([])
        self.assertEqual(result, "\n")

        result = dump.csv._csv_line(["a", "b"])
        self.assertEqual(result, "a;b\n")

    def test_csv_value(self):
        result = dump.csv._csv_value(None)
        self.assertEqual(result, "")

        result = dump.csv._csv_value(0)
        self.assertEqual(result, "0")

        result = dump.csv._csv_value(0.5)
        self.assertEqual(result, "0.5")

        result = dump.csv._csv_value("s")
        self.assertEqual(result, '"s"')

        result = dump.csv._csv_value({})
        self.assertEqual(result, '"{}"')

        result = dump.csv._csv_value("a\r\nb\nc")
        self.assertEqual(result, '"a b c"')

        result = dump.csv._csv_value("a\"b\"")
        self.assertEqual(result, '"a""b"""')

    @patch('gobapi.dump.csv.get_reference_fields', lambda x: dump.config.REFERENCE_FIELDS)
    def test_csv_header(self):
        result = dump.csv._csv_header({}, [])
        self.assertEqual(result, [])

        result = dump.csv._csv_header({'name': {'type': 'any type'}}, ['name'])
        self.assertEqual(result, ['"name"'])

        result = dump.csv._csv_header({'name': {'type': 'GOB.Reference'}}, ['name'])
        self.assertEqual(result, ['"name_ref"', '"name_id"', '"name_volgnummer"', '"name_bronwaarde"'])

        result = dump.csv._csv_header({'name': {'type': 'GOB.JSON', 'attributes': {'a': 'some a', 'b': 'some b'}}}, ['name'])
        self.assertEqual(result, ['"name_a"', '"name_b"'])

    @patch('gobapi.dump.csv.get_reference_fields', lambda x: dump.config.REFERENCE_FIELDS)
    def test_csv_reference_values(self):
        spec = {'type': 'GOB.Reference'}
        value = {}
        result = dump.csv._csv_reference_values(value, spec)
        self.assertEqual(result, ['', '', '', ''])

        value = {'id': 'any id', 'bronwaarde': 'any bronwaarde'}
        result = dump.csv._csv_reference_values(value, spec)
        self.assertEqual(result, ['"any id"', '"any id"', '', '"any bronwaarde"'])

        # defaults to ManyReference
        spec = {'type': 'any type'}
        values = []
        result = dump.csv._csv_reference_values(values, spec)
        self.assertEqual(result, ['[]', '[]', '[]', '[]'])

        spec = {'type': 'GOB.ManyReference'}
        values = [value]
        result = dump.csv._csv_reference_values(values, spec)
        self.assertEqual(result, ['["any id"]', '["any id"]', '[]', '["any bronwaarde"]'])

        spec = {'type': 'GOB.ManyReference'}
        values = [value, value]
        result = dump.csv._csv_reference_values(values, spec)
        self.assertEqual(result, ['["any id","any id"]', '["any id","any id"]', '[,]', '["any bronwaarde","any bronwaarde"]'])

    @patch('gobapi.dump.csv.get_reference_fields', lambda x: dump.config.REFERENCE_FIELDS)
    def test_csv_values(self):
        value = None
        result = dump.csv._csv_values(None, {'type': 'any type'})
        self.assertEqual(result, [dump.csv._csv_value(value)])

        value = {}
        spec = {'type': 'GOB.Reference', 'ref': 'any catalog:any collection'}
        result = dump.csv._csv_values(value, spec)
        self.assertEqual(result, dump.csv._csv_reference_values(value, spec))

        value = {'a': 1, 'b': 'any value', 'c': 'some other value'}
        spec = {'type': 'GOB.JSON', 'attributes': {'a': 'some a', 'b': 'some b'}}
        result = dump.csv._csv_values(value, spec)
        self.assertEqual(result, ['1', '"any value"'])

        value = {'a': 1}
        spec = {'type': 'GOB.JSON', 'attributes': {'a': 'some a', 'b': 'some b'}}
        result = dump.csv._csv_values(value, spec)
        self.assertEqual(result, ['1', ''])

    def test_csv_record(self):
        entity = None
        specs = {}
        result = dump.csv._csv_record(entity, specs, [])
        self.assertEqual(result, [])

        entity = MockEntity()
        specs = entity.specs()

        result = dump.csv._csv_record(entity, specs, ['a', 'b'])
        self.assertEqual(result, ['"a"', '5'])

    def test_csv_entities(self):
        entities = []
        model = {
            'catalog': 'any catalog',
            'entity_id': 'any entity id',
            'fields': {
                'any entity id': {
                    'type': 'any type'
                }
            },
            'all_fields': {
                'any entity id': {
                    'type': 'any type'
                }
            }
        }
        results = []
        for result in dump.csv.csv_entities(entities, model):
            results.append(result)

        self.assertEqual(results, [])

        entities = [MockEntity(), MockEntity()]
        model = {
            'catalog': 'any catalog',
            'entity_id': 'a',
            'fields': MockEntity.specs(),
            'all_fields': MockEntity.specs()
        }
        results = []
        for result in dump.csv.csv_entities(entities, model):
            results.append(result)

        self.assertEqual(results, ['"a";"b";"ref"\n', '"a";5;"a"\n', '"a";5;"a"\n'])


class TestDumpApi(TestCase):

    @patch('gobapi.api.dump_entities', lambda cat, col: ([], {}))
    @patch('gobapi.api.request')
    def test_dump_csv(self, mock_request):
        mock_request.method = 'GET'

        mock_request.args = {'format': 'csv'}
        result = api._dump("any catalog", "any collection")
        self.assertIsInstance(result, Response)

    @patch('gobapi.api.dump_entities', lambda cat, col: ([], {}))
    @patch('gobapi.api.sql_entities', lambda cat, col, model: [])
    @patch('gobapi.api.request')
    def test_dump_sql(self, mock_request):
        mock_request.method = 'GET'

        mock_request.args = {'format': 'sql'}
        result = api._dump("any catalog", "any collection")
        self.assertIsInstance(result, Response)

    @patch('gobapi.api.dump_entities', lambda cat, col: ([], {}))
    @patch('gobapi.api.request')
    def test_dump_other(self, mock_request):
        mock_request.method = 'GET'

        mock_request.args = {'format': 'any other format'}
        msg, status = api._dump("any catalog", "any collection")
        self.assertEqual(status, 400)

        mock_request.args = {}
        msg, status = api._dump("any catalog", "any collection")
        self.assertEqual(status, 400)

    @patch('gobapi.api.dump_to_db')
    @patch('gobapi.api.json')
    @patch('gobapi.api.request')
    def test_dump_db(self, mock_request, mock_json, mock_dump):
        mock_request.method = 'POST'
        mock_request.content_type = 'application/json'
        mock_json.loads.return_value = {}

        result = api._dump("any catalog", "any collection")
        mock_dump.assert_called_with("any catalog", "any collection", {})

        mock_request.content_type = 'any content type'
        mock_dump.reset_mock()
        result = api._dump("any catalog", "any collection")
        mock_dump.assert_not_called()


class TestDumpStorage(TestCase):

    @patch('gobapi.storage._Base', "any base")
    @patch('gobapi.storage.get_session', lambda: MockSession())
    @patch('gobapi.storage._get_table_and_model', lambda cat, col: ("any table", {}))
    def test_dump_entities(self):
        result = storage.dump_entities("any catalog", "any collection")
        self.assertEqual(result, ('any table', {'catalog': 'any catalog', 'collection': 'any collection'}))

class TestSkipFields(TestCase):

    @patch('gobapi.dump.config.is_relation', lambda m: True)
    def test_relation_skip_fields(self):
        model = {
            'all_fields': {
                'a': 'any field',
                **{k: 'any value' for k in dump.config.REL_FIELDS}
            }
        }
        result = dump.config.get_skip_fields(model)
        self.assertEqual(result, ['a'])
        self.assertNotEqual(result, [k for k in model['all_fields'].keys()])

class TestFieldOrder(TestCase):

    def test_field_order(self):
        model = {
            'catalog': 'any catalog',
            'entity_id': 'any entity id',
            'fields': {},
            'all_fields': {
                'any entity id': {'type': 'any id type'},
                'ref': {'type': 'any ref type'}
            }
        }
        order = dump.config.get_field_order(model)
        self.assertEqual(order, ['any entity id', 'ref'])

        model = {
            'catalog': 'any catalog',
            'entity_id': 'any',
            'fields': {
                'geo': {'type': 'GOB.Geo.Geometry'},
                'ref': {'type': 'GOB.Reference'},
                'any': {'type': 'Any type'},
                'a': {'type': 'Any type'},
                'volgnummer': {'type': 'Any type'},
                'b': {'type': 'Any type'},
            }
        }
        model['all_fields'] = {
            **model['fields'],
            'meta': {'type': 'any meta type'}
        }
        order = dump.config.get_field_order(model)
        self.assertEqual(order, ['any', 'volgnummer', 'a', 'b', 'ref', 'geo', 'ref', 'meta'])

    @patch('gobapi.dump.config.is_relation', lambda m: True)
    def test_field_order_relations(self):
        result = dump.config.get_field_order('any model')
        self.assertEqual(result, dump.config.REL_FIELDS)

class TestCSVStream(TestCase):

    def test_init(self):
        stream = CSVStream(iter([]), 1)
        self.assertIsNotNone(stream)

    def test_empty(self):
        stream = CSVStream(iter([]), 1)
        self.assertFalse(stream.has_items())

    def test_only_header(self):
        stream = CSVStream(iter(["a"]), 1)
        self.assertTrue(stream.has_items())
        result = stream.read()
        self.assertEqual(result, "a")

    def test_one_line(self):
        stream = CSVStream(iter(["a", "b"]), 1)
        self.assertTrue(stream.has_items())
        result = stream.read()
        self.assertEqual(result, "ab")

    def test_max_read(self):
        stream = CSVStream(iter(["a", "b", "c"]), 1)
        self.assertTrue(stream.has_items())
        result = stream.read()
        self.assertEqual(result, "ab")
        stream.reset_count()
        result = stream.read()
        self.assertEqual(result, "ac")

    def test_readline(self):
        stream = CSVStream(iter([]), 1)
        with self.assertRaises(NotImplementedError):
            stream.readline()


class MockStream():

    def __init__(self, *args):
        self._has_items = True

    def has_items(self):
        return self._has_items

    def reset_count(self):
        self._has_items = False

    total_count = 10


class TestToDB(TestCase):

    @patch('gobapi.dump.to_db.create_engine', MagicMock())
    @patch('gobapi.dump.to_db.URL', MagicMock())
    @patch('gobapi.dump.to_db.get_relation_name', lambda *args: 'any relation name')
    @patch('gobapi.dump.to_db._dump_to_db')
    @patch('gobapi.dump.to_db.dump_entities')
    def test_dump(self, mock_entities, mock_dump):
        config = {
            'db': {},
        }
        model = {
            'references': {
                'ref': 'any ref',
                'vmref': 'any very many ref'
            },
            'very_many_references': {
                'vmref': 'any very many ref'
            }
        }
        mock_entities.return_value = 'any entities', model
        mock_dump.return_value = iter([])
        results = [result for result in dump_to_db('any catalog name', 'any collection name', config)]
        self.assertEqual(mock_dump.call_count, 3)

        mock_dump.side_effect = lambda *args: 1 / 0
        results = "".join([result for result in dump_to_db('any catalog name', 'any collection name', config)])
        self.assertTrue("ERROR: Export failed" in results)

    @patch('gobapi.dump.to_db.create_engine', MagicMock())
    @patch('gobapi.dump.to_db.URL', MagicMock())
    @patch('gobapi.dump.to_db.get_relation_name', lambda *args: None)
    @patch('gobapi.dump.to_db._dump_to_db')
    @patch('gobapi.dump.to_db.dump_entities')
    def test_dump_non_existent_relation(self, mock_entities, mock_dump):
        config = {
            'db': {},
        }
        model = {
            'references': {
                'ref': 'any ref',
                'vmref': 'any very many ref'
            },
            'very_many_references': {
                'vmref': 'any very many ref'
            }
        }
        mock_entities.return_value = 'any entities', model
        mock_dump.return_value = iter([])
        results = [result for result in dump_to_db('any catalog name', 'any collection name', config)]
        self.assertEqual(mock_dump.call_count, 1)
        self.assertEqual(2, len([line for line in results if line.startswith('Skipping unmapped')]))

    @patch('gobapi.dump.to_db._create_schema', MagicMock())
    @patch('gobapi.dump.to_db._create_table', MagicMock())
    @patch('gobapi.dump.to_db.CSVStream', MockStream)
    @patch('gobapi.dump.to_db.COMMIT_PER', 1)
    def test_dump_to_db(self):
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        config = {
            'engine': mock_engine
        }
        model = {
            'catalog': 'any catalog'
        }
        mock_engine.raw_connection.return_value = mock_connection
        results = [result for result in _dump_to_db("any schema",
                                                    "any catalog name",
                                                    "any collection name",
                                                    iter([]),
                                                    model,
                                                    config)]
        self.assertEqual(mock_connection.commit.call_count, 2)
        mock_connection.close.assert_called()

    @patch('gobapi.dump.to_db._create_schema', MagicMock())
    @patch('gobapi.dump.to_db._create_table', MagicMock())
    @patch('gobapi.dump.to_db.CSVStream', MockStream)
    @patch('gobapi.dump.to_db.COMMIT_PER', 11)
    def test_dump_to_db_dots(self):
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        config = {
            'engine': mock_engine
        }
        model = {
            'catalog': 'any catalog'
        }
        mock_engine.raw_connection.return_value = mock_connection
        results = [result for result in _dump_to_db("any schema",
                                                    "any catalog name",
                                                    "any collection name",
                                                    iter([]),
                                                    model,
                                                    config)]
        self.assertEqual(mock_connection.commit.call_count, 1)
        mock_connection.close.assert_called()
        self.assertTrue("Export data.\nExported" in "".join(results))
