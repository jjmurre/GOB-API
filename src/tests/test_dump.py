from unittest import TestCase
from unittest.mock import MagicMock, patch

from flask import Response

from gobapi import dump, api, storage


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

from gobapi.dump.config import get_unique_reference, add_unique_reference
from gobapi.dump.config import get_field_specifications, get_field_value, joined_names


class TestConfig(TestCase):

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
        result = get_unique_reference(MockEntity(), MockEntity.specs())
        self.assertEqual(result, 'a_5')

    @patch('gobapi.dump.config.UNIQUE_ID', 'a')
    @patch('gobapi.dump.config.FIELD.SEQNR', 'c')
    def test_get_unique_reference_without_volgnummer(self):
        result = get_unique_reference(MockEntity(), MockEntity.specs())
        self.assertEqual(result, 'a')

    def test_get_field_specifications(self):
        model = {
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

    @patch('gobapi.dump.sql.get_field_order')
    @patch('gobapi.dump.sql.get_field_specifications')
    def test_create_table(self, mock_specs, mock_order):
        catalogue = {
            'description': 'Any description'
        }
        result = dump.sql._create_table(catalogue, 'any_schema', 'any_table', {})
        self.assertTrue("CREATE TABLE IF NOT EXISTS \"any_schema\".\"any_table\"" in result)

        mock_specs.return_value = {
            'a': {
                'type': 'GOB.String',
                'description': 'Any description'
            }
        }
        mock_order.return_value = ['a']
        result = dump.sql._create_table(catalogue, 'any_schema', 'any_table', {})
        self.assertTrue("\"a\" character varying" in result)

        mock_specs.return_value = {
            'a': {
                'type': 'GOB.Reference',
                'description': 'Any description'
            }
        }
        result = dump.sql._create_table(catalogue, 'any_schema', 'any_table', {})
        print(result)
        for s in ['ref', 'id', 'volgnummer', 'bronwaarde']:
            self.assertTrue(f"\"a_{s}\"" in result)
            self.assertTrue(f"character varying" in result)

        mock_specs.return_value = {
            'a': {
                'type': 'GOB.JSON',
                'fields': ['a', 'b'],
                'description': 'Any description'
            }
        }
        result = dump.sql._create_table(catalogue, 'any_schema', 'any_table', {})
        print(result)
        for s in ['a', 'b']:
            self.assertTrue(f"\"a_{s}\"" in result)
            self.assertTrue(f"character varying" in result)

    def test_import_csv(self):
        result = dump.sql._import_csv('any_schema', 'any_table', 'any_collection')
        self.assertTrue("\COPY \"any_schema\".\"any_table\" FROM 'any_collection.csv'" in result)

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

    def test_csv_header(self):
        result = dump.csv._csv_header({}, [])
        self.assertEqual(result, [])

        result = dump.csv._csv_header({'name': {'type': 'any type'}}, ['name'])
        self.assertEqual(result, ['"name"'])

        result = dump.csv._csv_header({'name': {'type': 'GOB.Reference'}}, ['name'])
        self.assertEqual(result, ['"name_ref"', '"name_id"', '"name_volgnummer"', '"name_bronwaarde"'])

        result = dump.csv._csv_header({'name': {'type': 'GOB.JSON', 'fields': ['a', 'b']}}, ['name'])
        self.assertEqual(result, ['"name_a"', '"name_b"'])

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

    def test_csv_values(self):
        value = None
        result = dump.csv._csv_values(None, {'type': 'any type'})
        self.assertEqual(result, [dump.csv._csv_value(value)])

        value = {}
        spec = {'type': 'GOB.Reference'}
        result = dump.csv._csv_values(value, spec)
        self.assertEqual(result, dump.csv._csv_reference_values(value, spec))

        value = {'a': 1, 'b': 'any value', 'c': 'some other value'}
        spec = {'type': 'GOB.JSON', 'fields': ['a', 'b']}
        result = dump.csv._csv_values(value, spec)
        self.assertEqual(result, ['1', '"any value"'])

        value = {'a': 1}
        spec = {'type': 'GOB.JSON', 'fields': ['a', 'b']}
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
        mock_request.args = {'format': 'csv'}
        result = api._dump("any catalog", "any collection")
        self.assertIsInstance(result, Response)

    @patch('gobapi.api.dump_entities', lambda cat, col: ([], {}))
    @patch('gobapi.api.sql_entities', lambda cat, col, model: [])
    @patch('gobapi.api.request')
    def test_dump_sql(self, mock_request):
        mock_request.args = {'format': 'sql'}
        result = api._dump("any catalog", "any collection")
        self.assertIsInstance(result, Response)

    @patch('gobapi.api.dump_entities', lambda cat, col: ([], {}))
    @patch('gobapi.api.request')
    def test_dump_other(self, mock_request):
        mock_request.args = {'format': 'any other format'}
        msg, status = api._dump("any catalog", "any collection")
        self.assertEqual(status, 400)

        mock_request.args = {}
        msg, status = api._dump("any catalog", "any collection")
        self.assertEqual(status, 400)


class TestDumpStorage(TestCase):

    @patch('gobapi.storage._Base', "any base")
    @patch('gobapi.storage.get_session', lambda: MockSession())
    @patch('gobapi.storage._get_table_and_model', lambda cat, col: ("any table", "any model"))
    def test_dump_entities(self):
        result = storage.dump_entities("any catalog", "any collection")
        self.assertEqual(result, ('any table', 'any model'))

class TestFieldOrder(TestCase):

    def test_field_order(self):
        model = {
            'entity_id': 'Any entity id',
            'fields': {},
            'all_fields': {}
        }
        order = dump.config.get_field_order(model)
        self.assertEqual(order, ['Any entity id', 'ref'])

        model = {
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
            'meta': {}
        }
        order = dump.config.get_field_order(model)
        self.assertEqual(order, ['any', 'volgnummer', 'a', 'b', 'ref', 'geo', 'ref', 'meta'])



