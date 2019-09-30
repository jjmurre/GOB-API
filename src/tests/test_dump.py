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
        return any_query


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

    def test_create_table(self):
        result = dump.sql._create_table('any_schema', 'any_table', {})
        self.assertTrue("CREATE TABLE IF NOT EXISTS \"any_schema.any_table\"" in result)

        specs = {
            'a': {
                'type': 'GOB.String'
            }
        }
        result = dump.sql._create_table('any_schema', 'any_table', specs)
        self.assertTrue("\"a\" character varying" in result)

        specs = {
            'a': {
                'type': 'GOB.Reference'
            }
        }
        result = dump.sql._create_table('any_schema', 'any_table', specs)
        for s in ['ref', 'id', 'volgnummer', 'bronwaarde']:
            self.assertTrue(f"\"a_{s}\" character varying" in result)

    def test_import_csv(self):
        result = dump.sql._import_csv('any_schema', 'any_table', 'any_collection')
        self.assertTrue("\COPY \"any_schema.any_table\" FROM 'any_collection.csv'" in result)

    def test_sql_entities(self):
        model = {
            'entity_id': 'any_entity_id',
            'all_fields': {}
        }
        result = dump.sql.sql_entities('any catalog', 'any collection', model)
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

    def test_csv_header(self):
        result = dump.csv._csv_header({})
        self.assertEqual(result, [])

        result = dump.csv._csv_header({'name': {'type': 'any type'}})
        self.assertEqual(result, ['"name"'])

        result = dump.csv._csv_header({'name': {'type': 'GOB.Reference'}})
        self.assertEqual(result, ['"name_ref"', '"name_id"', '"name_volgnummer"', '"name_bronwaarde"'])

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

    def test_csv_record(self):
        entity = None
        specs = {}
        result = dump.csv._csv_record(entity, specs)
        self.assertEqual(result, [])

        entity = MockEntity()
        specs = entity.specs()

        result = dump.csv._csv_record(entity, specs)
        self.assertEqual(result, ['"a"', '5'])

    def test_csv_entities(self):
        entities = []
        model = {
            'entity_id': 'any entity id',
            'all_fields': {}
        }
        results = []
        for result in dump.csv.csv_entities(entities, model):
            results.append(result)

        self.assertEqual(results, [])

        entities = [MockEntity(), MockEntity()]
        model = {
            'entity_id': 'a',
            'all_fields': MockEntity.specs()
        }
        results = []
        for result in dump.csv.csv_entities(entities, model):
            results.append(result)

        self.assertEqual(results, ['"a";"b";"ref"\n', '"a";5;"a"\n'])


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
        result = api._dump("any catalog", "any collection")
        self.assertIsNone(result)

        mock_request.args = {}
        result = api._dump("any catalog", "any collection")
        self.assertIsNone(result)


class TestDumpStorage(TestCase):

    @patch('gobapi.storage._Base', "any base")
    @patch('gobapi.storage.get_session', lambda: MockSession())
    @patch('gobapi.storage._get_table_and_model', lambda cat, col: ("any table", "any model"))
    def test_dump_entities(self):
        result = storage.dump_entities("any catalog", "any collection")
        self.assertEqual(result, ('any table', 'any model'))
