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
                'type': 'GOB.String'
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


class TestDump(TestCase):

    def test_names_join(self):
        result = dump._names_join()
        self.assertEqual(result, "")

        result = dump._names_join("a")
        self.assertEqual(result, "a")

        result = dump._names_join("a", "b", "c")
        self.assertEqual(result, "a_b_c")

    def test_add_ref(self):
        result = dump._add_ref({})
        self.assertEqual(result, {'ref': None})

        result = dump._add_ref({'any': 'value'})
        self.assertEqual(result, {'any': 'value', 'ref': None})

        result = dump._add_ref({'id': 'any id'})
        self.assertEqual(result, {'id': 'any id', 'ref': 'any id'})

        result = dump._add_ref({'id': 'any id', 'volgnummer': 'any volgnummer'})
        self.assertEqual(result, {'id': 'any id', 'ref': 'any id_any volgnummer', 'volgnummer': 'any volgnummer'})

    def test_csv_line(self):
        result = dump._csv_line([])
        self.assertEqual(result, "\n")

        result = dump._csv_line(["a", "b"])
        self.assertEqual(result, "a;b\n")

    def test_csv_value(self):
        result = dump._csv_value(None)
        self.assertEqual(result, "")

        result = dump._csv_value(0)
        self.assertEqual(result, "0")

        result = dump._csv_value(0.5)
        self.assertEqual(result, "0.5")

        result = dump._csv_value("s")
        self.assertEqual(result, '"s"')

        result = dump._csv_value({})
        self.assertEqual(result, '"{}"')

    def test_csv_header(self):
        result = dump._csv_header({})
        self.assertEqual(result, [])

        result = dump._csv_header({'name': {'type': 'any type'}})
        self.assertEqual(result, ['"name"'])

        result = dump._csv_header({'name': {'type': 'GOB.Reference'}})
        self.assertEqual(result, ['"name_ref"', '"name_id"', '"name_volgnummer"', '"name_bronwaarde"'])

    def test_csv_reference_values(self):
        spec = {'type': 'GOB.Reference'}
        value = {}
        result = dump._csv_reference_values(value, spec)
        self.assertEqual(result, ['', '', '', ''])

        value = {'id': 'any id', 'bronwaarde': 'any bronwaarde'}
        result = dump._csv_reference_values(value, spec)
        self.assertEqual(result, ['"any id"', '"any id"', '', '"any bronwaarde"'])

        # defaults to ManyReference
        spec = {'type': 'any type'}
        values = []
        result = dump._csv_reference_values(values, spec)
        self.assertEqual(result, ['[]', '[]', '[]', '[]'])

        spec = {'type': 'GOB.ManyReference'}
        values = [value]
        result = dump._csv_reference_values(values, spec)
        self.assertEqual(result, ['["any id"]', '["any id"]', '[]', '["any bronwaarde"]'])

        spec = {'type': 'GOB.ManyReference'}
        values = [value, value]
        result = dump._csv_reference_values(values, spec)
        self.assertEqual(result, ['["any id","any id"]', '["any id","any id"]', '[,]', '["any bronwaarde","any bronwaarde"]'])

    def test_csv_values(self):
        value = None
        result = dump._csv_values(None, {'type': 'any type'})
        self.assertEqual(result, [dump._csv_value(value)])

        value = {}
        spec = {'type': 'GOB.Reference'}
        result = dump._csv_values(value, spec)
        self.assertEqual(result, dump._csv_reference_values(value, spec))

    def test_csv_record(self):
        entity = None
        specs = {}
        result = dump._csv_record(entity, specs)
        self.assertEqual(result, [])

        entity = MockEntity()
        specs = entity.specs()

        result = dump._csv_record(entity, specs)
        self.assertEqual(result, ['"a"', '5'])

    def test_csv_entities(self):
        entities = []
        model = {
            'all_fields': {}
        }
        results = []
        for result in dump.csv_entities(entities, model):
            results.append(result)

        self.assertEqual(results, [])

        entities = [MockEntity(), MockEntity()]
        model = {
            'all_fields': MockEntity.specs()
        }
        results = []
        for result in dump.csv_entities(entities, model):
            results.append(result)

        self.assertEqual(results, ['"a";"b"\n', '"a";5\n'])


class TestDumpApi(TestCase):

    @patch('gobapi.api.dump_entities', lambda cat, col: ([], {}))
    @patch('gobapi.api.request')
    def test_dump(self, mock_request):
        mock_request.args = {'format': 'csv'}
        result = api._dump("any catalog", "any collection")
        self.assertIsInstance(result, Response)


class TestDumpStorage(TestCase):

    @patch('gobapi.storage._Base', "any base")
    @patch('gobapi.storage.get_session', lambda: MockSession())
    @patch('gobapi.storage._get_table_and_model', lambda cat, col: ("any table", "any model"))
    def test_dump_entities(self):
        result = storage.dump_entities("any catalog", "any collection")
        self.assertEqual(result, ('any table', 'any model'))
