from unittest import TestCase
from unittest.mock import patch

from gobapi.dump.config import REFERENCE_FIELDS
from gobapi.dump.csv import _csv_line, _csv_value, _csv_header, _csv_reference_values, _csv_values, _csv_record, csv_entities


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


class TestCSV(TestCase):

    def test_csv_line(self):
        result = _csv_line([])
        self.assertEqual(result, "\n")

        result = _csv_line(["a", "b"])
        self.assertEqual(result, "a;b\n")

    def test_csv_value(self):
        result = _csv_value(None)
        self.assertEqual(result, "")

        result = _csv_value(0)
        self.assertEqual(result, "0")

        result = _csv_value(0.5)
        self.assertEqual(result, "0.5")

        result = _csv_value("s")
        self.assertEqual(result, '"s"')

        result = _csv_value({})
        self.assertEqual(result, '"{}"')

        result = _csv_value("a\r\nb\nc")
        self.assertEqual(result, '"a b c"')

        result = _csv_value("a\"b\"")
        self.assertEqual(result, '"a""b"""')

    @patch('gobapi.dump.csv.get_reference_fields', lambda x: REFERENCE_FIELDS)
    def test_csv_header(self):
        result = _csv_header({}, [])
        self.assertEqual(result, [])

        result = _csv_header({'name': {'type': 'any type'}}, ['name'])
        self.assertEqual(result, ['"name"'])

        result = _csv_header({'name': {'type': 'GOB.Reference'}}, ['name'])
        self.assertEqual(result, ['"name_bronwaarde"'])

        result = _csv_header({'name': {'type': 'GOB.JSON', 'attributes': {'a': 'some a', 'b': 'some b'}}}, ['name'])
        self.assertEqual(result, ['"name_a"', '"name_b"'])

    @patch('gobapi.dump.csv.get_reference_fields', lambda x: REFERENCE_FIELDS)
    def test_csv_reference_values(self):
        spec = {'type': 'GOB.Reference'}
        value = {}
        result = _csv_reference_values(value, spec)
        self.assertEqual(result, [''])

        value = {'id': 'any id', 'bronwaarde': 'any bronwaarde'}
        result = _csv_reference_values(value, spec)
        self.assertEqual(result, ['"any bronwaarde"'])

        # defaults to ManyReference
        spec = {'type': 'any type'}
        values = []
        result = _csv_reference_values(values, spec)
        self.assertEqual(result, ['[]'])

        spec = {'type': 'GOB.ManyReference'}
        values = [value]
        result = _csv_reference_values(values, spec)
        self.assertEqual(result, ['["any bronwaarde"]'])

        spec = {'type': 'GOB.ManyReference'}
        values = [value, value]
        result = _csv_reference_values(values, spec)
        self.assertEqual(result, ['["any bronwaarde","any bronwaarde"]'])

    @patch('gobapi.dump.csv.get_reference_fields', lambda x: REFERENCE_FIELDS)
    def test_csv_values(self):
        value = None
        result = _csv_values(None, {'type': 'any type'})
        self.assertEqual(result, [_csv_value(value)])

        value = {}
        spec = {'type': 'GOB.Reference', 'ref': 'any catalog:any collection'}
        result = _csv_values(value, spec)
        self.assertEqual(result, _csv_reference_values(value, spec))

        value = None
        spec = {'type': 'GOB.Reference', 'ref': 'any catalog:any collection'}
        result = _csv_values(value, spec)
        self.assertEqual(result, _csv_reference_values(value, spec))

        value = {'a': 1, 'b': 'any value', 'c': 'some other value'}
        spec = {'type': 'GOB.JSON', 'attributes': {'a': 'some a', 'b': 'some b'}}
        result = _csv_values(value, spec)
        self.assertEqual(result, ['1', '"any value"'])

        value = {'a': 1}
        spec = {'type': 'GOB.JSON', 'attributes': {'a': 'some a', 'b': 'some b'}}
        result = _csv_values(value, spec)
        self.assertEqual(result, ['1', ''])

        value = None
        spec = {'type': 'GOB.JSON', 'attributes': {'a': 'some a', 'b': 'some b'}}
        result = _csv_values(value, spec)
        self.assertEqual(result, ['', ''])

    def test_csv_record(self):
        entity = None
        specs = {}
        result = _csv_record(entity, specs, [])
        self.assertEqual(result, [])

        entity = MockEntity()
        specs = entity.specs()

        result = _csv_record(entity, specs, ['a', 'b'])
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
        for result in csv_entities(entities, model):
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
        for result in csv_entities(entities, model):
            results.append(result)

        self.assertEqual(results, ['"a";"b";"ref"\n', '"a";5;"a"\n', '"a";5;"a"\n'])
