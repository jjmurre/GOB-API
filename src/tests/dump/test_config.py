from unittest import TestCase
from unittest.mock import patch

from flask import Response

from gobapi import api, storage

from gobapi.dump.config import (
    get_unique_reference, add_unique_reference, get_field_specifications, get_field_value,
    joined_names, get_field_order, get_reference_fields, REFERENCE_FIELDS, FIELD, REL_FIELDS, get_skip_fields
)


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

    def set_catalog_collection(self, *args):
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


class TestConfig(TestCase):

    @patch('gobapi.dump.config.GOBModel', MockModel)
    def test_reference_fields(self):
        MockModel.cat_col_has_states = True
        self.assertEqual(get_reference_fields({'ref': 'a:b'}), REFERENCE_FIELDS)
        self.assertTrue(FIELD.SEQNR in get_reference_fields({'ref': 'a:b'}))

        MockModel.cat_col_has_states = False
        self.assertTrue(FIELD.SEQNR not in get_reference_fields({'ref': 'a:b'}))

        MockModel.collection = None
        self.assertEqual([FIELD.SOURCE_VALUE], get_reference_fields({'ref': 'a:b'}))

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


@patch('gobapi.api.stream_with_context', lambda f: f)
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
    @patch('gobapi.api.stream_with_context', lambda f: f)
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
                **{k: 'any value' for k in REL_FIELDS}
            }
        }
        result = get_skip_fields(model)
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
        order = get_field_order(model)
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
        order = get_field_order(model)
        self.assertEqual(order, ['any', 'volgnummer', 'a', 'b', 'ref', 'geo', 'ref', 'meta'])

    @patch('gobapi.dump.config.is_relation', lambda m: True)
    def test_field_order_relations(self):
        result = get_field_order('any model')
        self.assertEqual(result, REL_FIELDS)
