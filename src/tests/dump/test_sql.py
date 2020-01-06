from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobapi.dump.sql import _create_table, _create_schema, _import_csv, sql_entities
from gobapi.dump.config import REFERENCE_FIELDS


class TestSQL(TestCase):

    def test_create_schema(self):
        result = _create_schema('any_name')
        self.assertTrue("CREATE SCHEMA IF NOT EXISTS \"any_name\"" in result)

    @patch('gobapi.dump.sql.GOBModel', MagicMock())
    @patch('gobapi.dump.sql.get_reference_fields', lambda x: REFERENCE_FIELDS)
    @patch('gobapi.dump.sql.Authority')
    @patch('gobapi.dump.sql.get_field_order')
    @patch('gobapi.dump.sql.get_field_specifications')
    def test_create_table(self, mock_specs, mock_order, mock_authority):
        catalogue = {
            'description': "Any 'catalogue' description"
        }
        result = _create_table('any_schema', catalogue, 'any_table', {})
        self.assertTrue("CREATE TABLE IF NOT EXISTS \"any_schema\".\"any_table\"" in result)
        self.assertTrue("COMMENT ON TABLE  \"any_schema\".\"any_table\"" in result)

        mock_specs.return_value = {
            'a': {
                'type': 'GOB.String',
                'description': "Any 'a' description"
            }
        }
        mock_order.return_value = ['a']
        result = _create_table(catalogue, 'any_schema', 'any_table', {})
        self.assertTrue("\"a\" character varying" in result)
        self.assertTrue("Any ''a'' description" in result)
        mock_authority.assert_called()

        mock_specs.return_value = {
            'a': {
                'type': 'GOB.Reference',
                'description': 'Any description'
            }
        }
        result = _create_table(catalogue, 'any_schema', 'any_table', {})
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
        result = _create_table(catalogue, 'any_schema', 'any_table', {})
        for s in ['a', 'b']:
            self.assertTrue(f"\"a_{s}\"" in result)
            self.assertTrue(f"character varying" in result)

    def test_import_csv(self):
        result = _import_csv('any_schema', 'any_collection', 'any_collection.csv')
        self.assertTrue("\COPY \"any_schema\".\"any_collection\" FROM 'any_collection.csv'" in result)

    @patch('gobapi.dump.sql.Authority', MagicMock())
    @patch('gobapi.dump.sql.GOBModel', MagicMock())
    @patch('gobapi.dump.sql.get_field_order', MagicMock())
    @patch('gobapi.dump.sql.get_field_specifications')
    def test_sql_entities(self, mock_specs):
        mock_specs.return_value = {
            'entity_id': 'any_entity_id',
            'all_fields': {}
        }
        result = sql_entities('any catalog', 'any collection', {})
        self.assertTrue("CREATE SCHEMA IF NOT EXISTS" in result)
        self.assertTrue("CREATE TABLE IF NOT EXISTS" in result)
        self.assertTrue("\COPY" in result)

