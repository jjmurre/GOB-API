from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobapi.dump.sql import _create_table, _create_schema, _import_csv, sql_entities, get_max_eventid, \
    delete_entities_with_source_ids, _quoted_tablename, _rename_table, _delete_table, _create_indexes, _create_index, to_sql_string_value
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
    @patch('gobapi.dump.sql._create_indexes')
    def test_sql_entities(self, mock_create, mock_specs):
        mock_specs.return_value = {
            'entity_id': 'any_entity_id',
            'all_fields': {}
        }
        result = sql_entities('any catalog', 'any collection', {})
        self.assertTrue("CREATE SCHEMA IF NOT EXISTS" in result)
        self.assertTrue("CREATE TABLE IF NOT EXISTS" in result)
        self.assertTrue("\COPY" in result)
        mock_create.assert_called()

    def test_quoted_tablename(self):
        result = _quoted_tablename('schema', 'collection')
        self.assertEqual('"schema"."collection"', result)

    def test_delete_table(self):
        result = _delete_table('any schema', 'any name')
        self.assertEqual('DROP TABLE IF EXISTS "any schema"."any name" CASCADE',
                         result)

    def test_rename_table(self):
        result = _rename_table('schema', 'current_name', 'new_name')
        self.assertEqual('\nDROP  TABLE IF EXISTS "schema"."new_name"     CASCADE;\n'
                         'ALTER TABLE IF EXISTS "schema"."current_name" RENAME TO new_name\n',
                         result)

    @patch('gobapi.dump.sql.get_reference_fields')
    @patch('gobapi.dump.sql.get_field_specifications')
    @patch('gobapi.dump.sql._create_index')
    def test_create_indexes(self, mock_index, mock_specs, mock_get_reference_fields):
        model = {
            'entity_id': "any id"
        }

        specs = {
            model['entity_id']: "any id value",
            'ref': "any ref",
            'dst_ref': "any dst ref",
            'dst_reference': {
                'type': "should be skipped"
            },
            '_ref': {
                'type': "should be skipped"
            },
            'fk': {
                'type': "GOB.Reference"
            },
            'any geo': {
                'type': "GOB.Geo.whatever"
            }
        }
        mock_specs.return_value = specs
        mock_get_reference_fields.return_value = ["ref"]

        result = list(_create_indexes(model))
        self.assertEqual(len(result), len(specs.keys()) - 2)

        # Do not create indexes for references to non-existing collections
        mock_get_reference_fields.return_value = []

        result = list(_create_indexes(model))
        self.assertEqual(len(result), len(specs.keys()) - 3)

    def test_create_index(self):
        result = _create_index('schema', 'collection', 'field', 'method')
        self.assertEqual('\nCREATE INDEX collection_field ON "schema"."collection" USING method (field)\n', result)

    def test_get_max_eventid(self):
        result = get_max_eventid('schema', 'collection')
        self.assertEqual('SELECT max(_last_event) FROM "schema"."collection"', result)

    def test_delete_entities_with_source_ids(self):
        result = delete_entities_with_source_ids('schema', 'collection', ['source_id_a', 'source_id_b'])

        self.assertEqual(
            'DELETE FROM "schema"."collection" WHERE _source_id IN (\'source_id_a\',\'source_id_b\')',
            result
        )

    def test_quote_sql_string(self):
        result = to_sql_string_value("test")
        self.assertEqual(result, "'test'")

        result = to_sql_string_value("te'st")
        self.assertEqual(result, "'te''st'")

        result = to_sql_string_value("te''st")
        self.assertEqual(result, "'te''''st'")