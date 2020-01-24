from unittest import TestCase
from unittest.mock import MagicMock, patch, call

from gobapi.dump.to_db import dump_to_db, DbDumper, _dump_relations, FIELD


class MockStream():

    def __init__(self, *args):
        self._has_items = True

    def has_items(self):
        return self._has_items

    def reset_count(self):
        self._has_items = False

    total_count = 10


@patch("gobapi.dump.to_db.URL")
@patch("gobapi.dump.to_db.create_engine")
@patch("gobapi.dump.to_db.get_table_and_model", MagicMock(return_value=(1, {})))
class TestDbDumper(TestCase):
    catalog_name = 'catalog_name'
    collection_name = 'collection_name'

    def _get_dumper(self):
        config = {
            'db': {
                'config_key': 'config_value',
            }
        }
        return DbDumper(self.catalog_name, self.collection_name, config)

    def test_init(self, mock_create_engine, mock_url):
        db_dumper = self._get_dumper()

        mock_url.assert_called_with(config_key='config_value')
        mock_create_engine.assert_called_with(mock_url.return_value)

        self.assertEqual({'catalog': self.catalog_name, 'collection': self.collection_name}, db_dumper.model)
        self.assertEqual(mock_create_engine.return_value, db_dumper.engine)
        self.assertEqual(self.catalog_name, db_dumper.schema)
        self.assertNotEqual(db_dumper.collection_name, db_dumper.tmp_collection_name)
        self.assertTrue(db_dumper.collection_name in db_dumper.tmp_collection_name)

        db_dumper2 = DbDumper(self.catalog_name, self.collection_name, {'db': {}, 'schema': 'schema'})
        self.assertEqual('schema', db_dumper2.schema)

    def test_table_exists(self, mock_create_engine, mock_url):
        db_dumper = self._get_dumper()
        db_dumper.engine.execute = MagicMock(return_value=iter([('result_bool',)]))
        db_dumper.schema = 'schema'

        self.assertEqual('result_bool', db_dumper._table_exists('some_table'))
        db_dumper.engine.execute.assert_called_with("SELECT EXISTS ( SELECT 1 FROM information_schema.tables "
                                                    "WHERE table_schema='schema' AND table_name='some_table')")

    def test_get_columns(self, mock_create_engine, mock_url):
        result = [
            ('col_a', 'type_a'),
            ('col_b', 'type_b'),
        ]
        db_dumper = self._get_dumper()
        db_dumper.engine.execute = MagicMock(return_value=iter(result))
        db_dumper.schema = 'schema'

        self.assertEqual(result, db_dumper._get_columns('some_table'))
        db_dumper.engine.execute.assert_called_with(
            "SELECT column_name, udt_name FROM information_schema.columns "
            "WHERE table_schema='schema' AND table_name='some_table'"
        )

    def test_table_columns_equal(self, mock_create_engine, mock_url):
        columns = {
            'table_a': ['a', 'b', 'c'],
            'table_b': ['d', 'e', 'f'],
            'table_c': ['a', 'b'],
            'table_d': ['a', 'c', 'b'],
            'table_e': ['a', 'b', 'c', 'd'],
            'table_f': [],
            'table_g': ['a', 'b', 'c'],
        }
        db_dumper = self._get_dumper()
        db_dumper._get_columns = lambda x: columns[x]

        self.assertFalse(db_dumper._table_columns_equal('table_a', 'table_b'))
        self.assertFalse(db_dumper._table_columns_equal('table_a', 'table_c'))
        self.assertFalse(db_dumper._table_columns_equal('table_a', 'table_d'))
        self.assertFalse(db_dumper._table_columns_equal('table_a', 'table_e'))
        self.assertFalse(db_dumper._table_columns_equal('table_a', 'table_f'))

        self.assertTrue(db_dumper._table_columns_equal('table_a', 'table_a'))
        self.assertTrue(db_dumper._table_columns_equal('table_a', 'table_g'))

    def test_copy_table_into(self, mock_create_engine, mock_url):
        db_dumper = self._get_dumper()
        db_dumper.engine.execute = MagicMock()
        db_dumper.schema = 'schema'
        db_dumper._copy_table_into('src_table', 'dst_table')
        db_dumper.engine.execute.assert_called_with(
            'INSERT INTO "schema"."dst_table" SELECT * FROM "schema"."src_table"'
        )

    @patch('gobapi.dump.to_db.get_max_eventid')
    def test_get_max_eventid(self, mock_max_eventid, mock_create_engine, mock_url):
        mock_max_eventid.return_value = 'the maxeventid query'

        db_dumper = self._get_dumper()
        db_dumper.schema = 'schema'
        db_dumper.engine.execute = MagicMock(return_value=iter([('event_id',)]))

        self.assertEqual('event_id', db_dumper._get_max_eventid('table name'))
        mock_max_eventid.assert_called_with('schema', 'table name')
        db_dumper.engine.execute.assert_called_with('the maxeventid query')

    def test_delete_dst_entities(self, mock_create_engine, mock_url):
        db_dumper = self._get_dumper()
        db_dumper.schema = 'schema'
        db_dumper.engine.execute = MagicMock()

        self.assertEqual(db_dumper.engine.execute.return_value,
                         db_dumper._delete_dst_entities('table_name', ["ref'1", 'ref2']))

        db_dumper.engine.execute.assert_called_with(
            'DELETE FROM "schema"."table_name" WHERE ref IN (\'ref\'\'1\',\'ref2\')'
        )

    def test_max_eventid_dst(self, mock_create_engine, mock_url):
        db_dumper = self._get_dumper()
        db_dumper._table_exists = MagicMock(return_value=False)

        self.assertIsNone(db_dumper._max_eventid_dst())

        db_dumper._table_exists = MagicMock(return_value=True)
        db_dumper._get_max_eventid = MagicMock()

        self.assertEqual(db_dumper._get_max_eventid.return_value, db_dumper._max_eventid_dst())
        db_dumper._table_exists.assert_called_with(self.collection_name)
        db_dumper._get_max_eventid.assert_called_with(self.collection_name)

    @patch("gobapi.dump.to_db.get_src_max_eventid")
    def test_max_eventid_src(self, mock_get_src_max_eventid, mock_create_engine, mock_url):
        db_dumper = self._get_dumper()
        self.assertEqual(mock_get_src_max_eventid.return_value, db_dumper._max_eventid_src())
        mock_get_src_max_eventid.assert_called_with(self.catalog_name, self.collection_name)

    @patch("gobapi.dump.to_db._create_schema")
    @patch("gobapi.dump.to_db._create_table")
    def test_prepare_destination(self, mock_table, mock_schema, mock_create_engine, mock_url):
        db_dumper = self._get_dumper()
        list(db_dumper._prepare_destination())

        execute_schema_call = call(mock_schema.return_value)
        execute_table_call = call(mock_table.return_value)
        db_dumper.engine.execute.assert_has_calls([
            execute_schema_call,
            execute_schema_call.close(),
            execute_table_call,
            execute_table_call.close()
        ])
        mock_schema.assert_called_with(db_dumper.schema)
        mock_table.assert_called_with(
            db_dumper.schema,
            self.catalog_name,
            db_dumper.tmp_collection_name,
            db_dumper.model,
        )

    @patch("gobapi.dump.to_db._rename_table")
    def test_rename_tmp_table(self, mock_rename, mock_create_engine, mock_url):
        db_dumper = self._get_dumper()
        list(db_dumper._rename_tmp_table())

        mock_rename.assert_called_with(db_dumper.schema,
                                       current_name=db_dumper.tmp_collection_name,
                                       new_name=db_dumper.collection_name)
        db_dumper.engine.execute.assert_called_with(mock_rename.return_value)

    @patch('gobapi.dump.to_db.get_reference_fields')
    @patch('gobapi.dump.to_db.get_field_specifications')
    @patch('gobapi.dump.to_db._create_index')
    def test_create_indexes(self, mock_index, mock_specs, mock_get_reference_fields, mock_create_engine, mock_url):
        db_dumper = self._get_dumper()

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

        list(db_dumper._create_indexes(model))
        self.assertEqual(db_dumper.engine.execute.call_count, len(specs.keys()) - 2)

        # Do not create indexes for references to non-existing collections
        mock_get_reference_fields.return_value = []
        db_dumper.engine.execute.reset_mock()

        list(db_dumper._create_indexes(model))
        self.assertEqual(db_dumper.engine.execute.call_count, len(specs.keys()) - 3)

    @patch('gobapi.dump.to_db.CSVStream')
    @patch('gobapi.dump.to_db.csv_entities', lambda x, _: x)
    @patch('gobapi.dump.to_db.COMMIT_PER', 1)
    @patch('gobapi.dump.to_db.BUFFER_PER', 99)
    def test_dump_entities_to_table(self, mock_stream, mock_create_engine, mock_url):
        mock_stream.return_value = MockStream()
        mock_connection = MagicMock()
        db_dumper = self._get_dumper()
        db_dumper.engine.raw_connection.return_value = mock_connection

        entities = iter([])
        results = list(db_dumper._dump_entities_to_table(entities, MagicMock()))

        self.assertEqual(mock_connection.commit.call_count, 2)
        self.assertTrue("Export data\ncollection_name: 10\nExported" in "".join(results))

        mock_cursor = mock_connection.cursor.return_value.__enter__.return_value
        mock_cursor.copy_expert.assert_called_with(
            sql="COPY catalog_name.tmp_collection_name FROM STDIN DELIMITER ';' CSV HEADER;",
            file=mock_stream.return_value,
            size=99,
        )

    @patch('gobapi.dump.to_db.CSVStream', MockStream)
    @patch('gobapi.dump.to_db.csv_entities', lambda x, _: x)
    @patch('gobapi.dump.to_db.COMMIT_PER', 11)
    def test_dump_entities_dots(self, mock_create_engine, mock_url):
        mock_connection = MagicMock()
        db_dumper = self._get_dumper()
        db_dumper.engine.raw_connection.return_value = mock_connection

        entities = iter([])
        results = list(db_dumper._dump_entities_to_table(entities, MagicMock()))
        self.assertEqual(mock_connection.commit.call_count, 1)
        self.assertTrue("Export data.\nExported" in "".join(results))

    def test_filter_last_events_lambda(self, mock_create_engine, mock_url):
        db_dumper = self._get_dumper()

        table = type('MockTable', (object,), {'_last_event': 24})
        self.assertTrue(db_dumper._filter_last_events_lambda(23)(table))
        self.assertFalse(db_dumper._filter_last_events_lambda(24)(table))
        self.assertFalse(db_dumper._filter_last_events_lambda(25)(table))

    def _get_dumper_for_dump_to_db(self):
        db_dumper = self._get_dumper()
        db_dumper._prepare_destination = MagicMock(return_value="")
        db_dumper._dump_entities_to_table = MagicMock(return_value="")
        db_dumper._rename_tmp_table = MagicMock(return_value="")
        db_dumper._create_indexes = MagicMock(return_value="")
        db_dumper._copy_table_into = MagicMock()
        db_dumper._delete_dst_entities = MagicMock()
        db_dumper._max_eventid_src = MagicMock(return_value=None)
        return db_dumper

    @patch('gobapi.dump.to_db.dump_entities')
    def test_dump_to_db_full_no_events(self, mock_dump_entities, mock_create_engine, mock_url):
        mock_dump_entities.return_value = [], {}
        db_dumper = self._get_dumper_for_dump_to_db()
        db_dumper._max_eventid_dst = MagicMock(return_value=None)

        result = list(db_dumper.dump_to_db())
        db_dumper._copy_table_into.assert_not_called()

        self.assertIn('Do full dump\n', result)
        mock_dump_entities.assert_called_with(db_dumper.catalog_name,
                                              db_dumper.collection_name,
                                              order_by=FIELD.LAST_EVENT)

    @patch('gobapi.dump.to_db.dump_entities')
    def test_dump_to_db_force_full(self, mock_dump_entities, mock_create_engine, mock_url):
        mock_dump_entities.return_value = [], {}
        db_dumper = self._get_dumper_for_dump_to_db()

        result = list(db_dumper.dump_to_db(True))
        db_dumper._copy_table_into.assert_not_called()

        self.assertIn('Do full dump\n', result)
        mock_dump_entities.assert_called_with(db_dumper.catalog_name,
                                              db_dumper.collection_name,
                                              order_by=FIELD.LAST_EVENT)

    @patch('gobapi.dump.to_db.dump_entities')
    def test_dump_to_db_full_columns_not_equal(self, mock_dump_entities, mock_create_engine, mock_url):
        mock_dump_entities.return_value = [], {}
        db_dumper = self._get_dumper_for_dump_to_db()
        db_dumper._table_columns_equal = MagicMock(return_value=False)

        result = list(db_dumper.dump_to_db())
        db_dumper._copy_table_into.assert_not_called()
        self.assertIn('Do full dump\n', result)
        mock_dump_entities.assert_called_with(db_dumper.catalog_name,
                                              db_dumper.collection_name,
                                              order_by=FIELD.LAST_EVENT)

    @patch('gobapi.dump.to_db.dump_entities')
    def test_dump_to_db_full_dst_eventid_greater(self, mock_dump_entities, mock_create_engine, mock_url):
        """In case the event id in the destination database is greater than the event id in the source database,
        events are removed from the source database.

        Test that a full dump is done in case this happens, because it is not possible to sync in this case.

        :param mock_dump_entities:
        :param mock_create_engine:
        :param mock_url:
        :return:
        """
        mock_dump_entities.return_value = [], {}
        db_dumper = self._get_dumper_for_dump_to_db()
        db_dumper._max_eventid_src.return_value = 2
        db_dumper._max_eventid_dst = MagicMock(return_value=3)

        result = list(db_dumper.dump_to_db())
        db_dumper._copy_table_into.assert_not_called()

        mock_dump_entities.assert_called_with(db_dumper.catalog_name,
                                              db_dumper.collection_name,
                                              order_by=FIELD.LAST_EVENT)

    @patch('gobapi.dump.to_db.dump_entities')
    def test_dump_to_db_full_no_last_src_eventid(self, mock_dump_entities, mock_create_engine, mock_url):
        """In case that no src event id exists a full dump should occur

        :param mock_dump_entities:
        :param mock_create_engine:
        :param mock_url:
        :return:
        """
        def fail_on_eventid():
            raise Exception

        mock_dump_entities.return_value = [], {}
        db_dumper = self._get_dumper_for_dump_to_db()
        db_dumper._max_eventid_src = fail_on_eventid

        result = list(db_dumper.dump_to_db())
        db_dumper._copy_table_into.assert_not_called()

        mock_dump_entities.assert_called_with(db_dumper.catalog_name,
                                              db_dumper.collection_name,
                                              order_by=FIELD.LAST_EVENT)

    @patch('gobapi.dump.to_db.dump_entities')
    def test_dump_to_db_full_no_last_src_eventid(self, mock_dump_entities, mock_create_engine, mock_url):
        """In case that no dst event id exists a full dump should occur

        :param mock_dump_entities:
        :param mock_create_engine:
        :param mock_url:
        :return:
        """
        def fail_on_eventid():
            raise Exception

        mock_dump_entities.return_value = [], {}
        db_dumper = self._get_dumper_for_dump_to_db()
        db_dumper._max_eventid_dst = fail_on_eventid

        result = list(db_dumper.dump_to_db())
        db_dumper._copy_table_into.assert_not_called()

        mock_dump_entities.assert_called_with(db_dumper.catalog_name,
                                              db_dumper.collection_name,
                                              order_by=FIELD.LAST_EVENT)

    @patch('gobapi.dump.to_db.dump_entities')
    @patch('gobapi.dump.to_db.get_entity_refs_after')
    def test_dump_to_db_partial(self, mock_get_entity_refs, mock_dump_entities, mock_create_engine, mock_url):
        mock_dump_entities.return_value = [], {}
        mock_get_entity_refs.return_value = ['ref1', 'ref2', 'ref3']

        db_dumper = self._get_dumper_for_dump_to_db()
        db_dumper._table_columns_equal = MagicMock(return_value=True)
        db_dumper._max_eventid_dst = MagicMock(return_value='MAX_EVENTID')
        db_dumper._filter_last_events_lambda = MagicMock()

        result = list(db_dumper.dump_to_db())
        db_dumper._table_columns_equal.assert_called_with(db_dumper.collection_name, db_dumper.tmp_collection_name)
        db_dumper._copy_table_into.assert_called_with(db_dumper.collection_name, db_dumper.tmp_collection_name)
        db_dumper._delete_dst_entities.assert_called_with(db_dumper.tmp_collection_name, mock_get_entity_refs.return_value)

        mock_dump_entities.assert_called_with(db_dumper.catalog_name, db_dumper.collection_name,
                                              filter=db_dumper._filter_last_events_lambda.return_value,
                                              order_by=FIELD.LAST_EVENT)

    @patch('gobapi.dump.to_db.dump_entities')
    @patch('gobapi.dump.to_db.get_entity_refs_after')
    def test_dump_to_db_partial_no_source_ids_to_update(self, mock_get_entity_refs, mock_dump_entities,
                                                        mock_create_engine, mock_url):
        mock_dump_entities.return_value = [], {}
        mock_get_entity_refs.return_value = []

        db_dumper = self._get_dumper_for_dump_to_db()
        db_dumper._table_columns_equal = MagicMock(return_value=True)
        db_dumper._max_eventid_dst = MagicMock(return_value='MAX_EVENTID')
        db_dumper._filter_last_events_lambda = MagicMock()

        result = list(db_dumper.dump_to_db())
        db_dumper._delete_dst_entities.assert_not_called()

        mock_dump_entities.assert_called_with(db_dumper.catalog_name, db_dumper.collection_name,
                                              filter=db_dumper._filter_last_events_lambda.return_value,
                                              order_by=FIELD.LAST_EVENT)


@patch('gobapi.dump.to_db.DbDumper')
class TestModuleFunctions(TestCase):

    @patch('gobapi.dump.to_db.get_table_and_model')
    @patch('gobapi.dump.to_db.get_relation_name', lambda m, cat, col, rel: rel)
    @patch('gobapi.dump.to_db.SKIP_RELATIONS', ['rel2', 'rel3'])
    def test_dump_relations(self, mock_get_table_model, mock_dumper):
        mock_get_table_model.return_value = 'something', \
                                            {'references': {'rel1': {}, 'rel2': {}, 'rel3': {}}}

        config = {}
        list(_dump_relations('catalog_name', 'collection_name', config))

        mock_dumper.assert_called_once_with('rel', 'rel1', config)
        mock_dumper.return_value.dump_to_db.assert_called_once_with(full_dump=True)

    @patch('gobapi.dump.to_db._dump_relations')
    def test_dump_to_db(self, mock_dump_relations, mock_dumper):
        config = {
            'db': {},
        }
        list(dump_to_db('catalog_name', 'collection_name', config))

        mock_dumper.assert_called_with('catalog_name', 'collection_name', config)
        mock_dump_relations.assert_called_with('catalog_name', 'collection_name', config)

        mock_dump_relations.reset_mock()
        config['include_relations'] = False
        list(dump_to_db('catalog_name', 'collection_name', config))
        mock_dump_relations.assert_not_called()

    def test_dump_to_db_exception(self, mock_dumper):
        mock_dumper.side_effect = Exception

        result = "".join(list(dump_to_db('catalog_name', 'collection_name', {})))
        self.assertIn("ERROR: Dump failed", result)
