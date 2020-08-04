import re
from unittest import TestCase
from unittest.mock import MagicMock, patch, call, Mock

from gobapi.dump.to_db import dump_to_db, DbDumper, _dump_relations, FIELD, MAX_SYNC_ITEMS
from gobapi.dump.config import UNIQUE_REL_ID


class MockStream():

    def __init__(self, *args):
        self._has_items = True

    def has_items(self):
        return self._has_items

    def reset_count(self):
        self._has_items = False

    total_count = 10


@patch("gobapi.dump.to_db.DatastoreFactory")
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

    def test_init(self, mock_datastore_factory):
        db_dumper = self._get_dumper()

        mock_datastore_factory.get_datastore.assert_called_with({'config_key': 'config_value'})
        self.assertEqual(mock_datastore_factory.get_datastore.return_value, db_dumper.datastore)
        db_dumper.datastore.connect.assert_called_once()

        self.assertEqual({'catalog': self.catalog_name, 'collection': self.collection_name}, db_dumper.model)
        self.assertEqual(self.catalog_name, db_dumper.schema)
        self.assertNotEqual(db_dumper.collection_name, db_dumper.tmp_collection_name)
        self.assertTrue(db_dumper.collection_name in db_dumper.tmp_collection_name)

        db_dumper2 = DbDumper(self.catalog_name, self.collection_name, {'db': {}, 'schema': 'schema'})
        self.assertEqual('schema', db_dumper2.schema)

    @patch("gobapi.dump.to_db.GOBModel")
    def test_init_schema(self, mock_model, mock_datastore_factory):
        mock_model().get_catalog_from_abbr = lambda abbr: {'name': 'gebieden'} if abbr == 'gbd' else {}

        # Config schema has priority
        dumper = DbDumper('cat', 'gbd_bbk_bag_vot_la_bla_lda', {'db': {}, 'schema': 'the_schema'})
        self.assertEqual(dumper.schema, 'the_schema')

        # Otherwise return the catalog name
        dumper = DbDumper('cat', 'gbd_bbk_bag_vot_la_bla_lda', {'db': {}})
        self.assertEqual(dumper.schema, 'cat')

        # Unless the catalog is rel, then derive from relation name
        dumper = DbDumper('rel', 'gbd_bbk_bag_vot_la_bla_lda', {'db': {}})
        self.assertEqual(dumper.schema, 'gebieden')

    def test_table_exists(self, mock_datastore_factory):
        db_dumper = self._get_dumper()
        db_dumper.schema = 'schema'
        db_dumper._query = MagicMock(return_value=iter([('result_bool',)]))

        self.assertEqual('result_bool', db_dumper._table_exists('some_table'))
        db_dumper._query.assert_called_with("SELECT EXISTS ( SELECT 1 FROM information_schema.tables "
                                              "WHERE table_schema='schema' AND table_name='some_table')")

    def test_get_columns(self, mock_datastore_factory):
        result = [
            ('col_a', 'type_a'),
            ('col_b', 'type_b'),
        ]
        db_dumper = self._get_dumper()
        db_dumper._query = MagicMock(return_value=iter(result))
        db_dumper.schema = 'schema'

        self.assertEqual(result, db_dumper._get_columns('some_table'))
        db_dumper._query.assert_called_with(
            "SELECT column_name, udt_name FROM information_schema.columns "
            "WHERE table_schema='schema' AND table_name='some_table'"
        )

    def test_table_columns_equal(self, mock_datastore_factory):
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

    def test_copy_table_into(self, mock_datastore_factory):
        db_dumper = self._get_dumper()
        db_dumper._execute = MagicMock()
        db_dumper.schema = 'schema'
        db_dumper._copy_table_into('src_table', 'dst_table', [])
        db_dumper._execute.assert_called_with(
            'INSERT INTO "schema"."dst_table" SELECT * FROM "schema"."src_table" '
        )

        db_dumper._copy_table_into('src_table', 'dst_table', ['a', 'b'])
        db_dumper._execute.assert_called_with(
            'INSERT INTO "schema"."dst_table" SELECT * FROM "schema"."src_table" WHERE ref NOT IN (\'a\',\'b\')'
        )

        db_dumper.catalog_name = "rel"
        db_dumper._copy_table_into('src_table', 'dst_table', ['a', 'b'])
        db_dumper._execute.assert_called_with(
            'INSERT INTO "schema"."dst_table" SELECT * FROM "schema"."src_table" WHERE CONCAT(src_ref, \'_\', dst_ref) NOT IN (\'a\',\'b\')'
        )

    @patch('gobapi.dump.to_db.get_max_eventid')
    def test_get_max_eventid(self, mock_max_eventid, mock_datastore_factory):
        mock_max_eventid.return_value = 'the maxeventid query'

        db_dumper = self._get_dumper()
        db_dumper.schema = 'schema'
        db_dumper._query = MagicMock(return_value=iter([('event_id',)]))

        self.assertEqual('event_id', db_dumper._get_max_eventid('table name'))
        mock_max_eventid.assert_called_with('schema', 'table name')
        db_dumper._query.assert_called_with('the maxeventid query')

    def test_max_eventid_dst(self, mock_datastore_factory):
        db_dumper = self._get_dumper()
        db_dumper._table_exists = MagicMock(return_value=False)

        self.assertIsNone(db_dumper._max_eventid_dst())

        db_dumper._table_exists = MagicMock(return_value=True)
        db_dumper._get_max_eventid = MagicMock()

        self.assertEqual(db_dumper._get_max_eventid.return_value, db_dumper._max_eventid_dst())
        db_dumper._table_exists.assert_called_with(self.collection_name)
        db_dumper._get_max_eventid.assert_called_with(self.collection_name)

    @patch("gobapi.dump.to_db.get_src_max_eventid")
    def test_max_eventid_src(self, mock_get_src_max_eventid, mock_datastore_factory):
        db_dumper = self._get_dumper()
        self.assertEqual(mock_get_src_max_eventid.return_value, db_dumper._max_eventid_src())
        mock_get_src_max_eventid.assert_called_with(self.catalog_name, self.collection_name)

    @patch("gobapi.dump.to_db._create_schema")
    @patch("gobapi.dump.to_db._create_table")
    def test_prepare_destination(self, mock_table, mock_schema, mock_datastore_factory):
        db_dumper = self._get_dumper()
        db_dumper._execute = MagicMock()
        db_dumper._delete_tmp_table = MagicMock()
        db_dumper._table_exists = MagicMock(return_value=True)
        db_dumper._table_columns_equal = MagicMock(return_value=True)

        # Use mock manager to assert call order
        mock_manager = Mock()
        mock_manager.attach_mock(db_dumper._execute, '_execute')
        mock_manager.attach_mock(db_dumper._delete_tmp_table, '_delete_tmp_table')

        list(db_dumper._prepare_destination())

        mock_manager.assert_has_calls([
            call._execute(mock_schema.return_value),
            call._delete_tmp_table(),
            call._execute(mock_table.return_value),
            call._execute(mock_table.return_value),
        ])
        mock_schema.assert_called_with(db_dumper.schema)

        mock_table.assert_has_calls([
            call(
                db_dumper.schema,
                self.catalog_name,
                db_dumper.collection_name,
                db_dumper.model,
                tablename=db_dumper.tmp_collection_name
            ),
            call(
                db_dumper.schema,
                self.catalog_name,
                db_dumper.collection_name,
                db_dumper.model,
            )
        ])

    @patch("gobapi.dump.to_db._create_schema")
    @patch("gobapi.dump.to_db._create_table")
    def test_prepare_destination_replace_dst(self, mock_table, mock_schema, mock_datastore_factory):
        db_dumper = self._get_dumper()
        db_dumper._execute = MagicMock()
        db_dumper._delete_tmp_table = MagicMock()
        db_dumper._delete_table = MagicMock()
        db_dumper._table_exists = MagicMock(return_value=True)
        db_dumper._table_columns_equal = MagicMock(return_value=False)

        # Use mock manager to assert call order
        mock_manager = Mock()
        mock_manager.attach_mock(db_dumper._execute, '_execute')
        mock_manager.attach_mock(db_dumper._delete_tmp_table, '_delete_tmp_table')
        mock_manager.attach_mock(db_dumper._delete_table, '_delete_table')

        list(db_dumper._prepare_destination())

        mock_manager.assert_has_calls([
            call._execute(mock_schema.return_value),
            call._delete_tmp_table(),
            call._execute(mock_table.return_value),
            call._delete_table(db_dumper.collection_name),
            call._execute(mock_table.return_value),
        ])

    @patch("gobapi.dump.to_db._insert_into_table")
    def test_copy_tmp_table(self, mock_copy, mock_datastore_factory):
        db_dumper = self._get_dumper()
        db_dumper._execute = MagicMock()
        db_dumper._delete_tmp_table = MagicMock()
        list(db_dumper._copy_tmp_table())

        mock_copy.assert_called_with(db_dumper.schema,
                                       src_name=db_dumper.tmp_collection_name,
                                       dst_name=db_dumper.collection_name)
        db_dumper._execute.assert_called_with(mock_copy.return_value)
        
        db_dumper._delete_tmp_table.assert_called()

    @patch("gobapi.dump.to_db._delete_table")
    def test_delete_table(self, mock_delete, mock_datastore_factory):
        db_dumper = self._get_dumper()
        db_dumper._execute = MagicMock()
        db_dumper._delete_table('some table')

        mock_delete.assert_called_with(db_dumper.schema, 'some table')
        db_dumper._execute.assert_called_with(mock_delete.return_value)

    def test_delete_tmp_table(self, mock_datastore_factory):
        db_dumper = self._get_dumper()
        db_dumper._execute = MagicMock()
        db_dumper._delete_table = MagicMock()
        db_dumper._delete_tmp_table()

        db_dumper._delete_table.assert_called_with(db_dumper.tmp_collection_name)

    @patch('gobapi.dump.to_db._create_indexes')
    def test_create_indexes(self, mock_indexes, mock_datastore_factory):
        db_dumper = self._get_dumper()
        db_dumper._execute = MagicMock()

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

        mock_indexes.return_value = [{'field': 'any field'}, {'field': 'any other field'}]
        list(db_dumper._create_indexes(model))
        self.assertEqual(db_dumper._execute.call_count, 2)

    @patch('gobapi.dump.to_db.CSVStream')
    @patch('gobapi.dump.to_db.Authority', MagicMock())
    @patch('gobapi.dump.to_db.csv_entities', lambda x, model, cols: x)
    @patch('gobapi.dump.to_db.COMMIT_PER', 1)
    @patch('gobapi.dump.to_db.BUFFER_PER', 99)
    def test_dump_entities_to_table(self, mock_stream, mock_datastore_factory):
        mock_stream.return_value = MockStream()
        mock_connection = MagicMock()
        db_dumper = self._get_dumper()
        db_dumper.datastore.connection = mock_connection

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
    @patch('gobapi.dump.to_db.Authority', MagicMock())
    @patch('gobapi.dump.to_db.csv_entities', lambda x, model, cols : x)
    @patch('gobapi.dump.to_db.COMMIT_PER', 11)
    def test_dump_entities_dots(self, mock_datastore_factory):
        mock_connection = MagicMock()
        db_dumper = self._get_dumper()
        db_dumper.datastore.connection = mock_connection

        entities = iter([])
        results = list(db_dumper._dump_entities_to_table(entities, MagicMock()))
        self.assertEqual(mock_connection.commit.call_count, 1)
        self.assertTrue("Export data.\nExported" in "".join(results))

    @patch('gobapi.dump.to_db.CSVStream', MockStream)
    @patch('gobapi.dump.to_db.Authority')
    @patch('gobapi.dump.to_db.csv_entities')
    def test_dump_entities_suppress_cols(self, mock_csv_entities, mock_authority, mock_datastore_factory):
        db_dumper = self._get_dumper()
        db_dumper.datastore.connection = MagicMock()
        entities = iter([])
        model = MagicMock()
        results = list(db_dumper._dump_entities_to_table(entities, model))

        # Suppressed columns are passed to csv entities.
        mock_csv_entities.assert_called_with(entities, model, mock_authority().get_suppressed_columns())

    def test_filter_last_events_lambda(self, mock_datastore_factory):
        db_dumper = self._get_dumper()

        table = type('MockTable', (object,), {'_last_event': 24})
        self.assertTrue(db_dumper._filter_last_events_lambda(23)(table))
        self.assertFalse(db_dumper._filter_last_events_lambda(24)(table))
        self.assertFalse(db_dumper._filter_last_events_lambda(25)(table))

    def _get_dumper_for_dump_to_db(self):
        db_dumper = self._get_dumper()
        db_dumper._prepare_destination = MagicMock(return_value="")
        db_dumper._dump_entities_to_table = MagicMock(return_value="")
        db_dumper._copy_tmp_table = MagicMock(return_value="")
        db_dumper._create_indexes = MagicMock(return_value="")
        db_dumper._copy_table_into = MagicMock()
        db_dumper._delete_dst_entities = MagicMock()
        db_dumper._max_eventid_src = MagicMock(return_value=None)
        return db_dumper

    @patch('gobapi.dump.to_db.dump_entities')
    def test_dump_to_db_full_no_events(self, mock_dump_entities, mock_datastore_factory):
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
    def test_dump_to_db_force_full(self, mock_dump_entities, mock_datastore_factory):
        mock_dump_entities.return_value = [], {}
        db_dumper = self._get_dumper_for_dump_to_db()

        result = list(db_dumper.dump_to_db(True))
        db_dumper._copy_table_into.assert_not_called()

        self.assertIn('Do full dump\n', result)
        mock_dump_entities.assert_called_with(db_dumper.catalog_name,
                                              db_dumper.collection_name,
                                              order_by=FIELD.LAST_EVENT)

    @patch('gobapi.dump.to_db.dump_entities')
    def test_dump_to_db_full_columns_not_equal(self, mock_dump_entities, mock_datastore_factory):
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
    def test_dump_to_db_full_dst_eventid_greater(self, mock_dump_entities, mock_datastore_factory):
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
    def test_dump_to_db_full_no_last_src_eventid(self, mock_dump_entities, mock_datastore_factory):
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
    def test_dump_to_db_full_no_last_dst_eventid(self, mock_dump_entities, mock_datastore_factory):
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
    def test_dump_to_db_partial(self, mock_get_entity_refs, mock_dump_entities, mock_datastore_factory):
        mock_dump_entities.return_value = [], {}

        # New entities are available
        mock_get_entity_refs.return_value = ['ref1', 'ref2', 'ref3']

        db_dumper = self._get_dumper_for_dump_to_db()

        # Database scheme has not changed
        db_dumper._table_columns_equal = MagicMock(return_value=True)

        # src is more recent than destination
        db_dumper._max_eventid_src = MagicMock(return_value=5)
        db_dumper._max_eventid_dst = MagicMock(return_value=4)

        db_dumper._filter_last_events_lambda = MagicMock()

        result = list(db_dumper.dump_to_db())
        # Database scheme is compared
        db_dumper._table_columns_equal.assert_called_with(db_dumper.collection_name, db_dumper.tmp_collection_name)
        db_dumper._copy_table_into.assert_called_with(db_dumper.collection_name, db_dumper.tmp_collection_name, ['ref1', 'ref2', 'ref3'])

        mock_dump_entities.assert_called_with(db_dumper.catalog_name, db_dumper.collection_name,
                                              filter=db_dumper._filter_last_events_lambda.return_value,
                                              order_by=FIELD.LAST_EVENT)

    @patch('gobapi.dump.to_db.dump_entities')
    @patch('gobapi.dump.to_db.get_entity_refs_after')
    def test_dump_to_db_partial_no_source_ids_to_update(self, mock_get_entity_refs, mock_dump_entities,
                                                        mock_datastore_factory):
        mock_dump_entities.return_value = [], {}
        mock_get_entity_refs.return_value = []

        db_dumper = self._get_dumper_for_dump_to_db()
        db_dumper._table_columns_equal = MagicMock(return_value=True)
        db_dumper._max_eventid_src = MagicMock(return_value='MAX_EVENTID')
        db_dumper._max_eventid_dst = MagicMock(return_value='MAX_EVENTID')
        db_dumper._filter_last_events_lambda = MagicMock()
        db_dumper._delete_tmp_table = MagicMock()

        db_dumper._count_src = MagicMock(return_value=10)
        db_dumper._count_dst = MagicMock(return_value=10)

        list(db_dumper.dump_to_db())
        mock_dump_entities.assert_not_called()
        db_dumper._delete_tmp_table.assert_called()

    def assertQueryEquals(self, expected, query):
        def strip(query: str):
            # Remove comments
            stripped = re.sub(r'--.*\n', '', query)
            # Collapse all whitespace into single spaces
            stripped = re.sub(r'\s+', ' ', stripped)
            # Remove leading and trailing whitespace
            stripped = re.sub(r'^\s|\s$', '', stripped)

            return stripped

        self.assertEqual(strip(expected), strip(query))

    @patch("gobapi.dump.to_db.get_relation_name", lambda m, cat, col, rel: 'relation_name_' + rel if rel != 'refD' else None)
    @patch("gobapi.dump.to_db.GOBModel")
    def test_create_utility_view(self, mock_model, mock_datastore_factory):
        class MockedModel:
            def has_states(self, cat, col):
                # Will be called with dstcol or dstcolwithstates
                return 'withstates' in col

            def get_table_name(self, cat, col):
                return f'{cat}_{col}'

            def split_ref(self, ref):
                return ref.split(':')

        # First test case. src no states and refA a ManyRef and refB a single Reference
        mock_model.return_value = MockedModel()
        db_dumper = DbDumper('catalog', 'collection', {'db': {}})
        db_dumper._table_exists = lambda relname: relname != 'relation_name_refC'
        db_dumper._execute = MagicMock()
        db_dumper.model = {
            'abbreviation': 'abbr',
            'has_states': False,
            'references': {
                'refA': {},
                'refB': {},
                'refC': {},
                'refD': {},
            },
            'all_fields': {
                'refA': {
                    'ref': 'dstcat:dstcolwithstates',
                    'type': 'GOB.ManyReference',
                },
                'refB': {
                    'ref': 'dstcat:dstcol',
                    'type': 'GOB.Reference',
                },
                # refC will be ignored because its table doesn't exist (yet)
                'refC': {},
                # refD will be ignored because no relation_name is returned by get_relation_name, which indicates an
                # undefined relation
                'refD': {},
            }
        }

        expected_query = """create view catalog.v_collection as 
select abbr.*,
       refA.ref refA_ref,
       refA.dst_id refA_id,
       refA.dst_volgnummer refA_volgnummer,
       refB.dst_id refB_ref,
       refB.dst_id refB_id
from catalog.collection abbr
left join (
    select
        rel.src_id,
        array_agg(rel.dst_id) dst_id,
        array_agg(rel.dst_volgnummer) dst_volgnummer,
        array_agg(rel.dst_id || \'_\' || rel.dst_volgnummer) "ref"
    from catalog.relation_name_refA rel
    group by rel.src_id
) refA on refA.src_id = abbr._id
left join catalog.relation_name_refB refB on refB.src_id = abbr._id
"""

        self.assertEqual([
            'Creating view\n',
            'Excluding relation relation_name_refC from view because table does not exist\n',
            'Utility view catalog.v_collection created\n'
        ], list(db_dumper.create_utility_view()))

        db_dumper._execute.assert_any_call("drop view if exists catalog.v_collection")
        called_query = db_dumper._execute.call_args[0][0]
        self.assertQueryEquals(expected_query, called_query)

        # Second test case. src has states, refA single Reference and refB a ManyReference
        db_dumper.model['has_states'] = True
        db_dumper.model['all_fields']['refA']['type'] = 'GOB.Reference'
        db_dumper.model['all_fields']['refB']['type'] = 'GOB.ManyReference'
        db_dumper._execute.reset_mock()

        expected_query = """create view catalog.v_collection as 
select abbr.*,
       refA.dst_id || '_' || refA.dst_volgnummer refA_ref,
       refA.dst_id refA_id,
       refA.dst_volgnummer refA_volgnummer,
       refB.ref refB_ref,
       refB.dst_id refB_id
from catalog.collection abbr
left join catalog.relation_name_refA refA on refA.src_id = abbr._id and refA.src_volgnummer = abbr.volgnummer
left join (
    select
        rel.src_id,
        rel.src_volgnummer,
        array_agg(rel.dst_id) dst_id,
        array_agg(rel.dst_id) "ref"
    from catalog.relation_name_refB rel
    group by rel.src_id, rel.src_volgnummer
) refB on refB.src_id = abbr._id and refB.src_volgnummer = abbr.volgnummer
"""
        list(db_dumper.create_utility_view())
        called_query = db_dumper._execute.call_args[0][0]
        self.assertQueryEquals(expected_query, called_query)


    def test_sync_dump(self, mock_datastore_factory):
        db_dumper = self._get_dumper_for_dump_to_db()

        db_dumper._dump_entities = MagicMock()
        db_dumper._copy_table_into = MagicMock()
        db_dumper._filter_last_events_lambda = MagicMock()

        dst_max_eventid = 'any eventid'

        source_ids_to_update = ['any source id'] * (MAX_SYNC_ITEMS + 1)
        list(db_dumper._sync_dump(dst_max_eventid, source_ids_to_update))
        db_dumper._copy_table_into.assert_not_called()
        db_dumper._dump_entities.assert_called_with(filter=None)

        source_ids_to_update = ['any source id'] * MAX_SYNC_ITEMS
        list(db_dumper._sync_dump(dst_max_eventid, source_ids_to_update))
        db_dumper._copy_table_into.assert_called_with(
            db_dumper.collection_name,
            db_dumper.tmp_collection_name,
            source_ids_to_update)
        db_dumper._dump_entities.assert_called_with(filter=db_dumper._filter_last_events_lambda.return_value)

    @patch('gobapi.dump.to_db.get_src_count')
    def test_count_src(self, mock_get_count, mock_datastore_factory):
        db_dumper = DbDumper('catalog', 'collection', {'db': {}})
        result = db_dumper._count_src()
        self.assertEqual(result, mock_get_count.return_value)
        mock_get_count.assert_called_with('catalog', 'collection')

    @patch('gobapi.dump.to_db.get_dst_count')
    def test_count_dst(self, mock_get_count, mock_datastore_factory):
        db_dumper = DbDumper('catalog', 'collection', {'db': {}})
        db_dumper._count_dst()
        mock_get_count.assert_called_with('catalog', 'collection')

    @patch('gobapi.dump.to_db.get_entity_refs_after')
    def test_dump_scenarios(self, mock_get_entity_refs_after, mock_datastore_factory):
        def mock_dump(*args):
            yield "dump"
            return None, None

        def mock_actions():
            db_dumper._prepare_destination = MagicMock()
            db_dumper._delete_tmp_table = MagicMock()
            db_dumper._full_dump = MagicMock(side_effect=mock_dump)
            db_dumper._sync_dump = MagicMock(side_effect=mock_dump)
            db_dumper._dump_entities_to_table = MagicMock()
            db_dumper._copy_tmp_table = MagicMock()
            db_dumper._create_indexes = MagicMock()

        db_dumper = DbDumper('catalog', 'collection', {'db': {}})
        mock_actions()

        # The source and destination last event ids are compared
        db_dumper._max_eventid_src = MagicMock(return_value=10)
        db_dumper._max_eventid_dst = MagicMock(return_value=9)

        # The database scheme is compared for sync dumps
        db_dumper._table_columns_equal = MagicMock(return_value=True)

        # The counts are compared
        db_dumper._count_src = MagicMock(return_value=100)
        db_dumper._count_dst = MagicMock(return_value=100)

        # Call with full_dump
        mock_actions()
        list(db_dumper.dump_to_db(full_dump=True))
        db_dumper._full_dump.assert_called()
        db_dumper._sync_dump.assert_not_called()

        # Call and request sync dump
        mock_actions()
        result = list(db_dumper.dump_to_db())
        db_dumper._full_dump.assert_not_called()
        db_dumper._sync_dump.assert_called()

        mock_actions()
        # The event ids from both sides are compared, no src
        db_dumper._max_eventid_src = MagicMock(return_value=None)
        db_dumper._max_eventid_dst = MagicMock(return_value=10)
        list(db_dumper.dump_to_db())
        db_dumper._full_dump.assert_called()
        db_dumper._sync_dump.assert_not_called()

        mock_actions()
        # The event ids from both sides are compared, dst > src
        db_dumper._max_eventid_src = MagicMock(return_value=9)
        db_dumper._max_eventid_dst = MagicMock(return_value=10)
        list(db_dumper.dump_to_db())
        db_dumper._full_dump.assert_called()
        db_dumper._sync_dump.assert_not_called()

        mock_actions()
        # The event ids from both sides are compared, dst < src, but db scheme not equal
        db_dumper._max_eventid_src = MagicMock(return_value=10)
        db_dumper._max_eventid_dst = MagicMock(return_value=9)
        db_dumper._table_columns_equal = MagicMock(return_value=False)
        list(db_dumper.dump_to_db())
        db_dumper._full_dump.assert_called()
        db_dumper._sync_dump.assert_not_called()

        mock_actions()
        # The event ids from both sides are compared, dst < src, db scheme matches, items to sync
        db_dumper._max_eventid_src = MagicMock(return_value=10)
        db_dumper._max_eventid_dst = MagicMock(return_value=9)
        db_dumper._table_columns_equal = MagicMock(return_value=True)
        mock_get_entity_refs_after.return_value = [1, 2]
        list(db_dumper.dump_to_db())
        db_dumper._full_dump.assert_not_called()
        db_dumper._sync_dump.assert_called()

        mock_actions()
        # The event ids from both sides are compared, dst < src, db scheme matches, no items to sync, counts don't match
        db_dumper._max_eventid_src = MagicMock(return_value=10)
        db_dumper._max_eventid_dst = MagicMock(return_value=9)
        db_dumper._table_columns_equal = MagicMock(return_value=True)
        db_dumper._count_src = MagicMock(return_value=100)
        db_dumper._count_dst = MagicMock(return_value=90)
        mock_get_entity_refs_after.return_value = None
        list(db_dumper.dump_to_db())
        db_dumper._full_dump.assert_called()
        db_dumper._sync_dump.assert_not_called()

        mock_actions()
        # The event ids from both sides are compared, dst < src, db scheme matches, no items to sync, counts match
        db_dumper._max_eventid_src = MagicMock(return_value=10)
        db_dumper._max_eventid_dst = MagicMock(return_value=9)
        db_dumper._table_columns_equal = MagicMock(return_value=True)
        db_dumper._count_src = MagicMock(return_value=100)
        db_dumper._count_dst = MagicMock(return_value=100)
        mock_get_entity_refs_after.return_value = None
        list(db_dumper.dump_to_db())
        db_dumper._full_dump.assert_not_called()
        db_dumper._sync_dump.assert_not_called()

    def test_execute(self, mock_datastore_factory):
        db_dumper = DbDumper('catalog', 'collection', {'db': {}})
        db_dumper._execute('some query')
        db_dumper.datastore.execute.assert_called_with('some query')

    def test_query(self, mock_datastore_factory):
        db_dumper = DbDumper('catalog', 'collection', {'db': {}})
        res = db_dumper._query('some query')
        self.assertEqual(db_dumper.datastore.query.return_value, res)
        db_dumper.datastore.query.assert_called_with('some query')


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
        mock_dumper.return_value.dump_to_db.assert_called_once_with(full_dump=False)

        config = {'force_full': True}
        mock_dumper.reset_mock()
        # mock_dumper.return_value.dump_to_db.reset_mock()
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
        mock_dumper().dump_to_db.assert_called_with(full_dump=False)
        mock_dump_relations.assert_called_with('catalog_name', 'collection_name', config)
        mock_dumper.return_value.create_utility_view.assert_called_once()

        mock_dump_relations.reset_mock()
        config['include_relations'] = False
        config['force_full'] = True
        list(dump_to_db('catalog_name', 'collection_name', config))
        mock_dumper().dump_to_db.assert_called_with(full_dump=True)
        mock_dump_relations.assert_not_called()

        # Assert create_utility_view not called for 'rel' dumps
        mock_dumper.return_value.create_utility_view.reset_mock()
        list(dump_to_db('rel', 'collection_name', config))
        mock_dumper.return_value.create_utility_view.assert_not_called()


    def test_dump_to_db_exception(self, mock_dumper):
        mock_dumper.side_effect = Exception

        result = "".join(list(dump_to_db('catalog_name', 'collection_name', {})))
        self.assertIn("ERROR: Dump failed", result)
