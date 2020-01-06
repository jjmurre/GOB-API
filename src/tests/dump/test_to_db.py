from unittest import TestCase
from unittest.mock import MagicMock, patch, ANY

from gobapi.dump.to_db import dump_to_db, _dump_to_db, _create_indexes


class MockStream():

    def __init__(self, *args):
        self._has_items = True

    def has_items(self):
        return self._has_items

    def reset_count(self):
        self._has_items = False

    total_count = 10


class TestToDB(TestCase):

    @patch('gobapi.dump.to_db.SKIP_RELATIONS', ["skip me"])
    @patch('gobapi.dump.to_db.create_engine', MagicMock())
    @patch('gobapi.dump.to_db.URL', MagicMock())
    @patch('gobapi.dump.to_db.get_relation_name', lambda m, cat, col, rel: rel)
    @patch('gobapi.dump.to_db._dump_to_db')
    @patch('gobapi.dump.to_db.dump_entities')
    def test_dump(self, mock_entities, mock_dump):
        config = {
            'db': {},
        }
        model = {
            'references': {
                'ref': 'any ref',
                'vmref': 'any very many ref',
                'skip me': 'skip this ref'
            },
            'very_many_references': {
                'vmref': 'any very many ref'
            }
        }
        mock_entities.return_value = 'any entities', model
        mock_dump.return_value = iter([])
        results = [result for result in dump_to_db('any catalog name', 'any collection name', config)]
        self.assertEqual(mock_dump.call_count, 3)

        mock_dump.reset_mock()
        config['include_relations'] = False
        config['schema'] = 'any schema'
        results = [result for result in dump_to_db('any catalog name', 'any collection name', config)]
        self.assertEqual(mock_dump.call_count, 1)
        mock_dump.assert_called_with('any schema', 'any catalog name', 'any collection name', ANY, ANY, config)

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
        self.assertEqual(2, len([line for line in results if "Skipping" in line]))

    @patch('gobapi.dump.to_db._create_schema', MagicMock())
    @patch('gobapi.dump.to_db._create_indexes', MagicMock())
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
    @patch('gobapi.dump.to_db._create_indexes', MagicMock())
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

    @patch('gobapi.dump.to_db.get_reference_fields')
    @patch('gobapi.dump.to_db.get_field_specifications')
    def test_create_indexes(self, mock_specs, mock_get_reference_fields):
        mock_engine = MagicMock()
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

        results = _create_indexes(mock_engine, "any schema", "any collection", model)
        for result in results:
            print(result)
        self.assertEqual(mock_engine.execute.call_count, len(specs.keys()) - 2)

        # Do not create indexes for references to non-existing collections
        mock_get_reference_fields.return_value = []
        mock_engine.execute.reset_mock()

        results = _create_indexes(mock_engine, "any schema", "any collection", model)
        for result in results:
            print(result)
        self.assertEqual(mock_engine.execute.call_count, len(specs.keys()) - 3)
