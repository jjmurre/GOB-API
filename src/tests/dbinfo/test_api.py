from unittest import TestCase, mock

from gobapi.dbinfo.api import get_db_info

class TestDBInfoAPI(TestCase):

    @mock.patch("gobapi.dbinfo.api.DB_STATEMENTS", {'statement': "some statement"})
    @mock.patch("gobapi.dbinfo.api.exec_statement", lambda s: [[('exec', s)]])
    @mock.patch("gobapi.dbinfo.api.jsonify", lambda s: f'jsonify {s}')
    def test_db_info(self):
        result = get_db_info('some unknown statement')
        self.assertEqual(result, ("", 404))

        result = get_db_info('statement')
        self.assertEqual(result, "jsonify [{'exec': 'some statement'}]")
