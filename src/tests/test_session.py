import unittest
from unittest import mock

from gobapi.session import _db_session, set_session, get_session, shutdown_session

class MockG:
    def __init__(self):
        print("INIT")

class TestSession(unittest.TestCase):

    def testLoad(self):
        self.assertEqual(_db_session, None)

    def testSetGetSession(self):
        set_session(lambda: "any session")
        self.assertEqual(get_session(), "any session")
        set_session(None)

    @mock.patch('gobapi.session._db_session')
    def testShutdownGraphQLSession(self, mocked_session):
        mocked_session.remove = mock.MagicMock()

        shutdown_session()
        mocked_session.remove.assert_called()

        shutdown_session("Any exception")
        mocked_session.remove.assert_called()
