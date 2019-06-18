import unittest
from unittest import mock

from gobapi.session import _db_session, set_session, get_session, ManagedSession, shutdown_session

class MockG:
    def __init__(self):
        print("INIT")

class TestSession(unittest.TestCase):

    def testLoad(self):
        self.assertEqual(_db_session, None)

    @mock.patch('gobapi.session.g', MockG())
    def testSetGetSession(self):
        set_session(lambda: "any session")
        self.assertEqual(get_session(), "any session")
        set_session(None)

    @mock.patch('gobapi.session._db_session')
    def testManagedSession(self, mocked_session):
        with ManagedSession() as s:
            mocked_session.assert_called()

    @mock.patch('gobapi.session.g', MockG())
    @mock.patch('gobapi.session._db_session')
    def testShutdownGraphQLSession(self, mocked_session):
        mocked_session.remove = mock.MagicMock()

        shutdown_session()
        mocked_session.remove.assert_called()

        shutdown_session("Any exception")
        mocked_session.remove.assert_called()

    @mock.patch('gobapi.session.g')
    def testShutdownRESTSession(self, mocked_g):
        mock_g = MockG()
        mock_g.session = mock.MagicMock()

        mocked_g.return_value = mock_g

        shutdown_session()
        self.assertTrue(not hasattr(mocked_g, 'session'))
