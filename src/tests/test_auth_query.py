from unittest import TestCase, mock
from unittest.mock import patch, MagicMock

from gobapi.auth_query import AuthorizedQuery, GOB_AUTH_SCHEME, REQUEST_ROLES

role_a = "a"
role_b = "b"
role_c = "c"

mock_scheme = {
    "any catalog": {
        "collections": {
            "any collection": {
                "attributes": {
                    "any attribute": {
                        "roles": [role_a, role_b]
                    }
                }
            }
        }
    },
}

class MockEntity():

    def __init__(self):
        self.a = "value a"
        self.b = "value b"
        self.c = "value c"


class MockRequest():
    pass

mock_request = MockRequest()

@patch("gobapi.auth_query.super", MagicMock)
class TestAuthorizedQuery(TestCase):

    def test_create(self):
        q = AuthorizedQuery()
        self.assertIsNone(q._authority)

    def test_set_catalog_collection(self):
        q = AuthorizedQuery()
        q.set_catalog_collection("any catalog", "any collection")
        self.assertEqual(q._authority._catalog, "any catalog")
        self.assertEqual(q._authority._collection, "any collection")
        self.assertEqual(q._authority._auth_scheme, GOB_AUTH_SCHEME)

    @patch("gobapi.auth_query.request", mock_request)
    def test_get_roles(self):
        q = AuthorizedQuery()
        q.set_catalog_collection('cat', 'col')

        mock_request.headers = {
            REQUEST_ROLES: "any roles"
        }
        roles = q._authority.get_roles()
        self.assertEqual(roles, ["any roles"])

        mock_request.headers = {}
        roles = q._authority.get_roles()
        self.assertEqual(roles, [])

        delattr(mock_request, 'headers')
        roles = q._authority.get_roles()
        self.assertEqual(roles, [])

    @patch("gobapi.auth_query.request", mock_request)
    @patch("gobapi.auth_query.GOB_AUTH_SCHEME", mock_scheme)
    def test_get_suppressed_columns(self):
        q = AuthorizedQuery()
        q.set_catalog_collection("any catalog", "any collection")
        q._authority.get_roles = lambda : [role_a]
        self.assertEqual(q._authority.get_suppressed_columns(), [])
        q._authority.get_roles = lambda : [role_b]
        self.assertEqual(q._authority.get_suppressed_columns(), [])
        q._authority.get_roles = lambda : [role_c]
        self.assertEqual(q._authority.get_suppressed_columns(), ['any attribute'])

        q._authority.get_roles = lambda : [role_a]

        q.set_catalog_collection("some other catalog", "any collection")
        self.assertEqual(q._authority.get_suppressed_columns(), [])

        q.set_catalog_collection("any catalog", "some other collection")
        self.assertEqual(q._authority.get_suppressed_columns(), [])

class TestAuthorizedQueryIter(TestCase):

    @patch("gobapi.auth_query.super")
    def test_iter(self, mock_super):
        mock_super.return_value = iter([MockEntity(), MockEntity()])
        q = AuthorizedQuery()
        q._authority = mock.MagicMock()
        q._authority.get_suppressed_columns = lambda: ["a", "b", "some other col"]
        for result in q:
            self.assertFalse(hasattr(result, "a"))
            self.assertFalse(hasattr(result, "b"))
            self.assertFalse(hasattr(result, "some other col"))
            self.assertTrue(hasattr(result, "c"))

        # Do not fail on set suppressed columns
        q.set_suppressed_columns(None, ["a"])


