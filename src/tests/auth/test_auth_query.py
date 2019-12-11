from unittest import TestCase, mock
from unittest.mock import patch, MagicMock

from gobapi.auth.auth_query import Authority, AuthorizedQuery, GOB_AUTH_SCHEME, REQUEST_ROLES

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
    "secure catalog": {
        "roles": [role_a, role_b]
    },
    "secure catalog collection": {
        "collections": {
            "secure collection": {
                "roles": [role_a, role_b]
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

@patch("gobapi.auth.auth_query.super", MagicMock)
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

    @patch("gobapi.auth.auth_query.request", mock_request)
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

    @patch("gobapi.auth.auth_query.request", mock_request)
    @patch("gobapi.auth.auth_query.GOB_AUTH_SCHEME", mock_scheme)
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

    @patch("gobapi.auth.auth_query.super")
    def test_iter(self, mock_super):
        mock_super.return_value = iter([MockEntity(), MockEntity()])
        q = AuthorizedQuery()
        q._authority = mock.MagicMock()
        q._authority.get_suppressed_columns = lambda: ["a", "b", "some other col"]
        for result in q:
            self.assertIsNone(result.a)
            self.assertIsNone(result.b)
            self.assertFalse(hasattr(result, "some other col"))
            self.assertIsNotNone(result.c)

        # Do not fail on set suppressed columns
        q.set_suppressed_columns(None, ["a"])

    @patch("gobapi.auth.auth_query.super")
    def test_iter_unauthorized(self, mock_super):
        mock_super.return_value = iter([MockEntity(), MockEntity()])
        q = AuthorizedQuery()
        for result in q:
            for attr in ["a", "b", "c"]:
                self.assertTrue(hasattr(result, attr))


class TestAuthority(TestCase):

    def test_create(self):
        authority = Authority('cat', 'col')
        self.assertEqual(authority._catalog, 'cat')
        self.assertEqual(authority._collection, 'col')
        self.assertEqual(authority._auth_scheme, GOB_AUTH_SCHEME)

    @patch("gobapi.auth.auth_query.request", mock_request)
    def test_filter_row(self):
        authority = Authority('cat', 'col')
        authority.get_suppressed_columns = lambda: ['b', 'd']
        row = {'a': 1, 'b': 2, 'c': 3}
        authority.filter_row(row)
        self.assertEqual(row, {'a': 1, 'b': None, 'c': 3})

    @patch("gobapi.auth.auth_query.request")
    @patch("gobapi.auth.auth_query.User")
    def test_secured_value(self, mock_user, mock_request):
        authority = Authority('cat', 'col')
        mock_user.return_value = "any user"
        mock_secure_type = mock.MagicMock()
        result = authority.get_secured_value(mock_secure_type)
        mock_user.assert_called_with(mock_request)
        mock_secure_type.get_value.assert_called_with("any user")

    @patch("gobapi.auth.auth_query.request", mock_request)
    @patch("gobapi.auth.auth_query.GOB_AUTH_SCHEME", mock_scheme)
    def test_allows_access(self):
        authority = Authority('secure catalog', 'any col')
        authority.get_roles = lambda : []
        self.assertFalse(authority.allows_access())

        authority.get_roles = lambda : [role_b]
        self.assertTrue(authority.allows_access())

        authority._catalog = "secure catalog collection"
        authority._collection = "secure collection"
        authority.get_roles = lambda : []
        self.assertFalse(authority.allows_access())

        authority.get_roles = lambda : [role_b]
        self.assertTrue(authority.allows_access())

        authority._collection = "any collection"
        authority.get_roles = lambda : []
        self.assertTrue(authority.allows_access())
