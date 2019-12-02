from unittest import TestCase, mock
from unittest.mock import patch, MagicMock

from gobapi.auth import secure_route, public_route
from gobapi.auth import REQUEST_USER, REQUEST_ROLE


class TestAuth(TestCase):

    @patch('gobapi.auth.request')
    def test_secure_route(self, mock_request):
        func = lambda *args, **kwargs: "Any result"

        wrapped_func = secure_route("any rule", func)

        mock_request.headers = {}
        result = wrapped_func()
        self.assertEqual(result, (mock.ANY, 403))

        mock_request.headers = {
            REQUEST_USER: "any user"
        }
        result = wrapped_func()
        self.assertEqual(result, (mock.ANY, 403))

        mock_request.headers = {
            REQUEST_ROLE: "any role"
        }
        result = wrapped_func()
        self.assertEqual(result, (mock.ANY, 401))

        mock_request.headers = {
            REQUEST_USER: "any user",
            REQUEST_ROLE: "any role"
        }
        result = wrapped_func()
        self.assertEqual(result, "Any result")

    @patch('gobapi.auth.request')
    def test_public_route(self, mock_request):
        func = lambda *args, **kwargs: "Any result"

        wrapped_func = public_route("any rule", func)

        mock_request.headers = {}
        result = wrapped_func()
        self.assertEqual(result, "Any result")

        mock_request.headers = {
            REQUEST_USER: "any user"
        }
        result = wrapped_func()
        self.assertEqual(result, (mock.ANY, 403))

        mock_request.headers = {
            REQUEST_ROLE: "any role"
        }
        result = wrapped_func()
        self.assertEqual(result, (mock.ANY, 403))

        mock_request.headers = {
            REQUEST_USER: "any user",
            REQUEST_ROLE: "any role"
        }
        result = wrapped_func()
        self.assertEqual(result, (mock.ANY, 403))

    @patch('gobapi.auth.request')
    @patch('gobapi.auth._secure_headers_detected')
    def test_fraud_detected(self, mock_secure_headers, mock_request):
        func = lambda *args, **kwargs: "Any result"

        wrapped_func = public_route("any rule", func)

        mock_request.headers = {
            REQUEST_USER: "any user"
        }
        wrapped_func()
        mock_secure_headers.assert_called()
