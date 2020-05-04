import json

from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobapi.util.audit_log import get_log_handler, get_user_from_request, get_nested_item, DatabaseHandler


class TestFunctions(TestCase):

    @patch("gobapi.util.audit_log.DatabaseHandler")
    def test_get_log_handler(self, mock_database_handler):
        self.assertEqual(mock_database_handler.return_value, get_log_handler())

    @patch("gobapi.util.audit_log.get_client_ip", lambda r: r.ip)
    @patch("gobapi.util.audit_log.request")
    def test_get_user_from_request(self, mock_request):
        mock_request.headers = {
            'X-Auth-Email': 'the email',
            'X-Auth-Roles': ['a', 'b', 'c'],
            'X-Auth-Userid': 'the user'
        }

        self.assertEqual({
            'authenticated': True,
            'provider': 'Keycloak',
            'realm': '',
            'email': 'the email',
            'roles': ['a', 'b', 'c'],
            'ip': mock_request.ip
        }, get_user_from_request())

        mock_request.headers['X-Auth-Userid'] = None

        self.assertEqual({
            'authenticated': False,
            'provider': 'Keycloak',
            'realm': '',
            'email': 'the email',
            'roles': ['a', 'b', 'c'],
            'ip': mock_request.ip
        }, get_user_from_request())

    def test_get_nested_item(self):
        data = {
            'a': {
                'b': {
                    'c': 'the value',
                }
            }
        }

        self.assertEqual(data['a']['b']['c'], get_nested_item(data, 'a', 'b', 'c'))
        self.assertIsNone(get_nested_item(data, 'a', 'b', 'c', 'd'))
        self.assertIsNone(get_nested_item({}, 'a'))


class TestDatabaseHandler(TestCase):

    @patch("gobapi.util.audit_log.AuditLogger.get_instance")
    @patch("gobapi.util.audit_log.uuid.uuid4", lambda: 'SOME UUID')
    def test_emit(self, mock_get_instance):
        mocked_instance = mock_get_instance.return_value

        record = json.dumps({
            'audit': {
                'http_response': {
                    'the': 'response'
                },
                'http_request': {
                    'url': 'the url',
                },
                'user': {
                    'ip': 'the ip',
                },
                'a': 1,
                'b': 2,
            }
        })

        request_response_data = {
            'http_request': {
                'url': 'the url',
            },
            'user': {
                'ip': 'the ip',
            },
            'a': 1,
            'b': 2,
        }

        handler = DatabaseHandler()
        handler.format = lambda x: x
        handler.emit(record)

        mocked_instance.log_request.assert_called_with(
            source='the url',
            destination='the ip',
            extra_data=request_response_data,
            request_uuid='SOME UUID'
        )
        mocked_instance.log_response.assert_called_with(
            source='the url',
            destination='the ip',
            extra_data=request_response_data,
            request_uuid='SOME UUID'
        )

        # Except case
        handler.format = MagicMock(side_effect=json.JSONDecodeError)
        handler.emit(record)

        mocked_instance.log_request.assert_called_with(
            source='Could not get source from msg',
            destination='Could not get destination from msg',
            extra_data=None,
            request_uuid='SOME UUID'
        )

        mocked_instance.log_response.assert_called_with(
            source='Could not get source from msg',
            destination='Could not get destination from msg',
            extra_data=None,
            request_uuid='SOME UUID'
        )
