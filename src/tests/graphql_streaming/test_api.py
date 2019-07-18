from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobapi.graphql_streaming.api import GraphQLStreamingApi


class TestGraphQLStreaming(TestCase):

    def setUp(self) -> None:
        self.api = GraphQLStreamingApi()

    def test_snake_to_camelcase(self):
        testcases = [
            ('some_snake_case', 'someSnakeCase'),
            ('_some_snake_case', '_someSnakeCase'),
            ('some_snake_case_', 'someSnakeCase_')
        ]

        for arg, expected_result in testcases:
            self.assertEqual(expected_result, self.api.snake_to_camelcase(arg))

    def test_dict_keys_to_camelcase(self):
        self.api.snake_to_camelcase = lambda x: 'cc_' + x
        arg = {
            'key1': 'value1',
            'key2': ['value2a', 'value2b'],
            'key3': [{'key3a': 'val3a'}, {'key3a': ['val3aa', 'val3ab', 'val3ac']}],
            'key4': {
                'key5': {
                    'key6': 'lalala'
                }
            }
        }

        expected = {
            'cc_key1': 'value1',
            'cc_key2': ['value2a', 'value2b'],
            'cc_key3': [{'cc_key3a': 'val3a'}, {'cc_key3a': ['val3aa', 'val3ab', 'val3ac']}],
            'cc_key4': {
                'cc_key5': {
                    'cc_key6': 'lalala'
                }
            }
        }
        self.assertEqual(expected, self.api.dict_keys_to_camelcase(arg))

    def test_transform_graphql_streaming_result(self):
        self.api.dict_keys_to_camelcase = MagicMock()
        arg = {
            'key1': {
                'key1a': 'val1a',
            },
            'key2': 'val2',
            '_key3': {
                'key3a': 'val3a'
            }
        }

        expected = {
            'key1': {
                'key1a': 'val1a',
            },
            'key2': 'val2',
            '_embedded': {
                'key3': {
                    'key3a': 'val3a'
                }
            }
        }
        self.api.dict_keys_to_camelcase.return_value = arg

        self.assertEqual(expected, self.api.transform_graphql_streaming_result(arg))
        self.api.dict_keys_to_camelcase.assert_called_with(arg)

    @patch("gobapi.graphql_streaming.api.request")
    @patch("gobapi.graphql_streaming.api.Response")
    @patch("gobapi.graphql_streaming.api.get_session")
    @patch("gobapi.graphql_streaming.api.GraphQL2SQL")
    @patch("gobapi.graphql_streaming.api.ndjson_entities")
    @patch("gobapi.graphql_streaming.api.text", lambda x: 'text_' + x)
    def test_entrypoint(self, mock_ndjson_entities, mock_graphql2sql, mock_get_session, mock_response, mock_request):
        mock_request.data.decode.return_value = '{"query": "some query"}'
        mock_graphql2sql.graphql2sql.return_value = 'parsed query'

        self.api.entrypoint()
        mock_graphql2sql.graphql2sql.assert_called_with("some query")
        mock_get_session.return_value.execute.assert_called_with('text_parsed query')

        mock_response.assert_called_with(mock_ndjson_entities.return_value, mimetype='application/x-ndjson')
