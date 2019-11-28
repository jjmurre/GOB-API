import unittest
from unittest import mock

from gobapi.graphql_streaming.resolve import Resolver

class TestResolve(unittest.TestCase):

    def testResolver(self):
        resolver = Resolver()
        self.assertIsNotNone(resolver)
        self.assertIsNotNone(resolver._user)
        self.assertEqual(resolver._attributes, {})

    @mock.patch('gobapi.graphql_streaming.resolve.GOBModel')
    @mock.patch('gobapi.graphql_streaming.resolve.get_gob_type')
    @mock.patch('gobapi.graphql_streaming.resolve._SEC_TYPES', ['GOB.SecureString', 'GOB.SecureDateTime'])
    def testResolverWithAttributes(self, mock_get_gob_type, mock_model_class):
        mock_model = mock.MagicMock()
        mock_model_class.return_value = mock_model

        mock_gob_type = mock.MagicMock()
        mock_get_gob_type.return_value = mock_gob_type

        collection = {
            'attributes': {
                'a_b': {
                    'type': 'GOB.SecureString'
                },
                'c_d': {
                    'type': 'GOB.SecureDateTime'
                },
                'e_f': {
                    'type': 'some type'
                }
            }
        }

        mock_model.get_collection.return_value = collection

        resolver = Resolver()

        mock_gob_value = mock.MagicMock()
        mock_gob_value.get_value = lambda u: "resolved value"
        mock_gob_type.from_value.return_value = mock_gob_value
        row = {
            '_catalog': 'cat',
            '_collection': 'col',
            'aB': 'aB value',
            'cD': 'cD value',
            'eF': 'eF value',
            'attr': 'value'
        }
        result = {}
        resolver.resolve_row(row, result)
        mock_model.get_collection.assert_called_with('cat', 'col')
        self.assertEqual(resolver._attributes, {
            'cat': {
                'col': {'aB': mock.ANY, 'cD': mock.ANY}
            }
        })
        self.assertEqual(mock_get_gob_type.call_count, 2)
        self.assertEqual(result, {'aB': 'resolved value', 'cD': 'resolved value'})
