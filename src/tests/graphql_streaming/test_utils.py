import unittest
from unittest import mock

from gobapi.graphql_streaming.utils import to_snake, to_camelcase, resolve_schema_collection_name


class TestUtils(unittest.TestCase):

    def test_to_snake(self):
        self.assertEqual(to_snake(""), "")
        self.assertEqual(to_snake("a"), "a")
        self.assertEqual(to_snake("A"), "_a")
        self.assertEqual(to_snake("ABC"), "_a_b_c")

        self.assertEqual(to_snake("firstSecond"), "first_second")
        
    def test_to_camelcase(self):
        self.assertEqual(to_camelcase(""), "")
        self.assertEqual(to_camelcase("a"), "a")
        self.assertEqual(to_camelcase("_a"), "_a")
        self.assertEqual(to_camelcase("_a_b_c"), "_aBC")

        self.assertEqual(to_camelcase("first_second"), "firstSecond")

    @mock.patch('gobapi.graphql_streaming.utils.GOBModel')
    def test_resolve_schema_collection_name(self, mock_model_class):
        mock_model = mock.MagicMock()
        mock_model_class.return_value = mock_model

        mock_model.get_catalog = lambda cat: "catalog" if cat == "catalog" else None
        mock_model.get_collection = lambda cat, col: "collection" if col == "collection" else None
        result = resolve_schema_collection_name("catalogCollection")
        self.assertEqual(result, ('catalog', 'collection'))

        mock_model.get_catalog = lambda cat: None
        result = resolve_schema_collection_name("catalogCollection")
        self.assertEqual(result, (None, None))

        mock_model.get_catalog = lambda cat: "catalog" if cat == "catalog_ext" else None
        mock_model.get_collection = lambda cat, col: "collection" if col == "collection" else None
        result = resolve_schema_collection_name("catalogExtCollection")
        self.assertEqual(result, ('catalog_ext', 'collection'))

        mock_model.get_catalog = lambda cat: "catalog" if cat == "catalog" else None
        mock_model.get_collection = lambda cat, col: "collection" if col == "ext_collection" else None
        result = resolve_schema_collection_name("catalogExtCollection")
        self.assertEqual(result, ('catalog', 'ext_collection'))

