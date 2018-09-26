import importlib
from unittest import mock, TestCase
from unittest.mock import MagicMock

from tests import fixtures
from gobapicore.model import get_catalog_names, get_catalog, get_collections, get_collection


class TestConfig(TestCase):

    @mock.patch('gobcore.model.GOBModel')
    def test_catalogs(self, mock_model_class):
        # Prepare GOBModel
        mock_model = MagicMock()
        mock_model_class.return_value = mock_model

        expected = fixtures.random_array()
        mock_model.get_model_names.return_value = expected

        # Reload for this test
        from gobapicore import model
        importlib.reload(model)

        catalogs = model.get_catalogs()

        # Assert collections is based on GOBModel
        mock_model.get_model_names.assert_called()
        self.assertEqual(expected, catalogs['meetbouten']['collections'])

    @mock.patch('gobapicore.model.get_catalogs')
    def test_catalog_names(self, mock_catalogs):
        # setup fixtures
        name = fixtures.random_string()
        other_name = fixtures.random_string()
        catalog = fixtures.random_dict()
        collections = fixtures.random_array()
        collection = fixtures.random_array()
        collections.append(collection)
        catalog['collections'] = collections

        # setup mock
        mock_catalogs.return_value = {name: catalog}

        # name should be in catalog_names
        self.assertIn(name, get_catalog_names())

        # catalog should be returned, or None if no catalog by that name
        self.assertIsNone(get_catalog(other_name))
        self.assertEqual(catalog, get_catalog(name))

        # collections should be returned, or None if no catalog by that name
        self.assertIsNone(get_collections(other_name))
        self.assertEqual(collections, get_collections(name))

        # collection should be returned, or None if no catalog by that name, or no collection by that name
        self.assertIsNone(get_collection(other_name, collection))
        self.assertIsNone(get_collection(name, fixtures.random_string()))
        self.assertEqual(collection, get_collection(name, collection))