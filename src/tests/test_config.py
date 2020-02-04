from unittest import TestCase
from unittest.mock import patch
from gobapi.config import current_api_base_path
from gobcore.exceptions import GOBException


class TestConfig(TestCase):

    def test_current_api_base_path(self):
        mock_request = type('MockRequest', (object,), {'gob_base_path': '/base/path'})

        with patch("gobapi.config.request", mock_request):
            self.assertEqual('/base/path', current_api_base_path())

        mock_request = type('MockRequest', (object,), {'path': '/something/not/matching'})

        with patch("gobapi.config.request", mock_request), \
             self.assertRaisesRegex(GOBException, 'Could not determine base path'):
            current_api_base_path()

        mock_request = type('MockRequest', (object,), {'path': '/gob/secure/endpoint'})

        with patch("gobapi.config.request", mock_request):
            self.assertEqual('/gob/secure', current_api_base_path())

        mock_request = type('MockRequest', (object,), {'path': '/gob/endpoint'})

        with patch("gobapi.config.request", mock_request):
            self.assertEqual('/gob', current_api_base_path())
