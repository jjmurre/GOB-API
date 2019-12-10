import unittest
from unittest import mock

import datetime

from gobapi.serialize import secure_value, datetime_value

class TestSerialize(unittest.TestCase):

    @mock.patch('gobapi.serialize.Authority')
    def test_secure_value(self, mock_authority_class):
        mock_authority = mock.MagicMock()
        mock_authority_class.return_value = mock_authority

        sec_value = "Any secure value"

        mock_authority.get_secured_value.return_value = None
        self.assertEqual(secure_value(sec_value), None)

        mock_authority.get_secured_value.return_value = "any value"
        self.assertEqual(secure_value(sec_value), "any value")

        now = datetime.datetime.now()
        mock_authority.get_secured_value.return_value = now
        self.assertEqual(secure_value(sec_value), datetime_value(now))
