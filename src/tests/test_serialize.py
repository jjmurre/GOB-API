import unittest
from unittest import mock

import datetime

from gobapi.serialize import secure_value, datetime_value

class TestSerialize(unittest.TestCase):

    def test_secure_value(self):
        sec_value = mock.MagicMock()

        sec_value.get_value.return_value = None
        self.assertEqual(secure_value(sec_value), None)

        sec_value.get_value.return_value = "any value"
        self.assertEqual(secure_value(sec_value), "any value")

        now = datetime.datetime.now()
        sec_value.get_value.return_value = now
        self.assertEqual(secure_value(sec_value), datetime_value(now))
