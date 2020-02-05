from unittest import TestCase, mock

from gobapi.fat_file import fat_file

class TestFatFile(TestCase):

    def test_args(self):
        result = fat_file('arg')
        self.assertEqual(result, ("", 204))

        result = fat_file(-1)
        self.assertEqual(result, ("", 204))

    @mock.patch("builtins.open")
    @mock.patch("gobapi.fat_file.send_file")
    @mock.patch("gobapi.fat_file.os")
    def test_gbs(self, mock_os, mock_send_file, mock_open):
        # mock_open.__iter__.return_value = mock.MagicMock()
        result = fat_file(1)
        mock_send_file.assert_called()
