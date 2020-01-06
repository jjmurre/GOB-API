from unittest import TestCase

from gobapi.dump.csv_stream import CSVStream


class TestCSVStream(TestCase):

    def test_init(self):
        stream = CSVStream(iter([]), 1)
        self.assertIsNotNone(stream)

    def test_empty(self):
        stream = CSVStream(iter([]), 1)
        self.assertFalse(stream.has_items())

    def test_only_header_no_items(self):
        stream = CSVStream(iter(["a"]), 1)
        self.assertFalse(stream.has_items())
        result = stream.read()
        self.assertEqual(result, "a")

    def test_one_line(self):
        stream = CSVStream(iter(["a", "b"]), 1)
        self.assertTrue(stream.has_items())
        result = stream.read()
        self.assertEqual(result, "ab")

    def test_max_read(self):
        stream = CSVStream(iter(["a", "b", "c"]), 1)
        self.assertTrue(stream.has_items())
        result = stream.read()
        self.assertEqual(result, "ab")
        stream.reset_count()
        result = stream.read()
        self.assertEqual(result, "ac")

    def test_readline(self):
        stream = CSVStream(iter([]), 1)
        with self.assertRaises(NotImplementedError):
            stream.readline()


