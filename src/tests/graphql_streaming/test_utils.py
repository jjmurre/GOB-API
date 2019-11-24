import unittest

from gobapi.graphql_streaming.utils import to_snake


class TestUtils(unittest.TestCase):

    def test_to_snake(self):
        self.assertEqual(to_snake(""), "")
        self.assertEqual(to_snake("a"), "a")
        self.assertEqual(to_snake("A"), "_a")
        self.assertEqual(to_snake("ABC"), "_a_b_c")

        self.assertEqual(to_snake("firstSecond"), "first_second")