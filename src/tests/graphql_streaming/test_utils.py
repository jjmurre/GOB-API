import unittest

from gobapi.graphql_streaming.utils import to_snake, to_camelcase


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
