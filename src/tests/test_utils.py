from unittest import TestCase

from gobapi.utils import streaming_gob_response


class TestUtils(TestCase):

    def test_streaming_gob_response(self):
        # Success response. Return items from generator and append newline
        wrapped_generator = lambda: iter(range(10))
        f = streaming_gob_response(wrapped_generator)
        result = list(f())

        expected_result = list(range(10)) + ["\n"]
        self.assertEqual(expected_result, result)

    def test_streaming_gob_response_exception(self):
        # Error occurred. Show Aborted message.
        i = 0

        def wrapped_generator():
            if i == 0:
                yield "some item\n"

            raise Exception()

        expected_result = ["some item\n", "GOB_API_ERROR. Caught Exception. Response aborted. See logs.\n"]

        result = []
        with self.assertRaises(Exception):
            f = streaming_gob_response(wrapped_generator)

            for i in f():
                result.append(i)

        self.assertEqual(expected_result, result)
