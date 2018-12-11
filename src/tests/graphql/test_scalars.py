import datetime

from graphql.language import ast
from gobapi.graphql.scalars import Date


def test_date(monkeypatch):
    serialized = Date.serialize(datetime.date(2000, 1, 20))
    assert(serialized == "2000-01-20")

    class Literal(ast.StringValue):
        def __init__(self, value):
            self.value = value
    parsed_literal = Date.parse_literal(Literal("2000-01-20"))
    assert(str(parsed_literal) == str(datetime.date(2000, 1, 20)))

    parsed_literal = Date.parse_literal(Literal("null"))
    assert(parsed_literal == "null")

    parsed_literal = Date.parse_literal("non literal")
    assert(parsed_literal == None)  # value is not parsed