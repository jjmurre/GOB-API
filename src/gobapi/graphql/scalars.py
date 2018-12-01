"""Custom scalars

Mainly used for handling null values

"""
import datetime

from graphene.types import Scalar
from graphql.language import ast

class Date(Scalar):
    @staticmethod
    def serialize(dt):
        return dt.isoformat()

    @staticmethod
    def parse_literal(node):
        if isinstance(node, ast.StringValue):
            return "null" if node.value == "null" else datetime.datetime.strptime(
                node.value, "%Y-%m-%dT%H:%M:%S.%f")

    @staticmethod
    def parse_value(value):
        return "null" if value == "null" else datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")
