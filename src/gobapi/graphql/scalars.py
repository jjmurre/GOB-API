"""Custom scalars

Mainly used for handling null values

"""
import datetime

from graphene.types import Scalar
from graphql.language import ast

from gobapi.graphql.filters import FILTER_ON_NULL_VALUE


class Date(Scalar):

    @staticmethod
    def serialize(dt):
        """Serialize a Date

        :param dt: Date
        :return: dt as a string in iso format
        """
        return dt.isoformat()

    @staticmethod
    def parse_literal(node):
        """Parse literal

        :param node: literal node
        :return: datetime.date if node is a string value
        """
        if isinstance(node, ast.StringValue):
            return Date.parse_value(node.value)

    @staticmethod
    def parse_value(value):
        """Parse a value into a Date

        The value is expected to be a string value

        "null" is treated as a special value and denotes a date value of null (or None)

        :param value: string value to parse
        :return: value as a datetime.date
        """
        DATE_FORMAT = "%Y-%m-%d"

        if value == FILTER_ON_NULL_VALUE:
            return FILTER_ON_NULL_VALUE
        else:
            return datetime.datetime.strptime(value, DATE_FORMAT).date()
