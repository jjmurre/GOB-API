"""Custom scalars

Mainly used for handling null values

"""
import datetime
import json

import geoalchemy2
from graphene.types import Scalar
from graphql.language import ast

from gobapi.graphql.filters import FILTER_ON_NULL_VALUE
from gobapi.storage import get_session


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


class DateTime(Scalar):

    @staticmethod
    def serialize(dt):
        """Serialize a DateTime

        :param dt: DateTime
        :return: dt as a string in iso format
        """
        # Transform to internal string format and work around issue: https://bugs.python.org/issue13305
        return f"{dt.year:04d}-" + dt.strftime("%m-%dT%H:%M:%S.%f").replace('.000000', '')

    @staticmethod
    def parse_literal(node):
        """Parse literal

        :param node: literal node
        :return: datetime.date if node is a string value
        """
        if isinstance(node, ast.StringValue):
            return DateTime.parse_value(node.value)

    @staticmethod
    def parse_value(value):
        """Parse a value into a DateTime

        The value is expected to be a string value

        "null" is treated as a special value and denotes a date value of null (or None)

        :param value: string value to parse
        :return: value as a datetime.datetime
        """
        DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

        if value == FILTER_ON_NULL_VALUE:
            return FILTER_ON_NULL_VALUE
        else:
            return datetime.datetime.strptime(value, DATE_FORMAT)


class GeoJSON(Scalar):

    @staticmethod
    def serialize(geom):
        """Serialize a GeoJSON string to a dict

        The geojson string is serialized to a dict to prevent graphql to output
        the geojson as an escaped string

        :param geom: geom
        :return: geometry as dict
        """
        session = get_session()
        return json.loads(session.scalar(geom.ST_AsGeoJSON()))

    @staticmethod
    def parse_literal(node):
        """Parse literal

        :param node: literal node
        :return: datetime.date if node is a string value
        """
        if isinstance(node, ast.StringValue):
            return GeoJSON.parse_value(node.value)

    @staticmethod
    def parse_value(value):
        """Parse a value into a Geometry object

        :param value: string value to parse
        :return: value as a Geometry object
        """
        session = get_session()
        if value == FILTER_ON_NULL_VALUE:
            return FILTER_ON_NULL_VALUE
        else:
            return session.scalar(geoalchemy2.func.ST_GeomFromText(value))
