from flask import request

from gobcore.model import GOBModel
from gobcore.secure.user import User
from gobcore.typesystem import GOB_SECURE_TYPES, get_gob_type

from gobapi.graphql_streaming.utils import to_snake, to_camelcase

CATALOG_NAME = "_catalog"
COLLECTION_NAME = "_collection"

_SEC_TYPES = [f"GOB.{type.name}" for type in GOB_SECURE_TYPES]


class Resolver:

    def __init__(self):
        """
        Initialize a resolver

        Get the GOB types for all attributes that need to be resolved
        Currently only secure attributes are resolved

        :param info:
        """
        self._user = User(request)
        self._attributes = {}

    def _init_catalog_collection(self, catalog_name, collection_name):
        """
        Initialize the attributes to be resolved for the given catalog and collection

        :param catalog_name:
        :param collection_name:
        :return:
        """
        self._attributes[catalog_name] = self._attributes.get(catalog_name) or {}
        if self._attributes[catalog_name].get(collection_name) is not None:
            # Already initialized
            return

        collection = GOBModel().get_collection(catalog_name, collection_name)
        attributes = collection['attributes'].keys()
        self._attributes[catalog_name][collection_name] = {to_camelcase(key): value for key, value in
                            {attr: self._resolve_type(collection, attr) for attr in attributes}.items() if value}


    def _resolve_type(self, collection, attr):
        """
        Get the GOB type to resolve the given attr of the give collection

        :param collection:
        :param attr:
        :return:
        """
        attr_spec = collection['attributes'].get(to_snake(attr))
        if attr_spec and attr_spec['type'] in _SEC_TYPES:
            # Only resolve secure types
            return get_gob_type(attr_spec['type'])

    def resolve_row(self, row, result):
        """
        Resolve all values in the row
        Update the resolved value in the result

        :param row:
        :param result:
        :return:
        """
        catalog_name = row[CATALOG_NAME]
        collection_name = row[COLLECTION_NAME]
        self._init_catalog_collection(catalog_name, collection_name)
        for attr, value in row.items():
            gob_type = self._attributes[catalog_name][collection_name].get(attr)
            if gob_type:
                gob_value = gob_type.from_value(value)
                result[attr] = gob_value.get_value(self._user)
