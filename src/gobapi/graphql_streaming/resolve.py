from gobcore.model import GOBModel
from gobcore.typesystem import GOB_SECURE_TYPES, get_gob_type

from gobapi.graphql_streaming.utils import to_snake, to_camelcase
from gobapi.auth.auth_query import Authority

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

        if (catalog_name and collection_name):
            collection = GOBModel().get_collection(catalog_name, collection_name)
            attributes = collection['attributes'].keys()
        else:
            attributes = {}

        self._attributes[catalog_name][collection_name] =\
            {to_camelcase(key): value for key, value in
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
        catalog_name = row.get(CATALOG_NAME)
        collection_name = row.get(COLLECTION_NAME)
        self._init_catalog_collection(catalog_name, collection_name)

        # Filter row and result for columns that do not match with the roles of the current request
        authority = Authority(catalog_name, collection_name)
        authority.filter_row(row)
        authority.filter_row(result)

        for attr, value in row.items():
            gob_type = self._attributes[catalog_name][collection_name].get(attr)
            if gob_type:
                gob_value = gob_type.from_value(value)
                result[attr] = authority.get_secured_value(gob_value)

        for attr in [name for name in [CATALOG_NAME, COLLECTION_NAME] if name in row]:
            # Once a row has been resolved, don't resolve it twice
            del row[attr]
