from gobcore.model import GOBModel
from gobcore.typesystem import GOB_SECURE_TYPES

from gobapi.utils import to_camelcase
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
        Initialize the attributes mapping for the given catalog and collection

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
            attributes = []

        self._attributes[catalog_name][collection_name] = {
            attr: to_camelcase(attr) for attr in attributes
        }

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
        authority.filter_row(row, mapping=self._attributes[catalog_name][collection_name])
        authority.filter_row(result, mapping=self._attributes[catalog_name][collection_name])

        for attr in [name for name in [CATALOG_NAME, COLLECTION_NAME] if name in row]:
            # Once a row has been resolved, don't resolve it twice
            del row[attr]
