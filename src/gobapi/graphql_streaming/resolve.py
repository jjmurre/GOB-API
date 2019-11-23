from flask import request

from gobcore.model import GOBModel
from gobcore.secure.user import User
from gobcore.typesystem import GOB_SECURE_TYPES, get_gob_type

from gobapi.graphql_streaming.utils import to_snake

CATALOG_NAME = "_catalog"
COLLECTION_NAME = "_collection"

_SEC_TYPES = [f"GOB.{type.name}" for type in GOB_SECURE_TYPES]


class Resolver:

    def __init__(self, info):
        self.user = User(request)
        self.sec_attributes = {}

        catalog_name = info.get(CATALOG_NAME)
        collection_name = info.get(COLLECTION_NAME)
        if catalog_name and collection_name:
            collection = GOBModel().get_collection(catalog_name, collection_name)
            for attr in info.keys():
                attr_spec = collection['attributes'].get(to_snake(attr))
                if attr_spec and attr_spec['type'] in _SEC_TYPES:
                    self.sec_attributes[attr] = get_gob_type(attr_spec['type'])

    def resolve_row(self, row, result):
        for attr, value in row.items():
            gob_type = self.sec_attributes.get(attr)
            if gob_type:
                gob_value = gob_type.from_value(value)
                result[attr] = gob_value.get_value(self.user)
