from sqlalchemy.orm import Query
from flask import request

from gobcore.secure.user import User
from gobcore.model import GOBModel
from gobcore.typesystem import get_gob_type_from_info, gob_types, gob_secure_types

from gobcore.secure.config import REQUEST_ROLES
from gobapi.auth.schemes import GOB_AUTH_SCHEME

SUPPRESSED_COLUMNS = "_suppressed_columns"


class Authority():

    def __init__(self, catalog_name, collection_name):
        """
        An authority checks entities for columns that should not be communicated.

        The default authorization scheme is GOB_AUTH_SCHEME
        """
        self._catalog = catalog_name
        self._collection = collection_name
        collection = GOBModel().get_collection(self._catalog, self._collection)
        self._attributes = [attr for attr in collection['fields']] if collection else []
        self._auth_scheme = GOB_AUTH_SCHEME
        self._secured_columns = None
        self._suppressed_columns = None

    def get_roles(self):
        """
        Gets the user roles from the request headers
        """
        try:
            return [h for h in request.headers.get(REQUEST_ROLES, "").split(",") if h]
        except AttributeError:
            return []

    def get_checked_columns(self):
        """
        The checked columns are the columns that requires one or more roles
        """
        catalog_scheme = self._auth_scheme.get(self._catalog)
        if not catalog_scheme:
            return {}
        collection_scheme = catalog_scheme['collections'].get(self._collection)
        if not collection_scheme:
            return {}
        return collection_scheme.get('attributes', {})

    def allows_access(self):
        """
        Test if the request has access to the catalog/collection
        """
        catalog_scheme = self._auth_scheme.get(self._catalog, {})
        if self._allows_access(catalog_scheme):
            collection_scheme = catalog_scheme.get('collections', {}).get(self._collection, {})
            return self._allows_access(collection_scheme)
        return False

    def _allows_access(self, auth_schema):
        """
        Test if the request has access to the given authorisation scheme
        """
        return self._is_authorized_for(auth_schema) if auth_schema else True

    def _is_authorized_for(self, auth):
        roles = self.get_roles()
        auth_roles = auth.get('roles', [])
        return any([role for role in roles if role in auth_roles]) if auth_roles else True

    def get_suppressed_columns(self):
        """
        The suppressed columns are the columns that require a role that the user doesn't have
        """
        if not self._suppressed_columns:
            if self.allows_access():
                cols = [attr for attr, auth in self.get_checked_columns().items() if not self._is_authorized_for(auth)]
            else:
                cols = self._attributes
            self._suppressed_columns = cols
        return self._suppressed_columns

    def get_secured_columns(self):
        """
        The secured columns are the columns that (may) require decryption
        """
        if not self._secured_columns:
            collection = GOBModel().get_collection(self._catalog, self._collection)
            if collection:
                cols = {
                    column: {
                        'gob_type': get_gob_type_from_info(spec),
                        'spec': spec
                    }
                    for column, spec in collection['fields'].items()
                    if self.is_secure_type(spec)
                }
            else:
                cols = {}
            self._secured_columns = cols
        return self._secured_columns

    def filter_row(self, row, mapping=None):
        """
        Set all columns in the row that should be suppressed to None
        """
        if self.allows_access():
            """
            Note:    This is a best-effort solution. For regular REST and GraphQL traffic it is OK
                     For other API calls the logic might be inadequate
            Example: Someone calls the API for gebieden/stadsdelen/?view=enhanced_view. The enhanced view can be any
                     query, including renaming fields and joins with other tables.
                     Because the user has access only the suppressed columns will be removed from the result.
                     Access => suppress all columns on basis of their name
            """
            mapping = mapping or {}
            self._handle_suppressed_columns(mapping, row)
            self._handle_secured_columns(mapping, row)
        else:
            """
            Note:    if someone does not have access to a catalog/collection then the result should always be cleared.
            Example: Someone calls the API for gebieden/stadsdelen/?view=enhanced_view. The enhanced view can be any
                     query, including renaming fields and joins with other tables.
                     The most secure solution is to simply clear the row.
                     No access => no data
            """
            for key in row.keys():
                row[key] = None
        return row

    def _handle_secured_columns(self, mapping, row):
        """
        Handle secure columns by resolving their values.
        The exposed value is None if not authorized, else the decrypted value

        :param mapping: mapping between column names in the row and column names in the GOB model
        :param row: the row to process
        :return:
        """
        for column, info in self.get_secured_columns().items():
            column = mapping.get(column, column)
            try:
                row[column] = self.exposed_value(row[column], info)
            except (AttributeError, KeyError):
                pass

    def _handle_suppressed_columns(self, mapping, row):
        """
        Handle suppressed columns by removing their values

        :param mapping: mapping between column names in the row and column names in the GOB model
        :param row: the row to process
        :return:
        """
        for column in self.get_suppressed_columns():
            column = mapping.get(column, column)
            if column in row:
                row[column] = None

    @classmethod
    def exposed_value(cls, entity_value, info):
        """
        Get the exposed value for any encrypted entity value.

        :param entity_value:
        :param info: dictionary containing gob_type and type_spec
        :return: the decrypted value is the user is authorised, else None
        """
        if entity_value is None:
            return entity_value
        gob_type = info['gob_type']
        secure_type = gob_type.from_value_secure(entity_value, info['spec'])
        return cls.get_secured_value(secure_type)

    @classmethod
    def is_secure_type(cls, spec):
        """
        Tells if spec is a secure type
        Either a plain secure type or a JSON that contains a secure type

        :param spec:
        :return:
        """
        gob_type = get_gob_type_from_info(spec)
        if issubclass(gob_type, gob_secure_types.Secure):
            return True
        elif issubclass(gob_type, gob_types.JSON):
            attributes = {
                **spec.get('attributes', {}),
                **spec.get('secure', {})
            }
            return any([cls.is_secure_type(attr) for attr in attributes.values()])

    @classmethod
    def get_secure_type(cls, gob_type, spec, value):
        """
        Returns a Secure GOB type instance for the given type, spec and value

        Example:
        get_secure_type(GOB.SecureString, <<the gob model spec>>, <<any encrypted string value>>_
        returns a GOB.SecureString instance with the given value

        :param gob_type:
        :param spec:
        :param value:
        :return:
        """
        return gob_type.from_value_secure(value, spec)

    @classmethod
    def get_secured_value(cls, sec_type):
        """
        Create a user for his request and use this user to retrieve the value of the secure type
        """
        user = User(request)
        return sec_type.get_value(user)


class AuthorizedQuery(Query):

    def __init__(self, *args, **kwargs):
        """
        An authorized query checks every entity for columns that should not be communicated.
        """
        self._authority = None
        super().__init__(*args, **kwargs)

    def set_catalog_collection(self, catalog, collection):
        """
        Register the catalog and collection for the entities to be checked
        """
        self._authority = Authority(catalog, collection)

    def __iter__(self):
        """
        Iterator that yields entities for which the non-authorized columns have been cleared.
        An extra attribute is set on the entity that specifies the cleared columns
        """
        if self._authority:
            suppressed_columns = self._authority.get_suppressed_columns()
            secure_columns = self._authority.get_secured_columns()
        else:
            print("ERROR: UNAUTHORIZED ACCESS DETECTED")
            suppressed_columns = []
            secure_columns = {}

        for entity in super().__iter__():
            if isinstance(entity, tuple):
                self._suppress_columns(entity[0], suppressed_columns)
                self._handle_secure_columns(entity[0], secure_columns)
            else:
                self._suppress_columns(entity, suppressed_columns)
                self._handle_secure_columns(entity, secure_columns)
            yield entity

    def _handle_secure_columns(self, entity, secure_columns):
        for column, info in secure_columns.items():
            try:
                entity_value = getattr(entity, column)
                setattr(entity, column, Authority.exposed_value(entity_value, info))
            except AttributeError:
                pass

    def _suppress_columns(self, entity, suppressed_columns):
        self.set_suppressed_columns(entity, suppressed_columns)
        for column in [c for c in suppressed_columns if hasattr(entity, c)]:
            setattr(entity, column, None)

    def set_suppressed_columns(self, entity, suppressed_columns):
        try:
            # Register the suppressed columns with the entity
            setattr(entity, SUPPRESSED_COLUMNS, suppressed_columns)
        except AttributeError:
            pass
