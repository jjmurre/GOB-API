from sqlalchemy.orm import Query
from flask import request

from gobcore.secure.user import User

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
        self._auth_scheme = GOB_AUTH_SCHEME

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
        return collection_scheme['attributes']

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
        return [attr for attr, auth in self.get_checked_columns().items() if not self._is_authorized_for(auth)]

    def filter_row(self, row):
        """
        Set all columns in the row that should be suppressed to None
        """
        suppressed_columns = self.get_suppressed_columns()
        for column in [c for c in suppressed_columns if c in row]:
            row[column] = None
        return row

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
        else:
            print("ERROR: UNAUTHORIZED ACCESS DETECTED")
            suppressed_columns = []

        for entity in super().__iter__():
            if suppressed_columns:
                self.set_suppressed_columns(entity, suppressed_columns)
                for column in [c for c in suppressed_columns if hasattr(entity, c)]:
                    setattr(entity, column, None)
            yield entity

    def set_suppressed_columns(self, entity, suppressed_columns):
        try:
            # Register the suppressed columns with the entity
            setattr(entity, SUPPRESSED_COLUMNS, suppressed_columns)
        except AttributeError:
            pass
