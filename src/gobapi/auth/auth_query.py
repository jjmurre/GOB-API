from sqlalchemy.orm import Query
from flask import request

from gobcore.secure.user import User

from gobapi.auth.config import REQUEST_ROLES
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

    def get_suppressed_columns(self):
        """
        The suppressed columns are the columns that require a role that the user doesn't have
        """
        roles = self.get_roles()
        return [attr for attr, auth in self.get_checked_columns().items()
                if not any([role for role in roles if role in auth.get('roles', [])])]

    def filter_row(self, row):
        suppressed_columns = self.get_suppressed_columns()
        if suppressed_columns:
            for column in suppressed_columns:
                try:
                    del row[column]
                except KeyError:
                    pass
        return row

    def get_secured_value(self, sec_type):
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

        Unfortunately the delattr method does not work for query results.
        After delattr(), hasattr() still returns true
        However, executing delattr() twice results in an error
        """
        if self._authority:
            suppressed_columns = self._authority.get_suppressed_columns()
        else:
            print("UNAUTHORIZED ACCESS DETECTED")
            suppressed_columns = []

        for entity in super().__iter__():
            if suppressed_columns:
                self.set_suppressed_columns(entity, suppressed_columns)
                for column in suppressed_columns:
                    self.suppress_attribute(column, entity)
            yield entity

    def suppress_attribute(self, column, entity):
        try:
            # For query results, delattr sets the entity value to null
            delattr(entity, column)
        except AttributeError:
            pass

    def set_suppressed_columns(self, entity, suppressed_columns):
        try:
            setattr(entity, SUPPRESSED_COLUMNS, suppressed_columns)
        except AttributeError:
            pass
