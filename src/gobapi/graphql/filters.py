"""Graphene filters

Filters provide for a way to dynamically filter collections on field values

"""
from graphene_sqlalchemy import SQLAlchemyConnectionField

FILTER_ON_NULL_VALUE = "null"


def _build_query(query, model, **kwargs):
    """Build a query to filter a model on the contents of kwargs

    :param query: the query to start with
    :param model: the model to filter
    :param kwargs: the filter arguments
    :return: the query to filter model on the filter arguments
    """
    # Skip the default GraphQL filters
    RELAY_ARGS = ['first', 'last', 'before', 'after', 'sort']

    for field, value in kwargs.items():
        if field not in RELAY_ARGS:
            # null is defined as a special string value because Python None or JSON null does not work
            if value == FILTER_ON_NULL_VALUE:
                query = query.filter(getattr(model, field) == None)  # noqa: E711
            else:
                query = query.filter(getattr(model, field) == value)
    return query


class FilterConnectionField(SQLAlchemyConnectionField):

    @classmethod
    def get_query(cls, model, info, **kwargs):
        """Gets a query that returns the model filtered on the contents of kwargs

        :param model: the model class of the referenced collection
        :param info:
        :param kwargs: the filter arguments, <name of field>: <value of field>
        :return: the query to filter model on the filter arguments
        """
        query = super(FilterConnectionField, cls).get_query(model, info, **kwargs)
        return _build_query(query, model, **kwargs)


def get_resolve_attribute(model, ref_name):
    """Gets an attribute resolver

    An attribute resolver takes a get_session function, a model class and the name of a reference

    It returns a function that takes an object (obj) that contains a field (ref_name) that refers to the model
    It retrieves the value of obj[ref_name], which is a reference field
    The reference field is a dictionary that contains the id of the referenced column

    The collection of referenced objects is filtered on _id equal to the id of the referenced column

    Next, any other query filters are applied

    :param model: the model class of the collection that is referenced by the foreign key
    :param ref_name: the name of the field that contains the foreign key
    :return: a function that resolves the object to a list of referenced objects
    """

    def resolve_attribute(obj, info,  **kwargs):
        """Resolve attribute

        :param obj: the object that contains a reference field to another collection
        :param info: context info
        :param kwargs: any filter arguments, <name of field>: <value of field>
        :return: the list of referenced objects
        """
        try:
            query_args = {
                "_id": getattr(obj, ref_name)["id"],  # Filter the model on the foreign key
                **kwargs  # Add other filter arguments (_id need not to be unique for collections with states)
            }
        except KeyError:
            return []

        query = FilterConnectionField.get_query(model, info, **query_args)
        return query.all()

    return resolve_attribute
