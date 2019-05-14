"""Graphene filters

Filters provide for a way to dynamically filter collections on field values

"""
from graphene import Boolean
from graphene_sqlalchemy import SQLAlchemyConnectionField
from sqlalchemy import and_

from gobcore.model.metadata import FIELD

from gobapi.storage import get_session, filter_active
from gobapi import serialize


FILTER_ON_NULL_VALUE = "null"


def _build_query(query, model, relation, **kwargs):
    """Build a query to filter a model on the contents of kwargs

    :param query: the query to start with
    :param model: the model to filter
    :param kwargs: the filter arguments
    :return: the query to filter model on the filter arguments
    """
    # Skip the default GraphQL filters
    RELAY_ARGS = ['first', 'last', 'before', 'after', 'sort', 'active']

    for field, value in kwargs.items():
        if field not in RELAY_ARGS:
            # null is defined as a special string value because Python None or JSON null does not work
            if value == FILTER_ON_NULL_VALUE:
                query = query.filter(getattr(model, field) == None)  # noqa: E711
            else:
                query = query.filter(getattr(model, field) == value)
    if relation is not None:
        query = query.join(relation, and_(
            relation.c.dst_id == getattr(model, FIELD.ID),
            relation.c.dst_volgnummer == getattr(model, FIELD.SEQNR, None)))
    return query


class FilterConnectionField(SQLAlchemyConnectionField):

    def __init__(self, type, *args, **kwargs):
        kwargs.setdefault("active", Boolean(default_value=True))
        super(FilterConnectionField, self).__init__(type, *args, **kwargs)

    @classmethod
    def get_query(cls, model, info, relation=None, **kwargs):
        """Gets a query that returns the model filtered on the contents of kwargs

        :param model: the model class of the referenced collection
        :param info:
        :param relation:
        :param kwargs: the filter arguments, <name of field>: <value of field>
        :return: the query to filter model on the filter arguments
        """
        query = super(FilterConnectionField, cls).get_query(model, info, **kwargs)
        if kwargs.get('active'):
            query = filter_active(query, model)
        return _build_query(query, model, relation, **kwargs)


def get_resolve_secure_attribute(name, GOBType):
    """
    Gets a resolver for a secure attribute

    Secure attributes are serialized by a special secure serializer

    :param name: name of the secure attribute
    :param GOBType: the Secure GOBType class
    :return: a resolver function for secure attributes
    """
    def resolve_attribute(obj, info,  **kwargs):
        value = getattr(obj, name)
        return serialize.secure_value(GOBType(value))

    return resolve_attribute


def get_resolve_attribute(relation_table, model):
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
        session = get_session()
        # First get the relations for the specific object
        relation = session.query(relation_table).filter(
            relation_table.src_id == getattr(obj, FIELD.ID),
            relation_table.src_volgnummer == getattr(obj, FIELD.SEQNR))

        query = FilterConnectionField.get_query(model, info, relation.subquery(), **kwargs)
        return query.all()

    return resolve_attribute
