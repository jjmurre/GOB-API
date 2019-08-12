"""Graphene filters

Filters provide for a way to dynamically filter collections on field values

"""
from graphene import Boolean
from graphene_sqlalchemy import SQLAlchemyConnectionField
from sqlalchemy import tuple_

from gobcore.model.metadata import FIELD
from gobcore.sources import GOBSources
from gobcore.model import GOBModel

from gobapi.storage import filter_active, filter_deleted
from gobapi import serialize

from typing import List

gobsources = GOBSources()
gobmodel = GOBModel()

FILTER_ON_NULL_VALUE = "null"


class FilterConnectionField(SQLAlchemyConnectionField):

    def __init__(self, type, *args, **kwargs):
        kwargs.setdefault("active", Boolean(default_value=True))
        super(FilterConnectionField, self).__init__(type, *args, **kwargs)

    @classmethod
    def get_query(cls, model, info, **kwargs):
        """Gets a query that returns the model filtered on the contents of kwargs

        :param model: the model class of the referenced collection
        :param info:
        :param relation:
        :param kwargs: the filter arguments, <name of field>: <value of field>
        :return: the query to filter model on the filter arguments
        """
        query = super(FilterConnectionField, cls).get_query(model, info, **kwargs)
        # Exclude all records with date_deleted
        query = filter_deleted(query, model)
        if kwargs.get('active'):
            query = filter_active(query, model)
        return cls._build_query(query, model, **kwargs)

    @classmethod
    def _build_query(cls, query, model, **kwargs):
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
        return query


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


def get_resolve_attribute_missing_relation(field_name):
    """Gets a resolver for a reference that references a non-existing collection

    :param field_name:
    :return:
    """
    def resolve_attribute(obj, info, **kwargs):
        source_values = getattr(obj, field_name)

        return source_values

    return resolve_attribute


def add_bronwaardes_to_results(src_attribute, obj, results: list):
    """Adds bronwaardes to results from get_resolve_attribute.

    Fetches all bronwaardes from the original object and gets the destination_attribute from gobsources.
    Crossreferences bronwaardes from object with the values on destination_attribute in the results.

    The bronwaardes that are not matched with the results are added as new empty objects of the expected type with the
    bronwaarde attribute set.

    :param relation_table:
    :param model:
    :param obj:
    :param results:
    :return:
    """

    # Add bronwaarde to results
    for result in results:
        # For now set an empty bronwaarde on many references
        if isinstance(getattr(obj, src_attribute), list):
            setattr(result, 'bronwaarde', '')
        else:
            # Set the bronwaarde for single references
            setattr(result, 'bronwaarde', getattr(obj, src_attribute)['bronwaarde'])

    return results


def _extract_tuples(lst: List[dict], attrs: tuple):
    """Extracts tuples from list of dicts in the format defined by attrs.

    For example:
    _extract_tuples([{'a': 1, 'b': 2, 'c': 3}, {'a': 4, 'b': 5, 'c': 6}], ('a', 'c')) -> [(1, 3), (4, 6)]

    Ignores entries where one of the attributes is missing.

    :param lst:
    :param attrs:
    :return:
    """
    tuples = [tuple([item[t] for t in attrs if t in item]) for item in lst]
    return [t for t in tuples if len(t) == len(attrs)]  # Remove short tuples


def get_resolve_attribute(model, src_attribute_name):
    """Gets an attribute resolver

    An attribute resolver takes a get_session function, a model class and the name of a reference

    It returns a function that takes an object (obj) that contains a field (ref_name) that refers to the model
    It retrieves the value of obj[ref_name], which is a reference field
    The reference field is a dictionary that contains the id of the referenced column

    The collection of referenced objects is filtered on _id equal to the id of the referenced column

    Next, any other query filters are applied

    :param src_attribute_name: the attribute in the src model containing the references
    :param model: the model class of the collection that is referenced by the foreign key
    :return: a function that resolves the object to a list of referenced objects
    """

    def resolve_attribute(obj, info,  **kwargs):
        """Resolve attribute

        :param obj: the object that contains a reference field to another collection
        :param info: context info
        :param kwargs: any filter arguments, <name of field>: <value of field>
        :return: the list of referenced objects
        """
        bronwaardes = getattr(obj, src_attribute_name)

        if isinstance(bronwaardes, dict):
            bronwaardes = [bronwaardes]

        query = FilterConnectionField.get_query(model, info, **kwargs)
        if model.__has_states__:
            ids = _extract_tuples(bronwaardes, ('id', 'volgnummer'))
            query = query.filter(tuple_(getattr(model, FIELD.ID), getattr(model, FIELD.SEQNR)).in_(ids))
        else:
            ids = _extract_tuples(bronwaardes, ('id',))
            query = query.filter(getattr(model, FIELD.ID).in_(ids))

        return add_bronwaardes_to_results(src_attribute_name, obj, query.all())

    return resolve_attribute


def get_resolve_inverse_attribute(model, src_attribute_name, is_many_reference):
    """Gets an inverse attribute resolver

    :param src_attribute_name: The attribute name on the (original) source relation, the owner of the relation
    :param model: The owner of the relation
    :param is_many_reference:
    :return:
    """

    def resolve_attribute(obj, info, **kwargs):
        """Resolves inverse attribute

        :param obj: The originally referenced object. Now the base of the inverse relation.
        :param info:
        :param kwargs:
        :return:
        """
        query = FilterConnectionField.get_query(model, info, **kwargs)

        filter_args = {"id": getattr(obj, FIELD.ID)}

        if obj.__has_states__:
            filter_args['volgnummer'] = getattr(obj, FIELD.SEQNR)

        if is_many_reference:
            filter_args = [filter_args]

        query = query.filter(getattr(model, src_attribute_name).contains(filter_args))
        return query.all()
    return resolve_attribute
