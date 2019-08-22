"""Graphene filters

Filters provide for a way to dynamically filter collections on field values

"""
from graphene import Boolean
from graphene_sqlalchemy import SQLAlchemyConnectionField
from sqlalchemy import tuple_

from gobcore.model.metadata import FIELD
from gobcore.model import GOBModel
from gobcore.model.sa.gob import models

from gobapi.storage import filter_active, filter_deleted
from gobapi import serialize

from typing import List

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

    def resolve_attribute(obj, info, **kwargs):
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


def get_source_values_filter(result):
    """Returns filters to find result out of a list of source_values

    :param result:
    :return:
    """
    filters = [
        lambda item: FIELD.REFERENCE_ID in item and item[FIELD.REFERENCE_ID] == getattr(result, FIELD.ID),
    ]

    if result.__has_states__:
        filters.append(lambda item: FIELD.SEQNR in item and item[FIELD.SEQNR] == getattr(result, FIELD.SEQNR))

    return filters


def set_result_values(result, source_item: dict):
    """Adds bronwaarde and broninfo attributes to result, from source_item.

    :param result:
    :param source_item:
    :return:
    """
    reference_root_fields = [FIELD.REFERENCE_ID, FIELD.SEQNR, FIELD.SOURCE_VALUE]

    setattr(result, FIELD.SOURCE_VALUE, source_item[FIELD.SOURCE_VALUE])
    setattr(result, FIELD.SOURCE_INFO, {
        k: v for k, v in source_item.items() if k not in reference_root_fields
    })


def create_bronwaarde_result_objects(source_values: list, model):
    """Creates empty result objects of type model for the list of source_values

    :param source_values:
    :param model:
    :return:
    """
    result = []
    expected_type = models[model.__tablename__]

    for source_value in source_values:
        source_value_result = expected_type()
        set_result_values(source_value_result, source_value)
        result.append(source_value_result)
    return result


def add_bronwaardes_to_results(src_attribute, model, obj, results: list):
    """Adds bronwaardes to results from get_resolve_attribute.


    The bronwaardes that are not matched with the results are added as new empty objects of the expected type with the
    bronwaarde attribute set.

    :param relation_table:
    :param model:
    :param obj:
    :param results:
    :return:
    """
    source_values = getattr(obj, src_attribute)
    return_results = []

    # Add bronwaarde to results
    for result in results:
        if isinstance(source_values, list):

            # To find the item in source_values belonging to this result
            filters = get_source_values_filter(result)
            source_item_index = [idx for idx, item in enumerate(source_values) if all([f(item) for f in filters])]

            if source_item_index:
                source_item = source_values[source_item_index[0]]
                set_result_values(result, source_item)
                del source_values[source_item_index[0]]

        else:
            # Set the bronwaarde for single references
            set_result_values(result, getattr(obj, src_attribute))

        return_results.append(result)

    if isinstance(source_values, list):
        # Add bronwaardes with missing relations
        return_results.extend(create_bronwaarde_result_objects(source_values, model))

    return return_results


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

    def resolve_attribute(obj, info, **kwargs):
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

        return add_bronwaardes_to_results(src_attribute_name, model, obj, query.all())

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
