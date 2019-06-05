"""Graphene filters

Filters provide for a way to dynamically filter collections on field values

"""
from graphene import Boolean
from graphene_sqlalchemy import SQLAlchemyConnectionField
from sqlalchemy import and_

from gobcore.model.metadata import FIELD
from gobcore.model.relations import get_reference_name_from_relation_table_name
from gobcore.sources import GOBSources
from gobcore.model import GOBModel
from gobcore.model.sa.gob import models

from gobapi.storage import get_session, filter_active, filter_deleted
from gobapi import serialize

gobsources = GOBSources()
gobmodel = GOBModel()

FILTER_ON_NULL_VALUE = "null"


def _add_query_filter_kwargs(query, model, **kwargs):
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


def _build_query(query, model, relation, **kwargs):
    """Build a query to filter a model on the contents of kwargs

    :param query: the query to start with
    :param model: the model to filter
    :param kwargs: the filter arguments
    :return: the query to filter model on the filter arguments
    """
    query = _add_query_filter_kwargs(query, model, **kwargs)

    if relation is not None:
        join_condition = relation.c.dst_id == getattr(model, FIELD.ID)

        if model.__has_states__:
            join_condition = and_(
                join_condition,
                relation.c.dst_volgnummer == getattr(model, FIELD.SEQNR, None)
            )
        query = query.join(relation, join_condition)
    return query


def _build_query_inverse(query, model, relation, **kwargs):
    """Build a query to filter a model on the contents of kwargs

    :param query: the query to start with
    :param model: the model to filter
    :param kwargs: the filter arguments
    :return: the query to filter model on the filter arguments
    """
    query = _add_query_filter_kwargs(query, model, **kwargs)

    if relation is not None:
        join_condition = relation.c.src_id == getattr(model, FIELD.ID)

        if model.__has_states__:
            join_condition = and_(
                join_condition,
                relation.c.src_volgnummer == getattr(model, FIELD.SEQNR, None)
            )
        query = query.join(relation, join_condition)
    return query


class BaseFilterConnectionField(SQLAlchemyConnectionField):
    build_query_fn = None

    def __init__(self, type, *args, **kwargs):
        kwargs.setdefault("active", Boolean(default_value=True))
        super(BaseFilterConnectionField, self).__init__(type, *args, **kwargs)

    @classmethod
    def get_query(cls, model, info, relation=None, **kwargs):
        """Gets a query that returns the model filtered on the contents of kwargs

        :param model: the model class of the referenced collection
        :param info:
        :param relation:
        :param kwargs: the filter arguments, <name of field>: <value of field>
        :return: the query to filter model on the filter arguments
        """
        query = super(BaseFilterConnectionField, cls).get_query(model, info, **kwargs)
        # Exclude all records with date_deleted
        query = filter_deleted(query, model)
        if kwargs.get('active'):
            query = filter_active(query, model)
        return cls.build_query_fn(query, model, relation, **kwargs)


class FilterConnectionField(BaseFilterConnectionField):
    build_query_fn = _build_query


class FilterInverseConnectionField(BaseFilterConnectionField):
    build_query_fn = _build_query_inverse


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


def add_bronwaardes_to_results(relation_table, model, obj, results: list):
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
    catalog = gobmodel.get_catalog_from_table_name(obj.__tablename__)
    collection = gobmodel.get_collection_from_table_name(obj.__tablename__)
    reference_name = get_reference_name_from_relation_table_name(relation_table.__tablename__)

    source_values = getattr(obj, reference_name)

    if isinstance(source_values, dict):
        source_values = [source_values]
    source_values = [item['bronwaarde'] for item in source_values] if source_values else []

    # Filtering should always return exactly one source (otherwise this resolver would not have been generated)
    source = [item for item in gobsources._relations[catalog][collection] if item['field_name'] == reference_name]
    destination_attribute = source[0]['destination_attribute']

    # Add bronwaarde to results
    return_results = []
    for result in results:
        source_value = getattr(result, destination_attribute)

        try:
            source_values.remove(source_value)
            setattr(result, 'bronwaarde', source_value)
            return_results.append(result)
        except ValueError:
            # It could happen that this object is requested from the API after an import has taken place, but before
            # the relations have been updated. In that case a bronwaarde could have been removed from the object,
            # while the relation still exists. Remove the relation from the result altogether.
            pass

    # Create empty result objects of model type with only bronwaarde set
    expected_type = models[model.__tablename__]
    for source_value in source_values:
        bronwaarde = expected_type()
        setattr(bronwaarde, 'bronwaarde', source_value)
        return_results.append(bronwaarde)

    return return_results


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
        relation = session.query(relation_table).filter(relation_table.src_id == getattr(obj, FIELD.ID))
        if obj.__has_states__:
            relation = relation.filter(relation_table.src_volgnummer == getattr(obj, FIELD.SEQNR))

        query = FilterConnectionField.get_query(model, info, relation.subquery(), **kwargs)
        return add_bronwaardes_to_results(relation_table, model, obj, query.all())

    return resolve_attribute


def get_resolve_inverse_attribute(relation_table, model):

    def resolve_attribute(obj, info, **kwargs):
        session = get_session()
        relation = session.query(relation_table).filter(relation_table.dst_id == getattr(obj, FIELD.ID))

        if obj.__has_states__:
            relation = relation.filter(relation_table.dst_volgnummer == getattr(obj, FIELD.SEQNR))

        query = FilterInverseConnectionField.get_query(model, info, relation.subquery(), **kwargs)
        return query.all()
    return resolve_attribute
