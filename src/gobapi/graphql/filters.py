"""Graphene filters

Filters provide for a way to dynamically filter collections on field values

"""
from graphql.utils.ast_to_dict import ast_to_dict
from graphene import Boolean
from graphene_sqlalchemy import SQLAlchemyConnectionField
from sqlalchemy import tuple_, and_

from gobcore.model.metadata import FIELD
from gobcore.model import GOBModel
from gobcore.model.relations import get_relation_name
from gobcore.model.sa.gob import models, Base

from gobapi import serialize
from gobapi.utils import to_camelcase
from gobapi.storage import filter_active, filter_deleted

from gobapi.graphql_streaming.utils import resolve_schema_collection_name

from typing import List

gobmodel = GOBModel()

FILTER_ON_NULL_VALUE = "null"

START_VALIDITY_RELATION = "begin_geldigheid_relatie"
END_VALIDITY_RELATION = "eind_geldigheid_relatie"


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
        # Assure that query results are authorised
        catalog, collection = resolve_schema_collection_name(model.__tablename__)
        query.set_catalog_collection(catalog, collection)

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
    setattr(result, FIELD.SOURCE_VALUE, source_item[FIELD.SOURCE_VALUE])
    setattr(result, FIELD.SOURCE_INFO, source_item.get(FIELD.SOURCE_INFO))


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


def _get_catalog_collection_name_from_table_name(table_name):
    """Gets the catalog and collection name from the table name

    :param table_name:
    """

    catalog_name = gobmodel.get_catalog_from_table_name(table_name)
    collection_name = gobmodel.get_collection_from_table_name(table_name)

    return catalog_name, collection_name


def _extract_relation_model(src_obj, dst_model, relation_name):

    # Get the source catalogue and collection from the source object
    src_table_name = getattr(src_obj, '__tablename__')
    src_catalog_name, src_collection_name = _get_catalog_collection_name_from_table_name(src_table_name)

    # Get the destination catalogue and collection from the destination model
    dst_table_name = getattr(dst_model, '__tablename__')
    dst_catalog_name, dst_collection_name = _get_catalog_collection_name_from_table_name(dst_table_name)

    relation_table_name = f"rel_{get_relation_name(gobmodel, src_catalog_name, src_collection_name, relation_name)}"

    return models[relation_table_name]


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

        # Extract the requested fields in the reference
        query_fields = get_fields_in_query(info)

        # Check if a relation field is requested and we need to join the relation table
        join_relation = any([i in query_fields for i in [
                            to_camelcase(START_VALIDITY_RELATION),
                            to_camelcase(END_VALIDITY_RELATION)]])

        if join_relation:
            query = add_relation_join_query(obj, model, src_attribute_name, query)

        results = [flatten_join_query_result(result) if join_relation else result for result in query.all()]

        return add_bronwaardes_to_results(src_attribute_name, model, obj, results)

    return resolve_attribute


def add_relation_join_query(obj, model, src_attribute_name, query):

    relation_model = _extract_relation_model(src_obj=obj, dst_model=model, relation_name=src_attribute_name)

    relation_join_args = [
        getattr(obj, FIELD.ID) == getattr(relation_model, 'src_id'),
        getattr(model, FIELD.ID) == getattr(relation_model, 'dst_id')
    ]

    if obj.__has_states__:
        relation_join_args.append(getattr(obj, FIELD.SEQNR) == getattr(relation_model, 'src_volgnummer'))

    if model.__has_states__:
        relation_join_args.append(getattr(model, FIELD.SEQNR) == getattr(relation_model, 'dst_volgnummer'))

    # Add the relationship table join and query the relation fields
    query = query.join(relation_model, and_(*relation_join_args)) \
                 .add_columns(getattr(relation_model, FIELD.START_VALIDITY).label(START_VALIDITY_RELATION),
                              getattr(relation_model, FIELD.END_VALIDITY).label(END_VALIDITY_RELATION))

    return query


def get_fields_in_query(info):
    """Gets the fields in a GraphQL query and returns a list with all fields in the reference attribute

    :param info:
    """
    fields = []
    node = ast_to_dict(info.field_asts[0])

    fields = collect_fields(node, fields)

    return fields


def collect_fields(node, fields):
    """ Recursively look in the GraphQL query to get the references fields.
    It stops after the first set of edges and node to only get this reference's fields

    :param node:
    :param fields:
    """

    if node.get('selection_set'):
        for leaf in node['selection_set']['selections']:
            if leaf['kind'] == 'Field':
                variable_name = leaf['name']['value']
                if variable_name in ('edges', 'node'):
                    collect_fields(leaf, fields)
                else:
                    fields.append(leaf['name']['value'])
    return fields


def flatten_join_query_result(result):
    """SQLAlchemy returns a named tuple when querying for extra columns besided the reference model.
    This function adds all extra variables to the reference object, to be used in GraphQL

    :param result:
    """

    # The first item in the result is the requested reference object
    reference_object = result[0]

    for key, value in result._asdict().items():
        if isinstance(value, Base):
            continue
        else:
            setattr(reference_object, key, value)
    return reference_object


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
