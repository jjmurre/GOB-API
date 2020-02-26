"""Graphene filters

Filters provide for a way to dynamically filter collections on field values

"""
from graphene import Boolean
from graphene_sqlalchemy import SQLAlchemyConnectionField
from sqlalchemy import and_

from gobcore.model.metadata import FIELD
from gobcore.model import GOBModel
from gobcore.model.relations import get_relation_name
from gobcore.model.sa.gob import models, Base

from gobapi.utils import dict_to_camelcase
from gobapi.storage import filter_active, filter_deleted

from gobapi.constants import API_FIELD
from gobapi.graphql_streaming.utils import resolve_schema_collection_name

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


class RelationQuery:
    """Fetches related objects through relation table and adds bronwaarde/geldigheid attributes.
    Results are formatted so that Graphene knows how to handle them.

    Usage:
    query = RelationQuery(....)
    results = query.get_results()

    Optional step:
    query.populate_source_info(results)

    """
    RELAY_ARGS = ['first', 'last', 'before', 'after', 'sort', 'active']
    src_side = 'src'
    dst_side = 'dst'
    add_relation_table_columns = True

    def __init__(self, src_object, dst_model, attribute_name, **kwargs):
        self.src_object = src_object
        self.dst_model = dst_model
        self.attribute_name = attribute_name

        self.kwargs = kwargs
        self.kwargs.setdefault("active", Boolean(default_value=True))

    def _add_relation_table_filters(self, query, relation_model):
        """Add filters on relation table to query.

        :param query:
        :return:
        """
        # Ignore deleted relations and possibly active -> No need to filter for deleted/active dst objects too, as
        # relations pointing to deleted objects will always be marked deleted as well. Same for expiration date
        query = filter_deleted(query, relation_model)

        if self.kwargs.get('active'):
            query = filter_active(query, relation_model)

        # Filter relation table rows
        query = query.filter(getattr(self.src_object, FIELD.ID) == getattr(relation_model, f'{self.src_side}_id'))

        if self.src_object.__has_states__:
            query = query.filter(getattr(self.src_object, FIELD.SEQNR) ==
                                 getattr(relation_model, f'{self.src_side}_volgnummer'))
        return query

    def _add_dst_table_join(self, query, relation_model):
        """Adds destination table join to query.
        Filtering on destination objects happens in this on clause (instead of in the where clause), so that we don't
        filter out rows from the relation table; this way we can be sure we have a row for every bronwaarde and we
        don't need to do extra work to fetch the bronwaardes without a matching destination object.

        :param query:
        :return:
        """
        join_args = [getattr(relation_model, f'{self.dst_side}_id') == getattr(self.dst_model, FIELD.ID)] + (
            [getattr(relation_model, f'{self.dst_side}_volgnummer') == getattr(self.dst_model, FIELD.SEQNR)]
            if self.dst_model.__has_states__
            else []
        )

        for field, value in self.kwargs.items():
            if field not in self.RELAY_ARGS:
                if value == FILTER_ON_NULL_VALUE:
                    join_args.append(getattr(self.dst_model, field).is_(None))
                else:
                    join_args.append(getattr(self.dst_model, field) == value)

        return query.join(self.dst_model, and_(*join_args), isouter=True)

    def _build_query(self):
        relation_model = self._get_relation_model()
        query = getattr(self.dst_model, 'query')

        # Assure that query results are authorised
        catalog, collection = resolve_schema_collection_name(self.dst_model.__tablename__)
        query.set_catalog_collection(catalog, collection)

        query = query.select_from(relation_model)

        query = self._add_relation_table_filters(query, relation_model)
        query = self._add_dst_table_join(query, relation_model)

        if self.add_relation_table_columns:
            query = query.add_columns(
                getattr(relation_model, FIELD.SOURCE_VALUE),
                getattr(relation_model, FIELD.START_VALIDITY).label(API_FIELD.START_VALIDITY_RELATION),
                getattr(relation_model, FIELD.END_VALIDITY).label(API_FIELD.END_VALIDITY_RELATION),
            )

        return query

    def get_results(self):
        query = self._build_query()

        return [self._flatten_join_query_result(result) if self.add_relation_table_columns else result
                for result in query.all()]

    def populate_source_info(self, results):
        source_values = getattr(self.src_object, self.attribute_name)
        source_values = [source_values] if isinstance(source_values, dict) else source_values

        source_infos = {item[FIELD.SOURCE_VALUE]: item.get(FIELD.SOURCE_INFO) for item in source_values}

        for result in results:
            setattr(result, FIELD.SOURCE_INFO, source_infos.get(getattr(result, FIELD.SOURCE_VALUE)))

    def _flatten_join_query_result(self, result):
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

    def _get_relation_model(self):
        relation_owner = (self.src_object if self.src_side == 'src' else self.dst_model)

        # Get the source catalogue and collection from the source object
        owner_table_name = getattr(relation_owner, '__tablename__')
        owner_catalog_name, owner_collection_name = _get_catalog_collection_name_from_table_name(owner_table_name)

        relation_name = get_relation_name(gobmodel, owner_catalog_name, owner_collection_name, self.attribute_name)
        relation_table_name = f"rel_{relation_name}"

        return models[relation_table_name]


class InverseRelationQuery(RelationQuery):
    src_side = 'dst'
    dst_side = 'src'
    add_relation_table_columns = False


def get_resolve_json_attribute(name):
    """
    Gets a resolver for a JSON type attribute

    JSON attributes are camelcased

    :param name: name of the JSON attribute
    :return: a resolver function for JSON attributes
    """

    def resolve_attribute(obj, info, **kwargs):
        value = getattr(obj, name)
        return dict_to_camelcase(value)

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


def _get_catalog_collection_name_from_table_name(table_name):
    """Gets the catalog and collection name from the table name

    :param table_name:
    """

    catalog_name = GOBModel().get_catalog_from_table_name(table_name)
    collection_name = GOBModel().get_collection_from_table_name(table_name)

    return catalog_name, collection_name


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
        query = RelationQuery(src_object=obj, dst_model=model, attribute_name=src_attribute_name, **kwargs)
        results = query.get_results()

        query.populate_source_info(results)

        return results

    return resolve_attribute


def get_resolve_inverse_attribute(model, src_attribute_name):
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
        query = InverseRelationQuery(src_object=obj, dst_model=model, attribute_name=src_attribute_name, **kwargs)
        return query.get_results()

    return resolve_attribute
