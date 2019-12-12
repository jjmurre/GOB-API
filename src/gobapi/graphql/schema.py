"""GraphQL schema

The schema is generated from the GOB models and model classes as defined in GOB Core

"""
import graphene
import re
import sys
import geoalchemy2

from graphene.types.generic import GenericScalar
from graphene_sqlalchemy import SQLAlchemyObjectType
from graphene_sqlalchemy.converter import convert_sqlalchemy_type, get_column_doc, is_column_nullable
import sqlalchemy
from sqlalchemy.dialects import postgresql

from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.model.relations import get_fieldnames_for_missing_relations
from gobcore.model.sa.gob import models
from gobcore.typesystem import GOB_SECURE_TYPES, get_gob_type

from gobapi.graphql import graphene_type, exclude_fields
from gobapi.graphql.filters import FilterConnectionField, get_resolve_attribute, get_resolve_secure_attribute, \
    get_resolve_inverse_attribute, get_resolve_attribute_missing_relation
from gobapi.graphql.scalars import DateTime, GeoJSON

# Use the GOB model to generate the GraphQL query
model = GOBModel()
connection_fields = {}  # FilterConnectionField() per collection
inverse_connection_fields = {}  # FilterConnectionField() per collection without bronwaardes

# Generation of GraphQL schema goes past the default recursion limit of 1000 (something in Graphene)
sys.setrecursionlimit(1500)

bronwaarde_description = "De bronwaarde die als basis dient voor deze relatie"
broninfo_description = "De extra waarden meegegeven vanuit de bron naast de bronwaarde voor deze relatie"

begin_geldigheid_relatie_description = "De datum waarop deze relatie is ontstaan"
eind_geldigheid_relatie_description = "De datum waarop deze relatie is geëindigd"


def get_collection_references(collection):
    REF_TYPES = ["GOB.Reference", "GOB.ManyReference"]

    refs = collection["references"]
    return {key: value for key, value in refs.items() if value["type"] in REF_TYPES}


def get_inverse_references(catalogue, collection):
    try:
        return model.get_inverse_relations()[catalogue][collection]
    except KeyError:
        return {}


def get_collection_secure_attributes(collection):
    SEC_TYPES = [f"GOB.{type.name}" for type in GOB_SECURE_TYPES]

    attrs = collection["attributes"]
    return {key: value for key, value in attrs.items() if value["type"] in SEC_TYPES}


def _get_sorted_references(model):
    """Get an ordered list of references

    A reference is a "catalogue:collection" string

    Each collection in the given model might refer to another collection.

    For each collection in the model, all references to other collections are collected
    these references are stored in a dictionary

    Then a list is constructed so that when A refers to B, B will be before A in the list

    :param model: the model that contains all catalogs and collections
    :return: a sorted list of references ("catalogue:collection")
    """
    refs = {}
    for catalog_name, catalog in model.get_catalogs().items():
        for collection_name, collection in model.get_collections(catalog_name).items():
            from_ref = f"{catalog_name}:{collection_name}"

            # Get all references for the collection
            refs[from_ref] = [ref["ref"] for ref in get_collection_references(collection).values()]

    sorted_refs = []
    for from_ref, to_refs in refs.items():
        # Get all the references that are already in the list (all B where A => B)
        to_refs_in_list = [to_ref for to_ref in to_refs if to_ref in sorted_refs]
        if len(to_refs_in_list) == 0:
            # from_ref Free to put in the list at any place (no B where A => B)
            sorted_refs.insert(0, from_ref)
        else:
            # A => B implies B before A
            # put A after the last B
            # Get the index of the last occurrence of any of the referenced collections
            max_index = max([sorted_refs.index(to_ref) for to_ref in to_refs_in_list])
            # Put the collection after any of its referenced collections
            sorted_refs.insert(max_index + 1, from_ref)

    return sorted_refs


def _get_inverse_connections_for_references(inverse_references: dict) -> list:
    inverse_connections = []
    for cat_name, collections in inverse_references.items():
        for col_name, relation_names in collections.items():
            for relation_name in relation_names:
                schema_collection_name = model.get_table_name(cat_name, col_name)
                inverse_connections.append({
                    "src_catalog": cat_name,
                    "src_collection": col_name,
                    "src_relation_name": relation_name,
                    "field_name": f"inv_{relation_name}_{cat_name}_{col_name}",
                    "connection_field": graphene.Dynamic(get_inverse_connection_field(schema_collection_name)),
                })
    return inverse_connections


def _create_connection_class(name: str, attrs: dict, meta: type) -> type:
    """Creates a connection class for collection with name and attrs

    :param name:
    :param attrs:
    :return:
    """
    # class <name>ObjectType(SQLAlchemyObjectType):
    #     attribute = FilterConnectionField((attributeClass, attributeClass fields)
    #     resolve_attribute = lambda obj, info, **args
    #     class Meta:
    #         model = <SQLAlchemy Model>
    #         exclude_fields = [fieldname, ...]
    #         interfaces = (graphene.relay.Node, )
    object_type_class = type(f"{name}ObjectType", (SQLAlchemyObjectType,), {
        **attrs,
        "Meta": meta
    })

    # class <name>Connection(graphene.relay.Connection):
    #     class Meta:
    #         node = <object_type_class>
    return type(f"{name}Connection", (graphene.relay.Connection,), {
        "Meta": type("Meta", (), {
            "node": object_type_class
        })
    })


def get_graphene_query():
    base_models = {}  # SQLAlchemy model per collection

    # Sort references so that if a refers to b, a will be handled before b
    sorted_refs = _get_sorted_references(model)
    missing_relations = get_fieldnames_for_missing_relations(model)
    root_connection_fields = {}

    for ref in sorted_refs:
        # A reference is a "catalogue:collection" string
        pattern = re.compile('(\w+):(\w+)')
        catalog_name, collection_name = re.findall(pattern, ref)[0]
        collection = model.get_collection(catalog_name, collection_name)

        sec_attributes = get_collection_secure_attributes(collection)

        # Get all references for the collection
        ref_items = get_collection_references(collection)
        connections = []  # field name and corresponding FilterConnectionField()
        for key in ref_items.keys():
            cat_name, col_name = re.findall(pattern, ref_items[key]["ref"])[0]

            connections.append({
                "dst_name": model.get_table_name(cat_name, col_name),
                "connection_field": graphene.Dynamic(get_connection_field(model.get_table_name(cat_name, col_name))),
                "field_name": key
            })

        inverse_references = get_inverse_references(catalog_name, collection_name)
        inverse_connections = _get_inverse_connections_for_references(inverse_references)
        missing_rels = missing_relations.get(catalog_name, {}).get(collection_name, [])

        model_name = model.get_table_name(catalog_name, collection_name)
        base_model = models[model_name]  # SQLAlchemy model
        object_type_fields = {
            "__repr__": lambda self: f"SQLAlchemyObjectType {model_name}",
            **{connection["field_name"]: connection["connection_field"] for connection in connections},
            **{connection["field_name"]: connection["connection_field"] for connection in inverse_connections},
            **get_secure_resolvers(catalog_name, collection_name, sec_attributes),
            **get_relation_resolvers(connections),
            **get_inverse_relation_resolvers(inverse_connections),
            **get_missing_relation_resolvers(missing_rels),
            **{rel: graphene.JSONString for rel in missing_rels},
        }
        meta = type("Meta", (), {
            "model": base_model,
            "exclude_fields": exclude_fields,
            "interfaces": (graphene.relay.Node,)
        })

        root_connection_class = _create_connection_class(f"{model_name}Root",
                                                         object_type_fields,
                                                         meta)
        rel_connection_class = _create_connection_class(f"{model_name}Rel", {
            "bronwaarde": graphene.String(description=bronwaarde_description),
            "broninfo": GenericScalar(description=broninfo_description),
            FIELD.START_VALIDITY_RELATION: DateTime(description=begin_geldigheid_relatie_description),
            FIELD.END_VALIDITY_RELATION: DateTime(description=eind_geldigheid_relatie_description),
            **object_type_fields,
        }, meta)

        # 'type' is not allowed as an attribute name, so skip it as a filterable attribute
        collection["attributes"].pop('type', None)
        # Let the FilterConnectionField be filterable on all attributes of the collection
        attributes = {attr: graphene_type(value["type"], value["description"]) for attr, value in
                      collection["attributes"].items() if
                      not graphene_type(value["type"]) is None}
        root_connection_fields[f'{model_name}'] = FilterConnectionField(root_connection_class, **attributes)
        connection_fields[f'{model_name}'] = FilterConnectionField(rel_connection_class, **attributes)

        # Use root_connection_class for inverse relations as well. No need for bronwaardes here.
        inverse_connection_fields[f'{model_name}'] = FilterConnectionField(root_connection_class, **attributes)
        base_models[f'{model_name}'] = base_model

    Query = type("Query", (graphene.ObjectType,),
                 # <collection> = FilterConnectionField(<collection>Connection, filters...)
                 root_connection_fields
                 )
    return Query


def get_connection_field(key):
    """Gets a connection field resolver. Returns the correct type or a GenericScalar
    if the connection can not be found (Happens when relations are defined for collections
    which aren't in the model yet.)

    Used to be able to lazy load the schema to allow for circular references

    :param key: the key to lookup the correct connection field
    :return: the connection field or GenericScalar
    """

    def connection_field():
        try:
            return connection_fields[key]
        except KeyError:
            return GenericScalar

    return connection_field


def get_inverse_connection_field(key):
    """Gets a connection field resolver for an inverse relation. See get_connection_field

    :param key: the key to lookup the correct connection field
    :return: the connection field or GenericScalar
    """

    def connection_field():
        try:
            return inverse_connection_fields[key]
        except KeyError:
            return GenericScalar

    return connection_field


def get_secure_resolvers(src_catalog_name, src_collection_name, attributes):
    resolvers = {}
    for name, type_info in attributes.items():
        GOBType = get_gob_type(type_info["type"])
        resolvers[f"resolve_{name}"] = get_resolve_secure_attribute(name, GOBType)
    return resolvers


def get_inverse_relation_resolvers(inverse_connections):
    resolvers = {}

    for connection in inverse_connections:
        collection = model._data[connection['src_catalog']]['collections'][connection['src_collection']]
        is_many = collection['attributes'][connection['src_relation_name']]['type'] == 'GOB.ManyReference'

        resolvers[f"resolve_{connection['field_name']}"] = get_resolve_inverse_attribute(
            models[model.get_table_name(connection['src_catalog'], connection['src_collection'])],
            connection['src_relation_name'],
            is_many,
        )
    return resolvers


def get_missing_relation_resolvers(missing_relations):
    resolvers = {}

    for field_name in missing_relations:
        resolvers[f"resolve_{field_name}"] = get_resolve_attribute_missing_relation(field_name)

    return resolvers


def get_relation_resolvers(connections):
    resolvers = {}
    for connection in connections:
        try:
            resolvers[f"resolve_{connection['field_name']}"] = get_resolve_attribute(
                models[connection['dst_name']],
                connection['field_name'])
        except KeyError:
            pass
    return resolvers


@convert_sqlalchemy_type.register(sqlalchemy.types.DateTime)
def _convert_datetime(thetype, column, registry=None):
    return DateTime(description=get_column_doc(column), required=not (is_column_nullable(column)))


@convert_sqlalchemy_type.register(geoalchemy2.Geometry)
def _convert_geometry(thetype, column, registry=None):
    return GeoJSON(description=get_column_doc(column), required=not (is_column_nullable(column)))


@convert_sqlalchemy_type.register(postgresql.HSTORE)
@convert_sqlalchemy_type.register(postgresql.JSON)
@convert_sqlalchemy_type.register(postgresql.JSONB)
def _convert_json(thetype, column, registry=None):
    return GenericScalar(description=get_column_doc(column), required=not (is_column_nullable(column)))


schema = graphene.Schema(query=get_graphene_query())
