"""GraphQL schema

The schema is generated from the GOB models and model classes as defined in GOB Core

"""
import graphene
import re

import geoalchemy2

from graphene.types.generic import GenericScalar
from graphene_sqlalchemy import SQLAlchemyObjectType
from graphene_sqlalchemy.converter import convert_sqlalchemy_type, get_column_doc, is_column_nullable
import sqlalchemy
from sqlalchemy.dialects import postgresql

from gobcore.model import GOBModel
from gobcore.model.sa.gob import models

from gobapi.graphql import graphene_type, exclude_fields
from gobapi.graphql.filters import FilterConnectionField, get_resolve_attribute
from gobapi.graphql.scalars import DateTime, GeoJSON


def get_collection_references(collection):
    # Currently implemented for single references only
    REF_TYPES = ["GOB.Reference"]

    refs = collection["references"]
    return {key: value for key, value in refs.items() if value["type"] in REF_TYPES}


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


def get_graphene_query():
    connection_fields = {}  # FilterConnectionField() per collection
    base_models = {}  # SQLAlchemy model per collection

    # Use the GOB model to generate the GraphQL query
    model = GOBModel()

    # Sort references so that if a refers to b, a will be handled before b
    sorted_refs = _get_sorted_references(model)

    for ref in sorted_refs:
        # A reference is a "catalogue:collection" string
        pattern = re.compile('(\w+):(\w+)')
        catalog_name, collection_name = re.findall(pattern, ref)[0]
        collection = model.get_collection(catalog_name, collection_name)

        # Get all references for the collection
        ref_items = get_collection_references(collection)
        fields = {}  # field name and corresponding FilterConnectionField()
        for key in ref_items.keys():
            cat_name, col_name = re.findall(pattern, ref_items[key]["ref"])[0]
            if not connection_fields.get(col_name) is None:
                fields[col_name] = {
                    "connection": connection_fields[col_name],
                    "field_name": key
                }

        # class <Collection>(SQLAlchemyObjectType):
        #     attribute = FilterConnectionField((attributeClass, attributeClass fields)
        #     resolve_attribute = lambda obj, info, **args
        #     class Meta:
        #         model = <SQLAlchemy Model>
        #         exclude_fields = [fieldname, ...]
        #         interfaces = (graphene.relay.Node, )
        base_model = models[model.get_table_name(catalog_name, collection_name)]  # SQLAlchemy model
        object_type_class = type(collection_name, (SQLAlchemyObjectType,), {
            "__repr__": lambda self: f"SQLAlchemyObjectType {collection_name}",
            **{value["field_name"]: value["connection"] for key, value in fields.items()},
            **{f"resolve_{value['field_name']}": get_resolve_attribute(base_models[key], value["field_name"])
               for key, value in fields.items()},
            "Meta": type(f"{collection_name}_Meta", (), {
                "model": base_model,
                "exclude_fields": exclude_fields,
                "interfaces": (graphene.relay.Node,)
            })
        })

        # class <Collection>Connection(graphene.relay.Connection):
        #     class Meta:
        #         node = <Collection>
        connection_class = type(f"{collection_name}Connection", (graphene.relay.Connection,), {
            "Meta": type(f"{collection_name}_Connection_Meta", (), {
                "node": object_type_class
            })
        })

        # 'type' is not allowed as an attribute name, so skip it as a filterable attribute
        collection["attributes"].pop('type', None)
        # Let the FilterConnectionField be filterable on all attributes of the collection
        attributes = {attr: graphene_type(value["type"], value["description"]) for attr, value in
                      collection["attributes"].items() if
                      not graphene_type(value["type"]) is None}
        connection_fields[collection_name] = FilterConnectionField(connection_class, **attributes)
        base_models[collection_name] = base_model

    Query = type("Query", (graphene.ObjectType,),
                 # <collection> = FilterConnectionField(<collection>Connection, filters...)
                 connection_fields
                 )
    return Query


@convert_sqlalchemy_type.register(sqlalchemy.types.DateTime)
def _convert_datetime(thetype, column, registry=None):
    return DateTime(description=get_column_doc(column), required=not(is_column_nullable(column)))


@convert_sqlalchemy_type.register(geoalchemy2.Geometry)
def _convert_geometry(thetype, column, registry=None):
    return GeoJSON(description=get_column_doc(column), required=not(is_column_nullable(column)))


@convert_sqlalchemy_type.register(postgresql.HSTORE)
@convert_sqlalchemy_type.register(postgresql.JSON)
@convert_sqlalchemy_type.register(postgresql.JSONB)
def _convert_json(thetype, column, registry=None):
    return GenericScalar(description=get_column_doc(column), required=not(is_column_nullable(column)))


schema = graphene.Schema(query=get_graphene_query())
