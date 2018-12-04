"""GraphQL schema

The schema is generated from the GOB models and model classes as defined in GOB Core

"""
import graphene
import re

from graphene_sqlalchemy import SQLAlchemyObjectType

from gobcore.model import GOBModel
from gobcore.model.metadata import PRIVATE_META_FIELDS, PUBLIC_META_FIELDS, FIXED_FIELDS
from gobcore.model.sa.gob import models

from gobapi.storage import get_session
from gobapi.graphql.scalars import Date
from gobapi.graphql.filters import FilterConnectionField

def graphene_type(gob_typename, description=""):
    conversion = {
        "GOB.String": graphene.String,
        "GOB.Integer": graphene.Int,
        "GOB.Decimal": graphene.Float,
        "GOB.Boolean": graphene.Boolean,
        "GOB.Date": Date,
        "GOB.DateTime": graphene.DateTime
    }
    if conversion.get(gob_typename):
        return conversion.get(gob_typename)(description=description)

def get_resolve_attribute(mdl, ref_name):

    def resolve_attribute(obj, args, **kwargs):
        RELAY_ARGS = ['first', 'last', 'before', 'after', 'sort']
        _id = getattr(obj, ref_name)["id"]

        result = get_session().query(mdl).filter_by(_id=_id)
        for field, value in kwargs.items():
            if field not in RELAY_ARGS:
                if value == "null":
                    result = result.filter(getattr(mdl, field) == None)
                else:
                    result = result.filter(getattr(mdl, field) == value)
        return result.all()

    return resolve_attribute


exclude_fields = tuple(name for name in {
    **PRIVATE_META_FIELDS,
    **PUBLIC_META_FIELDS,
    **FIXED_FIELDS
}.keys())
queries = []
model = GOBModel()


refs = {}
for catalog_name, catalog in model.get_catalogs().items():
    for collection_name, collection in model.get_collections(catalog_name).items():
        from_table_name = f"{catalog_name}:{collection_name}"
        refs[from_table_name] = []

        # Get all references for the collection
        ref_types = ["GOB.Reference", "GOB.ManyReference"]
        ref_attrs = [ref for ref in collection["attributes"].values() if ref["type"] in ref_types]
        for ref in ref_attrs:
            refs[from_table_name].append(ref["ref"])

sorted_refs = []
for key, value in refs.items():
    values = [val for val in value if val in sorted_refs]
    min_value = min([sorted_refs.index(val) for val in values]) if len(values) else None
    sorted_refs.append(key) if min_value is None else sorted_refs.insert(min_value, key)
sorted_refs.reverse()

connection_fields = {}
connection_lists = {}
base_models = {}
for ref in sorted_refs:
    pattern = re.compile('(\w+):(\w+)')
    catalog_name, collection_name = re.findall(pattern, ref)[0]
    catalog = model.get_catalog(catalog_name)
    collection = model.get_collection(catalog_name, collection_name)

    # Get all references for the collection
    ref_types = ["GOB.Reference", "GOB.ManyReference"]
    ref_items = {key: value for key, value in collection["attributes"].items() if value["type"] in ref_types}
    fields = {}
    for key in ref_items.keys():
        cat_name, col_name = re.findall(pattern, ref_items[key]["ref"])[0]
        if not connection_fields.get(col_name) is None:
            fields[col_name] = {
                "connection": connection_fields[col_name],
                "list": connection_lists[col_name],
                "field_name": key
            }

    # class <Collection>(SQLAlchemyObjectType):
    #     class Meta:
    #         model = <SQLAlchemy Model>
    #         interfaces = (graphene.relay.Node, )
    base_model = models[model.get_table_name(catalog_name, collection_name)]
    object_type_class = type(collection_name, (SQLAlchemyObjectType,), {
        "__repr__": lambda self: f"{collection_name}",
        **{key: value["connection"] for key, value in fields.items()},
        **{f"resolve_{key}": get_resolve_attribute(base_models[key], value["field_name"]) for key, value in fields.items()},
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

    attributes = {attr: graphene_type(value["type"], value["description"]) for attr, value in
                  collection["attributes"].items() if
                  not graphene_type(value["type"]) is None}
    connection_fields[collection_name] = FilterConnectionField(connection_class, **attributes)
    connection_lists[collection_name] = graphene.List(connection_class, **attributes)

    queries.append({
        "collection_name": collection_name,
        "attributes": attributes,
        "object_type_class": object_type_class,
        "connection_class": connection_class
    })
    base_models[collection_name] = base_model

Query = type("Query", (graphene.ObjectType,),
             # <collection> = FilterConnectionField(<collection>Connection, filters...)
             connection_fields
             # {query["collection_name"]: FilterConnectionField(query["connection_class"], **query["attributes"]) for
             #  query in queries}
             )

schema = graphene.Schema(query=Query)
