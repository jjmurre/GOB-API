"""GraphQL schema

The schema is generated from the GOB models and model classes as defined in GOB Core

"""
import graphene

from graphene_sqlalchemy import SQLAlchemyObjectType

from gobcore.model import GOBModel
from gobcore.model.metadata import PRIVATE_META_FIELDS, PUBLIC_META_FIELDS, FIXED_FIELDS
from gobcore.model.sa.gob import models

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


exclude_fields = tuple(name for name in {
    **PRIVATE_META_FIELDS,
    **PUBLIC_META_FIELDS,
    **FIXED_FIELDS
}.keys())
queries = []
model = GOBModel()
for catalog_name, catalog in model.get_catalogs().items():
    for collection_name, collection in model.get_collections(catalog_name).items():
        # class <Collection>(SQLAlchemyObjectType):
        #     class Meta:
        #         model = <SQLAlchemy Model>
        #         interfaces = (graphene.relay.Node, )
        object_type_class = type(collection_name, (SQLAlchemyObjectType,), {
            "__repr__": lambda self: f"{collection_name}",
            "Meta": type(f"{collection_name}_Meta", (), {
                "model": models[model.get_table_name(catalog_name, collection_name)],
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

        queries.append({
            "collection_name": collection_name,
            "attributes": {attr: graphene_type(value["type"], value["description"]) for attr, value in
                           collection["attributes"].items() if
                           not graphene_type(value["type"]) is None},
            "object_type_class": object_type_class,
            "connection_class": connection_class
        })

Query = type("Query", (graphene.ObjectType,),
             # <collection> = FilterConnectionField(<collection>Connection, filters...)
             {query["collection_name"]: FilterConnectionField(query["connection_class"], **query["attributes"]) for
              query in queries}
             )

schema = graphene.Schema(query=Query)
