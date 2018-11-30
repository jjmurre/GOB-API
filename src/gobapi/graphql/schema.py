"""GraphQL schema

The schema is generated from the GOB models and model classes as defined in GOB Core

"""
import graphene

from graphene_sqlalchemy import SQLAlchemyObjectType, SQLAlchemyConnectionField

from gobcore.model import GOBModel
from gobcore.model.metadata import PRIVATE_META_FIELDS, PUBLIC_META_FIELDS, FIXED_FIELDS
from gobcore.model.sa.gob import models

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
            "object_type_class": object_type_class,
            "connection_class": connection_class
        })

Query = type("Query", (graphene.ObjectType,),
             # <collection> = SQLAlchemyConnectionField(<collection>Connection)
             {query["collection_name"]: SQLAlchemyConnectionField(query["connection_class"]) for query in queries}
             )

schema = graphene.Schema(query=Query)
