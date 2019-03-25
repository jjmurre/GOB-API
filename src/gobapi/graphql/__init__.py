"""GraphQL

"""
import graphene

from gobcore.model.metadata import PRIVATE_META_FIELDS, PUBLIC_META_FIELDS, FIXED_FIELDS
from gobapi.graphql.scalars import Date, DateTime, GeoJSON


def graphene_type(gob_typename, description=""):
    """Get the corresponding Graphene type for any GOB type

    :param gob_typename: The typename within GOB
    :param description: The description to add to the Graphine type
    :return: The Graphene type if a corresponding type can be found, else None
    """
    conversion = {
        "GOB.String": graphene.String,
        "GOB.Integer": graphene.Int,
        "GOB.Decimal": graphene.Float,
        "GOB.Boolean": graphene.Boolean,
        "GOB.Date": Date,
        "GOB.DateTime": DateTime,
        "GOB.Geo.Geometry": GeoJSON,
        "GOB.JSON": graphene.JSONString,
    }
    if conversion.get(gob_typename):
        return conversion.get(gob_typename)(description=description)


# Not all GOB fields are exposed in the GraphQL interface
exclude_fields = tuple(name for name in {
    **PRIVATE_META_FIELDS,
    **PUBLIC_META_FIELDS,
    **FIXED_FIELDS
}.keys())
