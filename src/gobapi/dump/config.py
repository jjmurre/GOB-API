from gobcore.typesystem import get_gob_type
from gobcore.model.metadata import FIELD
from gobcore.typesystem import is_gob_geo_type, is_gob_reference_type

# CSV definitions
DELIMITER_CHAR = ";"
QUOTATION_CHAR = '"'
ESCAPE_CHAR = QUOTATION_CHAR

# Reference types definitions and conversions
SIMPLE_ID = 'id'
UNIQUE_ID = 'ref'
REFERENCE_TYPES = ["GOB.Reference", "GOB.ManyReference"]
REFERENCE_FIELDS = [UNIQUE_ID, SIMPLE_ID, FIELD.SEQNR, FIELD.SOURCE_VALUE]

# Types and Fields to skip when dumping contents
SKIP_TYPES = ["GOB.VeryManyReference"]
SKIP_FIELDS = ["_hash", "_gobid"]

# SQL constants
SQL_TYPE_CONVERSIONS = {
    "GOB.String": "character varying",
    "GOB.Character": "character varying",
    "GOB.Date": "date",
    "GOB.DateTime": "timestamp without time zone",
    "GOB.Integer": "integer",
    "GOB.Decimal": "numeric",
    "GOB.Boolean": "boolean",
    "GOB.Geo.Geometry": "geometry",
    "GOB.Geo.Polygon": "geometry",
    "GOB.Geo.Point": "geometry"
}


def get_unique_reference(entity, specs):
    """
    Returns a unique reference id for the given entity
    The unique reference is the combination of id and volgnummer for entities with state
    and id only for entities without state

    :param entity:
    :param specs:
    :return:
    """
    entity_id_field = specs[UNIQUE_ID]['entity_id']
    volgnummer_field = FIELD.SEQNR
    id = get_field_value(entity, entity_id_field, specs[entity_id_field])
    has_volgnummer = specs.get(volgnummer_field) is not None
    if has_volgnummer:
        volgnummer = get_field_value(entity, volgnummer_field, specs[volgnummer_field])
        return joined_names(id, volgnummer)
    else:
        return id


def get_field_order(model):
    model_fields = [k for k in model['fields'].keys()]

    relation_fields = [k for k in model_fields if is_gob_reference_type(model['fields'][k]['type'])]
    geo_fields = [k for k in model_fields if is_gob_geo_type(model['fields'][k]['type'])]
    data_fields = [k for k in model_fields if k not in relation_fields and k not in geo_fields]

    fields = data_fields + relation_fields + geo_fields + [UNIQUE_ID]

    fields += [k for k in model['all_fields'].keys() if k not in fields]

    fields = [k for k in fields if k not in SKIP_FIELDS]
    return fields


def get_field_specifications(model):
    """
    Return the field specs given the model of a collection

    :param model:
    :return:
    """
    specs = model['all_fields']
    specs[UNIQUE_ID] = {
        'type': 'GOB.String',
        'description': 'identificatie_volgnummer or identificatie',
        'entity_id': model['entity_id']
    }
    specs = {k: v for k, v in specs.items() if k not in SKIP_FIELDS and v['type'] not in SKIP_TYPES}
    return specs


def joined_names(*args):
    """
    Joins zero or more strings by underscores

    :param args:
    :return:
    """
    return "_".join(str(arg) for arg in args)


def add_unique_reference(dst):
    """
    Add a reference to a remote entity
    For entities without state this is the id, for other entities it is the id + volgnummer

    :param dst:
    :return:
    """
    dst[UNIQUE_ID] = dst.get(SIMPLE_ID)
    if dst.get(FIELD.SEQNR) is not None:
        dst[UNIQUE_ID] = joined_names(dst[UNIQUE_ID], dst[FIELD.SEQNR])
    return dst


def get_field_value(entity, field, spec):
    gob_type = get_gob_type(spec['type'])
    entity_value = getattr(entity, field, None)
    return gob_type.from_value(entity_value).to_value
