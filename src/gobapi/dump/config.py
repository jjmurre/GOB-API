from gobcore.typesystem import get_gob_type
from gobcore.model import GOBModel
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

# Relation fields, basically src_ref, src_id, src_volgnummer, dst_ref, dst_id, dst_volgnummer and expiration date
REL_INFO_FIELDS = [UNIQUE_ID, "id", FIELD.SEQNR]
REL_SIDES = ["src", "dst"]
REL_UNIQUE_IDS = [f"{side}_{UNIQUE_ID}" for side in REL_SIDES]
REL_FIELDS = [f"src_{field}" for field in REL_INFO_FIELDS] +\
             [f"dst_{field}" for field in REL_INFO_FIELDS] +\
             ["_expiration_date"]

# Types and Fields to skip when dumping contents
SKIP_TYPES = ["GOB.VeryManyReference"]
SKIP_FIELDS = ["_hash", "_gobid"]

SKIP_RELATIONS = ["brk_tng_brk_sdl_is_gebaseerd_op_stukdeel"]

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
SQL_QUOTATION_MARK = "'"


def is_unique_id(field_name):
    """
    Unique id's are the combination of the id and a optional volgnummer (for entities with state)

    :param field_name: any field name
    :return: True if the fieldname is the name of a unique id field
    """
    return field_name == UNIQUE_ID or field_name in REL_UNIQUE_IDS


def is_relation(model):
    """
    Tells if the model is a relation model, eg a model that represents the relation between two entities

    :param model: the model to investigate
    :return: True is the model is a relation model
    """
    return model['catalog'] == 'rel'


def get_reference_fields(spec):
    """
    Get the reference fields for a given referenced (spec)

    If the reference refers to an entity without states then do not include volgnummer

    For references without an existing destination relation, only include bronwaarde
    :param spec: type specification
    :return: array with reference fields
    """
    catalog_name, collection_name = spec['ref'].split(':')

    if GOBModel().get_collection(catalog_name, collection_name) is None:
        # Relation does not exist yet, only use SOURCE_VALUE
        return [FIELD.SOURCE_VALUE]

    if GOBModel().has_states(catalog_name, collection_name):
        return REFERENCE_FIELDS
    else:
        return [rf for rf in REFERENCE_FIELDS if rf != FIELD.SEQNR]


def get_unique_reference(entity, field_name, specs):
    """
    Returns a unique reference id for the given entity
    The unique reference is the combination of id and volgnummer for entities with state
    and id only for entities without state.

    The fieldname is used to differentiate between regular collections and relations

    :param entity:
    :param field_name:
    :param specs:
    :return:
    """
    if field_name == UNIQUE_ID:
        entity_id_field = specs[UNIQUE_ID]['entity_id']
        volgnummer_field = FIELD.SEQNR
        id = get_field_value(entity, entity_id_field, specs[entity_id_field])
        has_volgnummer = specs.get(volgnummer_field) is not None
        if has_volgnummer:
            volgnummer = get_field_value(entity, volgnummer_field, specs[volgnummer_field])
            return joined_names(id, volgnummer)
        else:
            return id
    else:
        assert field_name in REL_UNIQUE_IDS, f"Field name {field_name} is not a unique identifier name"
        side = [side for side in REL_SIDES if field_name == f"{side}_{UNIQUE_ID}"][0]
        id_field = f"{side}_id"
        volgnummer_field = f"{side}_{FIELD.SEQNR}"
        id = get_field_value(entity, id_field, specs[id_field])
        volgnummer = get_field_value(entity, volgnummer_field, specs[volgnummer_field])
        return id if volgnummer is None else joined_names(id, volgnummer)


def get_skip_fields(model):
    """
    Returns the fields of the given model that should be skipped in the export

    :param model: model of any collection
    :return: an array of fieldnames to be skipped
    """
    if is_relation(model):
        # For relations the test is to exclude all fields that are not explicitly included
        return [k for k in model['all_fields'].keys() if k not in REL_FIELDS]
    else:
        # For regular collection the skipped fields is just a small list
        return SKIP_FIELDS


def get_field_order(model):
    """
    Return the fields of the given model in a standard order

    :param model:
    :return: list of field names
    """
    if is_relation(model):
        # The relation fields are already ordered
        return REL_FIELDS

    # Start with the regular model fields
    model_fields = [k for k in model['fields'].keys()]

    # Get id fields
    id_fields = [model['entity_id']]
    if FIELD.SEQNR in model_fields:
        id_fields.append(FIELD.SEQNR)

    # Split this in relations, geometries and other fields
    relation_fields = [k for k in model_fields if is_gob_reference_type(model['fields'][k]['type'])]
    geo_fields = [k for k in model_fields if is_gob_geo_type(model['fields'][k]['type'])]
    data_fields = [k for k in model_fields if k not in relation_fields + geo_fields + id_fields]

    # Basic order is identification, plain data fields, then all references, then geo fields and then the unique id
    fields = id_fields + data_fields + relation_fields + geo_fields + [UNIQUE_ID]

    # Add all meta fields
    fields += [k for k in model['all_fields'].keys() if k not in fields]

    # Finally filter the list for fields that should be skipped
    fields = [k for k in fields if k not in SKIP_FIELDS and model['all_fields'][k]['type'] not in SKIP_TYPES]
    return fields


def get_field_specifications(model):
    """
    Return the field specs given the model of a collection

    :param model:
    :return:
    """
    skip_fields = get_skip_fields(model)

    specs = model['all_fields']
    if is_relation(model):
        # Include src_ref and dst_red
        specs.update({
            f"{side}_{UNIQUE_ID}": {
                'type': 'GOB.String',
                'description': f'{side} identificatie_volgnummer or identificatie'
            } for side in REL_SIDES
        })
    else:
        # Include ref
        specs[UNIQUE_ID] = {
            'type': 'GOB.String',
            'description': 'identificatie_volgnummer or identificatie',
            'entity_id': model['entity_id']
        }
    specs = {k: v for k, v in specs.items() if k not in skip_fields and v['type'] not in SKIP_TYPES}
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
    """
    Get the value of the given field in the given entity.

    Use GOB type to correctly interpret the entity value
    :param entity:
    :param field:
    :param spec:
    :return:
    """
    gob_type = get_gob_type(spec['type'])
    entity_value = getattr(entity, field, None)
    return gob_type.from_value(entity_value).to_value
