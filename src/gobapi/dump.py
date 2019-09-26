"""
Dump GOB

Initially only csv dumps of catalog collections in csv format are supported
"""
from gobcore.typesystem import get_gob_type

# CSV definitions
DELIMITER_CHAR = ";"
QUOTATION_CHAR = '"'
ESCAPE_CHAR = QUOTATION_CHAR

# Reference types definitions and conversions
REFERENCE_TYPES = ["GOB.Reference", "GOB.ManyReference"]
REFERENCE_FIELDS = ['ref', 'id', 'volgnummer', 'bronwaarde']

# Types and Fields to skip when dumping contents
SKIP_TYPES = ["GOB.VeryManyReference"]
SKIP_FIELDS = ["geometrie", "_hash", "_gobid", "_last_event"]


def _names_join(*args):
    """
    Joins zero or more strings by underscores

    :param args:
    :return:
    """
    return "_".join(args)


def _add_ref(dst):
    """
    Add a reference to a remote entity
    For entities without state this is the id, for other entities it is the id + volgnummer
    :param dst:
    :return:
    """
    dst['ref'] = dst.get('id')
    if dst.get('volgnummer') is not None:
        dst['ref'] = _names_join(dst['ref'], dst['volgnummer'])
    return dst


def _csv_line(values):
    """
    Returns a CSV line for the given values

    :param values:
    :return:
    """
    return DELIMITER_CHAR.join(values) + "\n"


def _csv_value(value):
    """
    Return the CSV value for a given value

    :param value:
    :return:
    """
    if value is None:
        return ""
    elif isinstance(value, (int, float)):
        # Do not surround numeric values with quotes
        return str(value)
    else:
        return f"{QUOTATION_CHAR}{value}{QUOTATION_CHAR}"


def _csv_reference_values(value, spec):
    """
    Returns the CSV values for the given reference and type specification
    Note that the result is an array, as a reference value results in multiple CSV values

    :param value:
    :param spec:
    :return:
    """
    values = []
    if spec['type'] == "GOB.Reference":
        dst = value or {}
        _add_ref(dst)
        for field in REFERENCE_FIELDS:
            sub_value = dst.get(field, None)
            values.append(_csv_value(sub_value))
    else:  # GOB.ManyReference
        dsts = value or []
        for dst in dsts:
            _add_ref(dst)
        for field in REFERENCE_FIELDS:
            sub_values = []
            for dst in dsts:
                sub_value = dst.get(field, None)
                sub_values.append(_csv_value(sub_value))
            values.append("[" + ",".join(sub_values) + "]")
    return values


def _csv_values(value, spec):
    """
    Returns the CSV values for the given value and type specification
    Note that the result is an array, as reference value result in multiple CSV values

    :param value:
    :param spec:
    :return:
    """
    if spec['type'] in REFERENCE_TYPES:
        return _csv_reference_values(value, spec)
    else:
        return [_csv_value(value)]


def _csv_header(field_specs):
    """
    Returns the CSV header fields for the given type specifications

    :param field_specs:
    :return:
    """
    fields = []
    for field_name, field_spec in field_specs.items():
        if field_spec['type'] in REFERENCE_TYPES:
            for reference_field in REFERENCE_FIELDS:
                fields.append(_csv_value(_names_join(field_name, reference_field)))
        else:
            fields.append(_csv_value(field_name))
    return fields


def _csv_record(entity, field_specs):
    """
    Returns the CSV record fields for the given entity and corresponding type specifications
    :param entity:
    :param field_specs:
    :return:
    """
    fields = []
    for field_name, field_spec in field_specs.items():
        gob_type = get_gob_type(field_spec['type'])
        entity_value = getattr(entity, field_name, None)
        value = gob_type.from_value(entity_value).to_value
        fields.extend(_csv_values(value, field_spec))
    return fields


def csv_entities(entities, model):
    """
    Yield the given entities as a list, starting with a header.

    :param entities:
    :param model:
    :return:
    """
    field_specs = model['all_fields']
    field_specs = {k: v for k, v in field_specs.items() if k not in SKIP_FIELDS and v['type'] not in SKIP_TYPES}

    header = _csv_header(field_specs)
    for entity in entities:
        if header:
            fields = _csv_header(field_specs)
            header = None
        else:
            fields = _csv_record(entity, field_specs)
        yield _csv_line(fields)
