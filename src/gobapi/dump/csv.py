"""
Dump GOB

Dumps of catalog collections in csv format
"""
from gobapi.dump.config import DELIMITER_CHAR, QUOTATION_CHAR
from gobapi.dump.config import UNIQUE_ID, REFERENCE_TYPES, REFERENCE_FIELDS

from gobapi.dump.config import get_unique_reference, add_unique_reference
from gobapi.dump.config import get_field_specifications, get_field_value, joined_names


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
        add_unique_reference(dst)
        for field in REFERENCE_FIELDS:
            sub_value = dst.get(field, None)
            values.append(_csv_value(sub_value))
    else:  # GOB.ManyReference
        dsts = value or []
        for dst in dsts:
            add_unique_reference(dst)
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
                fields.append(_csv_value(joined_names(field_name, reference_field)))
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
        if field_name == UNIQUE_ID:
            value = get_unique_reference(entity, field_specs)
        else:
            value = get_field_value(entity, field_name, field_spec)
        fields.extend(_csv_values(value, field_spec))
    return fields


def csv_entities(entities, model):
    """
    Yield the given entities as a list, starting with a header.

    :param entities:
    :param model:
    :return:
    """
    field_specifications = get_field_specifications(model)

    header = _csv_header(field_specifications)
    for entity in entities:
        if header:
            fields = _csv_header(field_specifications)
            header = None
        else:
            fields = _csv_record(entity, field_specifications)
        yield _csv_line(fields)
