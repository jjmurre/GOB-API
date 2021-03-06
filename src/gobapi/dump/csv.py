"""
Dump GOB

Dumps of catalog collections in csv format
"""
import re

from gobapi.dump.config import DELIMITER_CHAR, QUOTATION_CHAR
from gobapi.dump.config import REFERENCE_TYPES, get_reference_fields

from gobapi.dump.config import get_unique_reference, add_unique_reference, is_unique_id
from gobapi.dump.config import get_field_specifications, get_field_order, get_field_value, joined_names


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
        value = str(value)
        value_without_crlf = re.compile(r"\r?\n")
        value = value_without_crlf.sub(" ", value)
        value = value.replace(QUOTATION_CHAR, 2 * QUOTATION_CHAR)
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
        for field in get_reference_fields(spec):
            sub_value = dst.get(field, None)
            values.append(_csv_value(sub_value))
    else:  # GOB.ManyReference
        dsts = value or []
        for dst in dsts:
            add_unique_reference(dst)
        for field in get_reference_fields(spec):
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
    elif spec['type'] == 'GOB.JSON':
        if type(value) == list:
            values = []
            for field in spec['attributes'].keys():
                sub_values = []
                for row in value:
                    sub_value = row.get(field, '')
                    sub_values.append(_csv_value(sub_value))
                values.append("[" + ",".join(sub_values) + "]")
            return values
        else:
            value = value or {}
            return [_csv_value(value.get(field)) for field in spec['attributes'].keys()]
    else:
        return [_csv_value(value)]


def _csv_header(field_specs, field_order):
    """
    Returns the CSV header fields for the given type specifications

    :param field_specs:
    :return:
    """
    fields = []
    for field_name in field_order:
        field_spec = field_specs[field_name]
        if field_spec['type'] in REFERENCE_TYPES:
            for reference_field in get_reference_fields(field_spec):
                fields.append(_csv_value(joined_names(field_name, reference_field)))
        elif field_spec['type'] == 'GOB.JSON':
            for field in field_spec['attributes'].keys():
                fields.append(_csv_value(joined_names(field_name, field)))
        else:
            fields.append(_csv_value(field_name))
    return fields


def _csv_record(entity, field_specs, field_order):
    """
    Returns the CSV record fields for the given entity and corresponding type specifications
    :param entity:
    :param field_specs:
    :return:
    """
    fields = []
    for field_name in field_order:
        field_spec = field_specs[field_name]
        if is_unique_id(field_name):
            value = get_unique_reference(entity, field_name, field_specs)
        else:
            value = get_field_value(entity, field_name, field_spec)
        fields.extend(_csv_values(value, field_spec))
    return fields


def csv_entities(entities, model, ignore_fields=None):
    """
    Yield the given entities as a list, starting with a header.

    :param entities:
    :param model:
    :return:
    """
    ignore_fields = ignore_fields or []
    field_specifications = get_field_specifications(model)
    field_order = [f for f in get_field_order(model) if f not in ignore_fields]

    header = _csv_header(field_specifications, field_order)
    for entity in entities:
        if header:
            yield _csv_line(header)
            header = None
        fields = _csv_record(entity, field_specifications, field_order)
        yield _csv_line(fields)
