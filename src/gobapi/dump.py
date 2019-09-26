from gobcore.typesystem import get_gob_type

DELIMITER_CHAR = ";"
QUOTATION_CHAR = '"'
ESCAPE_CHAR = QUOTATION_CHAR

SKIP_TYPES = ["GOB.VeryManyReference"]
REFERENCE_TYPES = ["GOB.Reference", "GOB.ManyReference"]
REFERENCE_FIELDS = ['ref', 'id', 'volgnummer', 'bronwaarde']

HIDDEN_FIELDS = ["geometrie", "_hash", "_gobid", "_last_event"]


def _names_join(*args):
    return "_".join(args)


def _add_ref(dst):
    dst['ref'] = dst.get('id')
    if dst.get('volgnummer') is not None:
        dst['ref'] = _names_join(dst['ref'], dst['volgnummer'])
    return dst


def _csv_line(values):
    return DELIMITER_CHAR.join(values) + "\n"


def _csv_value(value):
    if value is None:
        return ""
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        return f"{QUOTATION_CHAR}{value}{QUOTATION_CHAR}"


def _csv_header(field_specs):
    fields = []
    for field_name, field_spec in field_specs.items():
        if field_spec['type'] in REFERENCE_TYPES:
            for reference_field in REFERENCE_FIELDS:
                fields.append(_csv_value(_names_join(field_name, reference_field)))
        else:
            fields.append(_csv_value(field_name))
    return fields


def _csv_reference_values(value, spec):
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


def _csv_values(value, spec):  # noqa: C901
    if spec['type'] in REFERENCE_TYPES:
        return _csv_reference_values(value, spec)
    else:
        return [_csv_value(value)]


def _csv_record(entity, field_specs):
    fields = []
    for field_name, field_spec in field_specs.items():
        gob_type = get_gob_type(field_spec['type'])
        entity_value = getattr(entity, field_name, None)
        value = gob_type.from_value(entity_value).to_value
        fields.extend(_csv_values(value, field_spec))
    return fields


def csv_entities(entities, model):
    field_specs = model['all_fields']
    field_specs = {k: v for k, v in field_specs.items() if k not in HIDDEN_FIELDS and v['type'] not in SKIP_TYPES}

    header = _csv_header(field_specs)
    for entity in entities:
        if header:
            fields = _csv_header(field_specs)
            header = None
        else:
            fields = _csv_record(entity, field_specs)
        yield _csv_line(fields)
