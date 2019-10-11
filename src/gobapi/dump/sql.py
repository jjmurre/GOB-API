"""
Dump GOB

Dumps of catalog collections in sql format
"""
from gobcore.model import GOBModel

from gobapi.dump.config import DELIMITER_CHAR
from gobapi.dump.config import UNIQUE_ID, REFERENCE_TYPES, REFERENCE_FIELDS
from gobapi.dump.config import SQL_TYPE_CONVERSIONS, SQL_QUOTATION_MARK

from gobapi.dump.config import get_field_specifications, joined_names, get_field_order


def _quote(name):
    """
    Quote all SQL identifiers (schema, table, column names)
    to prevent weird errors with SQL keywords accidentally being used in identifiers.

    Note that quotation marks may differ per database type.
    Current escape char works for PostgreSQL

    :param name:
    :return:
    """
    QUOTE_CHAR = '"'
    return f"{QUOTE_CHAR}{name}{QUOTE_CHAR}"


def _create_schema(name):
    """
    Returns a SQL statement to create a schema with the given name

    :param name:
    :return:
    """
    return f"""
-- DROP SCHEMA {name} CASCADE;
CREATE SCHEMA IF NOT EXISTS {_quote(name)};
"""


def _create_field(name, type, description):
    """
    Create a database field

    :param name:
    :param type:
    :param description:
    :return: dict containing database field properties
    """
    return {
        'name': _quote(name),
        'type': SQL_TYPE_CONVERSIONS[type],
        'description': description
    }


def quote_sql_string(s):
    """
    Quote sql string by replacing sql quotation marks by 2 quotation marks, eg ' => ''
    :param s:
    :return:
    """
    return s.replace(SQL_QUOTATION_MARK, 2 * SQL_QUOTATION_MARK)


def _create_table(catalog, schema, table, model):
    """
    Returns a SQL statement to create a table in a schema
    The table fields are constructed from the specs

    :param schema:
    :param table:
    :param specs:
    :return:
    """
    specs = get_field_specifications(model)
    catalog_description = quote_sql_string(catalog['description'])
    fields = []
    for field_name in get_field_order(model):
        field_spec = specs[field_name]
        field_description = quote_sql_string(field_spec['description'])
        if field_spec['type'] in REFERENCE_TYPES:
            for reference_field in REFERENCE_FIELDS:
                name = joined_names(field_name, reference_field)
                fields.append(_create_field(name, 'GOB.String', f"{field_description} ({reference_field})"))
        elif field_spec['type'] == 'GOB.JSON':
            for field, spec in field_spec['attributes'].items():
                name = joined_names(field_name, field)
                fields.append(_create_field(name, spec['type'], f"{field_description} ({field})"))
        else:
            fields.append(_create_field(field_name, field_spec['type'], field_description))

    field_lengths = [len(field['name']) for field in fields]
    max_length = max(field_lengths) if field_lengths else 1

    table_name = (f"{_quote(schema)}.{_quote(table)}")
    table_fields = ",\n  ".join([f"{field['name']:{max_length}} {field['type']}" for field in fields])

    comments = ";\n".join([
        f"COMMENT ON COLUMN {table_name}.{field['name']:{max_length}} "
        f"IS {SQL_QUOTATION_MARK}{field['description']}{SQL_QUOTATION_MARK}" for field in fields
    ])

    return f"""
DROP TABLE IF EXISTS {table_name} CASCADE;
-- TRUNCATE TABLE {table_name};
CREATE TABLE IF NOT EXISTS {table_name}
(
  {table_fields},
  PRIMARY KEY ({UNIQUE_ID})
);

-- Table and Column comments
COMMENT ON TABLE  {table_name} {'':{max_length}} IS {SQL_QUOTATION_MARK}{catalog_description}{SQL_QUOTATION_MARK};
{comments}
"""


def _import_csv(schema, table, collection):
    """
    Returns a SQL statement to import a collection into a table

    :param schema:
    :param table:
    :param specs:
    :param collection:
    :return:
    """
    table_name = (f"{_quote(schema)}.{_quote(table)}")
    return f"""
\COPY {table_name} FROM '{collection}.csv' DELIMITER '{DELIMITER_CHAR}' CSV HEADER;
"""


def sql_entities(catalog_name, collection_name, model):
    """
    Returns a SQL statement to create a schema, table for the given model

    :param catalog_name:
    :param collection_name:
    :param entities:
    :param model:
    :return:
    """
    schema = catalog_name
    table = collection_name
    catalog = GOBModel().get_catalog(catalog_name)

    return f"""
-- Create schema
{_create_schema(schema)}

-- Create table
{_create_table(catalog, schema, table, model)}

-- Import data from csv
{_import_csv(schema, table, collection_name)}
"""
