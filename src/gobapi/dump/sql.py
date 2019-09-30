"""
Dump GOB

Dumps of catalog collections in sql format
"""
from gobcore.model import GOBModel

from gobapi.dump.config import DELIMITER_CHAR
from gobapi.dump.config import UNIQUE_ID, REFERENCE_TYPES, REFERENCE_FIELDS
from gobapi.dump.config import SCHEMA, SQL_TYPE_CONVERSIONS

from gobapi.dump.config import get_field_specifications, joined_names


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


def _create_table(schema, table, specs):
    """
    Returns a SQL statement to create a table in a schema
    The table fields are constructed from the specs

    :param schema:
    :param table:
    :param specs:
    :return:
    """
    fields = []
    for field_name, field_spec in specs.items():
        if field_spec['type'] in REFERENCE_TYPES:
            for reference_field in REFERENCE_FIELDS:
                name = joined_names(field_name, reference_field)
                fields.append(f"{_quote(name)} {SQL_TYPE_CONVERSIONS['GOB.String']}")
        else:
            fields.append(f"{_quote(field_name)} {SQL_TYPE_CONVERSIONS[field_spec['type']]}")
    fields = ",\n  ".join(fields)
    table_name = _quote(f"{schema}.{table}")
    return f"""
DROP TABLE IF EXISTS {table_name} CASCADE;
-- TRUNCATE TABLE {table_name};
CREATE TABLE IF NOT EXISTS {table_name}
(
  {fields},
  PRIMARY KEY ({UNIQUE_ID})
);
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
    table_name = _quote(f"{schema}.{table}")
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
    field_specifications = get_field_specifications(model)

    schema = SCHEMA
    table = GOBModel().get_table_name(catalog_name, collection_name)

    return f"""
-- Create schema
{_create_schema("analysis")}

-- Create table
{_create_table(schema, table, field_specifications)}

-- Import data from csv
{_import_csv(schema, table, collection_name)}
"""
