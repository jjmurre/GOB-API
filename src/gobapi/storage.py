"""Storage

This module encapsulates the GOB storage.
The API returns GOB data by calling any of the methods in this module.
By using this module the API does not need to have any knowledge about the underlying storage

"""
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

from gobcore.model import GOBModel
from gobcore.typesystem import get_gob_type, get_gob_type_from_sql_type
from gobcore.model.metadata import PUBLIC_META_FIELDS, PRIVATE_META_FIELDS, FIXED_COLUMNS

from gobapi.config import GOB_DB

# Ths session and Base will be initialised by the _init() method
# The _init() method is called at the end of this module
session = None
Base = None
metadata = None


def connect():
    """Module initialisation

    The connection with the underlying storage is initialised.
    Meta information is available via the Base variale.
    Data retrieval is facilitated via the session object

    :return:
    """
    global session, Base, metadata

    engine = create_engine(URL(**GOB_DB))

    Base = automap_base()
    Base.prepare(engine, reflect=True)
    session = Session(engine)

    metadata = MetaData(engine)


def _get_table_and_model(collection_name):
    """Table and Model

    Utility method to retrieve the Table and Model for a specific collection

    :param collection_name:
    :return:
    """
    return getattr(Base.classes, collection_name), GOBModel().get_model(collection_name)


def _to_gob_value(entity, field, spec):
    if isinstance(spec, dict):
        gob_type = get_gob_type(spec['type'])
    else:
        gob_type = get_gob_type_from_sql_type(spec)

    entity_value = getattr(entity, field)
    gob_value = gob_type.from_value(entity_value)

    return gob_value


def _entity_to_dict(entity, model, meta={}):
    """Entity - Dictionary conversion

    Converts an entity to a dictionary.
    The model is used to extract only the public attributes of the entity.

    :param entity:
    :param model:
    :return:
    """
    items = list(model['fields'].items()) + list(meta.items())
    return {k: _to_gob_value(entity, k, v) for k, v in items}


def _entity_to_dict_from_sql_type(entity, columns):
    """Entity - Dictionary conversion using the sql type returned from a database view

    Converts an entity to a dictionary.

    :param entity:
    :param columns:
    :return:
    """
    return {column.name: _to_gob_value(entity, column.name, type(column.type)) for column in columns}


def get_entities(collection_name, offset, limit, view=None):
    """Entities

    Returns the list of entities within a collection.
    Starting at offset (>= 0) and limiting the result to <limit> items

    :param collection_name:
    :param offset:
    :param limit:
    :return:
    """
    assert(session and Base)

    if view:
        table = Table(view, metadata, autoload=True)
    else:
        table, model = _get_table_and_model(collection_name)

    all_entities = session.query(table)
    all_count = all_entities.count()

    page_entities = all_entities.offset(offset).limit(limit).all()

    if view:
        # Get all metadata fields and filter them from the columns returned by the database view
        metadata_column_list = [k for k in {**PUBLIC_META_FIELDS, **PRIVATE_META_FIELDS, **FIXED_COLUMNS}.keys()]
        columns = [c for c in table.columns if c.name not in metadata_column_list]

        # Use the sqltypes to get the correct gobtype and return a dict
        entities = [
            _entity_to_dict_from_sql_type(entity, columns)
            for entity in page_entities
        ]
    else:
        entities = [
            _entity_to_dict(entity, model)
            for entity in page_entities
        ]

    return entities, all_count


def get_entity(collection_name, id, view=None):
    """Entity

    Returns the entity from the specified collection or the view identied by the id parameter.
    If the entity cannot be found, None is returned

    :param collection_name:
    :param id:
    :param view:
    :return:
    """
    assert(session and Base)

    filter = {
        "_id": id,
        "_date_deleted": None
    }

    if view:
        table = Table(view, metadata, autoload=True)
    else:
        table, model = _get_table_and_model(collection_name)

    entity = session.query(table).filter_by(**filter).one_or_none()

    if view:
        # Get the private and fixed metadata fields and filter them from the columns returned by the database view
        metadata_column_list = [k for k in {**PRIVATE_META_FIELDS, **FIXED_COLUMNS}.keys()]
        columns = [c for c in table.columns if c.name not in metadata_column_list]

        return _entity_to_dict_from_sql_type(entity, columns) if entity else None
    else:
        return _entity_to_dict(entity, model, PUBLIC_META_FIELDS) if entity else None
