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


def _get_table_and_model(collection_name, view):
    """Table and Model

    Utility method to retrieve the Table and Model for a specific collection.
    When a view is provided use the and do not retun the GOBModel.

    :param collection_name:
    :param view:
    :return:
    """
    if view:
        return Table(view, metadata, autoload=True), None
    else:
        return getattr(Base.classes, collection_name), GOBModel().get_model(collection_name)


def _to_gob_value(entity, field, spec):
    if isinstance(spec, dict):
        gob_type = get_gob_type(spec['type'])
    else:
        gob_type = get_gob_type_from_sql_type(spec)

    entity_value = getattr(entity, field)
    gob_value = gob_type.from_value(entity_value)

    return gob_value


def _get_convert_for_model(model, meta={}):
    """Get the entity to dict convert function for GOBModels

    The model is used to extract only the public attributes of the entity.

    :param entity:
    :param model:
    :return:
    """
    def convert(entity):
        return {k: _to_gob_value(entity, k, v) for k, v in items}

    items = list(model['fields'].items()) + list(meta.items())
    return convert


def _get_convert_for_table(table, filter={}):
    """Get the entity to dict convert function for database Tables or Views

    The table columns are used to extract only the public attributes of the entity.

    :param entity:
    :param model:
    :return:
    """
    def convert(entity):
        # Use the sqltypes to get the correct gobtype and return a dict
        return {column.name: _to_gob_value(entity, column.name, type(column.type)) for column in columns}

    # Get all metadata fields and filter them from the columns returned by the database view
    metadata_column_list = [k for k in filter.keys()]
    columns = [c for c in table.columns if c.name not in metadata_column_list]
    return convert


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

    table, model = _get_table_and_model(collection_name, view)

    all_entities = session.query(table)
    all_count = all_entities.count()

    page_entities = all_entities.offset(offset).limit(limit).all()

    if view:
        entity_convert = _get_convert_for_table(table,
                                                {**PUBLIC_META_FIELDS, **PRIVATE_META_FIELDS, **FIXED_COLUMNS})
    else:
        entity_convert = _get_convert_for_model(model)

    entities = [entity_convert(entity) for entity in page_entities]

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

    table, model = _get_table_and_model(collection_name, view)

    entity = session.query(table).filter_by(**filter).one_or_none()

    if view:
        entity_convert = _get_convert_for_table(table,
                                                {**PRIVATE_META_FIELDS, **FIXED_COLUMNS})
    else:
        entity_convert = _get_convert_for_model(model, PUBLIC_META_FIELDS)

    return entity_convert(entity) if entity else None
