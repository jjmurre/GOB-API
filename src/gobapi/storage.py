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


def _entity_to_dict_from_sql_type(entity, columns, meta={}):
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
        return _get_entities_from_view(view, offset, limit)
    else:
        return _get_entities_from_collection(collection_name, offset, limit)


def _get_entities_from_collection(collection_name, offset, limit):
    table, model = _get_table_and_model(collection_name)

    all_entities = session.query(table)

    all_count = all_entities.count()

    entities = [
        _entity_to_dict(entity, model)
        for entity in all_entities.offset(offset).limit(limit).all()
    ]

    return (entities, all_count)


def _get_entities_from_view(view_name, offset, limit):
    assert(session and Base)
    view = Table(view_name, metadata, autoload=True)
    all_entities = session.query(view)

    all_count = all_entities.count()

    metadata_column_list = [k for k in {**PUBLIC_META_FIELDS, **PRIVATE_META_FIELDS, **FIXED_COLUMNS}.keys()]
    columns = [c for c in view.columns if c.name not in metadata_column_list]

    entities = [
        _entity_to_dict_from_sql_type(entity, columns)
        for entity in all_entities.offset(offset).limit(limit).all()
    ]

    return (entities, all_count)


def get_entity(collection_name, id, view=None):
    """Entity

    Returns the entity within the specified collection identied by the id parameter.
    If the entity cannot be found, None is returned

    :param collection_name:
    :param id:
    :param view:
    :return:
    """
    assert(session and Base)
    if view:
        return _get_entity_from_view(view, id)
    else:
        return _get_entity_from_collection(collection_name, id)


def _get_entity_from_collection(collection_name, id):
    table, model = _get_table_and_model(collection_name)

    filter = {
        "_id": id,
        "_date_deleted": None
    }

    entity = session.query(table).filter_by(**filter).one_or_none()

    return _entity_to_dict(entity, model, PUBLIC_META_FIELDS) if entity else None


def _get_entity_from_view(view_name, id):
    """Entity

    Returns the entity within the specified view identied by the id parameter.
    If the entity cannot be found, None is returned

    :param view_name:
    :param id:
    :return:
    """
    assert(session and Base)
    view = Table(view_name, metadata, autoload=True)

    filter = {
        "_id": id,
        "_date_deleted": None
    }

    metadata_column_list = [k for k in {**PRIVATE_META_FIELDS, **FIXED_COLUMNS}.keys()]
    columns = [c for c in view.columns if c.name not in metadata_column_list]

    entity = session.query(view).filter_by(**filter).one_or_none()
    return _entity_to_dict_from_sql_type(entity, columns) if entity else None
