"""Storage

This module encapsulates the GOB storage.
The API returns GOB data by calling any of the methods in this module.
By using this module the API does not need to have any knowledge about the underlying storage

"""
from gobcore.typesystem import get_gob_type
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base

from gobcore.model import GOBModel
from gobapi.config import GOB_DB

# Ths session and Base will be initialised by the _init() method
# The _init() method is called at the end of this module
session = None
Base = None


def connect():
    """Module initialisation

    The connection with the underlying storage is initialised.
    Meta information is available via the Base variale.
    Data retrieval is facilitated via the session object

    :return:
    """
    global session, Base

    engine = create_engine(URL(**GOB_DB))

    Base = automap_base()
    Base.prepare(engine, reflect=True)
    session = Session(engine)


def _get_table_and_model(collection_name):
    """Table and Model

    Utility method to retrieve the Table and Model for a specific collection

    :param collection_name:
    :return:
    """
    return getattr(Base.classes, collection_name), GOBModel().get_model(collection_name)


def _to_gob_value(entity, field, spec):
    gob_type = get_gob_type(spec['type'])
    entity_value = getattr(entity, field)
    gob_value = gob_type.from_value(entity_value)

    return gob_value


def _entity_to_dict(entity, model):
    """Entity - Dictionary conversion

    Converts an entity to a dictionary.
    The model is used to extract only the public attributes of the entity.

    :param entity:
    :param model:
    :return:
    """

    return {k: _to_gob_value(entity, k, v) for k, v in model['fields'].items()}


def get_entities(collection_name, offset, limit):
    """Entities

    Returns the list of entities within a collection.
    Starting at offset (>= 0) and limiting the result to <limit> items

    :param collection_name:
    :param offset:
    :param limit:
    :return:
    """
    assert(session and Base)
    table, model = _get_table_and_model(collection_name)

    all_entities = session.query(table)

    all_count = all_entities.count()

    entities = [
        _entity_to_dict(entity, model)
        for entity in all_entities.offset(offset).limit(limit).all()
    ]

    return (entities, all_count)


def get_entity(collection_name, id):
    """Entity

    Returns the entity within the specified collection identied by the id parameter.
    If the entity cannot be found, None is returned

    :param collection_name:
    :param id:
    :return:
    """
    assert(session and Base)
    table, model = _get_table_and_model(collection_name)

    id_field = model['entity_id']

    filter = {
        id_field: id,
        "_date_deleted": None
    }

    entity = session.query(table).filter_by(**filter).one_or_none()

    return _entity_to_dict(entity, model) if entity else None
