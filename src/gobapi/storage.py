"""Storage

This module encapsulates the GOB storage.
The API returns GOB data by calling any of the methods in this module.
By using this module the API does not need to have any knowledge about the underlying storage

"""
import datetime
from collections import defaultdict

from sqlalchemy import create_engine, Table, MetaData, func, and_, or_, Integer, cast
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy_filters import apply_filters
from sqlalchemy.sql import label

from gobcore.model import GOBModel
from gobcore.model.relations import get_relation_name
from gobcore.model.sa.gob import Base
from gobcore.typesystem import get_gob_type, get_gob_type_from_sql_type
from gobcore.model.metadata import PUBLIC_META_FIELDS, PRIVATE_META_FIELDS, FIXED_COLUMNS, FIELD

from gobapi.config import GOB_DB, API_BASE_PATH
from gobapi.session import set_session, get_session

session = None
_Base = None
metadata = None


def connect():
    """Module initialisation

    The connection with the underlying storage is initialised.
    Meta information is available via the Base variale.
    Data retrieval is facilitated via the session object

    :return:
    """
    global session, _Base, metadata

    engine = create_engine(URL(**GOB_DB))
    session = scoped_session(sessionmaker(autocommit=False,
                                          autoflush=False,
                                          bind=engine))
    _Base = automap_base()
    _Base.prepare(engine, reflect=True)

    Base.metadata.bind = engine  # Bind engine to metadata of the base class
    Base.query = session.query_property()  # Used by graphql to execute queries

    metadata = MetaData(engine)

    set_session(session)


def _get_table_and_model(catalog_name, collection_name, view=None):
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
        return getattr(_Base.classes, GOBModel().get_table_name(catalog_name, collection_name)), \
                       GOBModel().get_collection(catalog_name, collection_name)


def _create_reference_link(reference, catalog, collection):
    identificatie = reference.get(FIELD.REFERENCE_ID)
    if identificatie:
        return {'_links': {'self': {'href': f'{API_BASE_PATH}/{catalog}/{collection}/{identificatie}/'}}}
    else:
        return {}


def _create_external_reference_link(entity, field, entity_catalog, entity_collection):
    identificatie = getattr(entity, FIELD.ID)
    field_path = field.replace('_', '-')
    return {'href': f'{API_BASE_PATH}/{entity_catalog}/{entity_collection}/{identificatie}/{field_path}/'}


def _create_reference(entity, field, spec, entity_catalog=None, entity_collection=None):
    """Create an embedded reference

    :param entity: The entity
    :param field: The field a reference is being created for
    :param spec: The field specification
    :param catalog: The catalog of the entity
    :param collection: The collection of the entity
    :return:
    """
    # Get the dict or array of dicts from a (Many)Reference field
    embedded = _to_gob_value(entity, field, spec).to_db

    if embedded is not None and spec['ref'] is not None:
        catalog, collection = spec['ref'].split(':')
        if spec['type'] == 'GOB.ManyReference':
            for reference in embedded:
                reference.update(_create_reference_link(reference, catalog, collection))
        else:
            embedded.update(_create_reference_link(embedded, catalog, collection))

    return embedded


def _to_gob_value(entity, field, spec):
    if isinstance(spec, dict):
        gob_type = get_gob_type(spec['type'])
    else:
        gob_type = get_gob_type_from_sql_type(spec)

    entity_value = getattr(entity, field, None)
    gob_value = gob_type.from_value(entity_value)

    return gob_value


def _get_convert_for_state(model, fields=[], private_attributes=False):
    """Get the entity to dict convert function for GOBModels with state

    The model is used to extract only the public attributes of the entity,
    fields can be used to only select certain attributes

    :param model:
    :param fields:
    :return:
    """
    def convert(entity):
        hal_entity = {k: _to_gob_value(entity, k, v) for k, v in items}
        return hal_entity

    # Select all attributes except if it's a reference, unless a specific list was passed
    if not fields:
        fields = [field for field in model['fields'].keys()
                  if field not in model['references'].keys()
                  and (not field.startswith('_') or private_attributes)]
    attributes = {k: v for k, v in model['fields'].items() if k in fields}
    items = list(attributes.items())
    return convert


def _get_convert_for_model(catalog, collection, model, meta={}, private_attributes=False):
    """Get the entity to dict convert function for GOBModels

    The model is used to extract only the public attributes of the entity.

    :param entity:
    :param model:
    :return:
    """
    def convert(entity):
        hal_entity = {k: _to_gob_value(entity, k, v) for k, v in items}

        # Add link to self in each entity
        id = getattr(entity, '_id')
        hal_entity['_links'] = {
            'self': {'href': f'{API_BASE_PATH}/{catalog}/{collection}/{id}/'}
        }
        # Add references to other entities, exclude private_attributes unless specifically requested
        if model['references']:
            hal_entity['_embedded'] = {k: _create_reference(entity, k, v, catalog, collection)
                                       for k, v in model['references'].items()
                                       if k not in model.get('very_many_references', {}).keys()
                                       and ((not k.startswith('_') and not v.get('hidden')) or private_attributes)}

            # Delete embedded from the entity if no references are added
            if not hal_entity['_embedded']:
                del hal_entity['_embedded']

        if model.get('very_many_references'):
            hal_entity['_links'].update({k: _create_external_reference_link(entity, k, catalog, collection)
                                        for k, v in model['very_many_references'].items()})

        return hal_entity
    # Get the attributes which are not a reference, exclude private_attributes unless specifically requested
    attributes = {k: v for k, v in model['fields'].items()
                  if k not in model['references'].keys()
                  and (not k.startswith('_') or private_attributes)
                  and not v.get('hidden')}

    items = list(attributes.items()) + list(meta.items())
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
        hal_entity = {column.name: _to_gob_value(entity, column.name, type(column.type)) for column in columns}

        # Add references to other entities
        if references:
            hal_entity['_embedded'] = {
                v['attribute_name']: _create_reference(entity, k, v) for k, v in references.items()
            }
        return hal_entity

    # Get all metadata or reference fields and filter them from the columns returned by the database view
    metadata_column_list = [k for k in filter.keys()]
    columns = [c for c in table.columns
               if c.name not in metadata_column_list
               and not isReference(c.name)]
    reference_columns = [c for c in table.columns if isReference(c.name)]

    # Create the list of references
    references = {}
    for c in reference_columns:
        '''
        Column name is in the form of '_ref_attribute_name_ctg_cln'
        We need to get the type of reference (ref or mref), the attribute name and
        the reference based on abbreviation. Abbreviations were used to escape the
        column name limit in SQL. The original column name is stored to
        be able to get the data from the row.

        For example: _ref_ligt_in_buurt_gdb_brt will result in:
        attribute_name: ligt_in_buurt
        catalog_abbreviation: gdb
        collection_abbreviation: brt
        ref: gebieden:buurt
        gob_type: GOB.Reference

        This will be used to create an embedded reference in the HAL JSON output
        '''
        # This will result in an array of e.g ['', 'ref', 'ligt', 'in', 'buurt', 'gbd', 'brt']
        column_name_array = c.name.split('_')

        # Join elements that make up the attribute name, position 2 until the third last (e.g. ligt_in_buurt)
        attribute_name = '_'.join(column_name_array[2:-2])

        # Get the abbreviation of the catalog (e.g. gbd) and collection (e.g. brt)
        catalog_abbreviation = str(column_name_array[-2])
        collection_abbreviation = str(column_name_array[-1])

        # Get a reference string by abbreviation (e.g. gebieden:buurten)
        ref = GOBModel().get_reference_by_abbreviations(catalog_abbreviation, collection_abbreviation)
        gob_type = 'GOB.ManyReference' if c.name.startswith('_mref') else 'GOB.Reference'

        # Create the reference specification
        references[c.name] = {
            'attribute_name': attribute_name,
            'type': gob_type,
            'ref': ref
        }
    return convert


def isReference(column_name):
    """ isReference

    Receives a table column_name and checks if it's a reference or many reference based on the
    column name

    Returns a boolean

    :param column_name:
    :return: boolean
    """
    return column_name.startswith(('_ref', '_mref'))


def get_entities(catalog, collection, offset, limit, view=None, reference_name=None, src_id=None):
    """Entities

    Returns the list of entities within a collection.
    Starting at offset (>= 0) and limiting the result to <limit> items

    :param collection_name:
    :param offset:
    :param limit:
    :param view: optional view for the collection
    :param reference_name: optional reference_name, will return entities for a specific relation
    :param src_id: optional e.g. 1234
    :return:
    """
    all_entities, entity_convert = query_reference_entities(catalog, collection, reference_name, src_id) \
        if reference_name else query_entities(catalog, collection, view)

    # For views count is slow on large views
    all_count = all_entities.count() if view is None else None

    # Limit and offset for pagination
    page_entities = all_entities.offset(offset).limit(limit).all()

    entities = [entity_convert(entity) for entity in page_entities]
    return entities, all_count


def query_entities(catalog, collection, view):
    assert _Base
    session = get_session()

    table, model = _get_table_and_model(catalog, collection, view)

    query = session.query(table)
    # Exclude all records with date_deleted
    all_entities = filter_deleted(query, table)

    if view is None:
        # The default result is where expiration date is in the future or empty
        all_entities = filter_active(all_entities, table)

    # Apply filters if defined in model
    try:
        filters = model['api']['filters']
    except (KeyError, TypeError) as e:
        pass
    else:
        all_entities = apply_filters(all_entities, filters)

    if view:
        entity_convert = _get_convert_for_table(table,
                                                {**PUBLIC_META_FIELDS, **PRIVATE_META_FIELDS, **FIXED_COLUMNS})
    else:
        entity_convert = _get_convert_for_model(catalog, collection, model)

    return all_entities, entity_convert


def query_reference_entities(catalog, collection, reference_name, src_id):
    assert _Base
    session = get_session()

    gob_model = GOBModel()

    rel_catalog_name = 'rel'
    rel_collection_name = get_relation_name(gob_model, catalog, collection, reference_name)

    rel_table, rel_model = _get_table_and_model(rel_catalog_name, rel_collection_name)

    dst_catalog_name, dst_collection_name = gob_model.get_collection(
        catalog, collection)['references'][reference_name]['ref'].split(':')

    # Destination table and model
    dst_table, dst_model = _get_table_and_model(dst_catalog_name, dst_collection_name)

    query = session.query(dst_table) \
                   .join(rel_table, dst_table._id == rel_table.dst_id) \
                   .filter(rel_table.src_id == src_id)

    # Exclude all records with date_deleted
    all_entities = filter_deleted(query, dst_table)

    # The default result is where expiration date is in the future or empty
    all_entities = filter_active(all_entities, dst_table)

    entity_convert = _get_convert_for_model(dst_catalog_name, dst_collection_name, dst_model)

    return all_entities, entity_convert


def get_collection_states(catalog, collection):
    """States

    Returns all entities with state from the specified collection

    :param catalog:
    :param collection:
    :return states: A dict containing all entities by _id for easy lookup
    """
    assert _Base
    session = get_session()

    entity, model = _get_table_and_model(catalog, collection)

    # Get the max sequence number for every id + start validity combination
    sub = session.query(getattr(entity, FIELD.ID),
                        getattr(entity, FIELD.START_VALIDITY),
                        label("max_seqnr", func.max(cast(getattr(entity, FIELD.SEQNR), Integer)))
                        )\
        .group_by(FIELD.ID, FIELD.START_VALIDITY)\
        .subquery()

    # Filter the entities to only the highest volgnummer per id + start validity combination
    all_entities = session.query(entity)\
        .join(sub, and_(getattr(sub.c, FIELD.ID) == getattr(entity, FIELD.ID),
                        getattr(sub.c, FIELD.START_VALIDITY) == getattr(entity, FIELD.START_VALIDITY),
                        sub.c.max_seqnr == cast(getattr(entity, FIELD.SEQNR), Integer)))\
        .all()

    states = defaultdict(list)

    if not all_entities:
        return states

    for entity in all_entities:
        states[entity._id].append(entity)
    return states


def get_entity(catalog, collection, id, view=None):
    """Entity

    Returns the entity from the specified collection or the view identied by the id parameter.
    If the entity cannot be found, None is returned

    :param collection_name:
    :param id:
    :param view:
    :return:
    """
    assert _Base
    session = get_session()

    filter = {
        "_id": id,
    }

    table, model = _get_table_and_model(catalog, collection, view)

    query = session.query(table).filter_by(**filter)
    # Exclude all records with date_deleted
    entity = filter_deleted(query, table)

    if view is None:
        # The default result is without deleted items
        entity = filter_active(entity, table)

    # Apply filters if defined in model
    try:
        filters = model['api']['filters']
    except (KeyError, TypeError) as e:
        pass
    else:
        entity = apply_filters(entity, filters)

    entity = entity.one_or_none()
    if view:
        entity_convert = _get_convert_for_table(table,
                                                {**PRIVATE_META_FIELDS, **FIXED_COLUMNS})
    else:
        entity_convert = _get_convert_for_model(catalog, collection, model,
                                                PUBLIC_META_FIELDS, private_attributes=True)

    return entity_convert(entity) if entity else None


def filter_deleted(query, model):
    """Filter a query to exclude records with date deleted

    :param query:
    :param model: The SQLAlchemy model
    :return: query
    """
    # The table can also be a view on a table and doesn't always have _date_deleted
    try:
        query = query.filter(getattr(model, FIELD.DATE_DELETED) == None)  # noqa: E711 (== None)
    except AttributeError:
        pass
    return query


def filter_active(query, model):
    """Filter a query to return only the active records

    :param query:
    :param model: The SQLAlchemy model
    :return: query
    """
    return query.filter(or_(
        getattr(model, FIELD.EXPIRATION_DATE) > datetime.datetime.now(),
        getattr(model, FIELD.EXPIRATION_DATE) == None
    ))  # noqa: E711 (== None)
