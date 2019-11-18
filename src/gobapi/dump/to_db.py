import re

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from gobapi.storage import dump_entities

from gobcore.model import GOBModel
from gobcore.model.relations import get_relation_name

from gobapi.dump.config import get_field_specifications, get_reference_fields, SKIP_RELATIONS
from gobapi.dump.sql import _create_schema, _create_table, _rename_table, _create_index
from gobapi.dump.csv import csv_entities
from gobapi.dump.csv_stream import CSVStream

STREAM_PER = 100               # Stream per STREAM_PER lines
COMMIT_PER = 100 * STREAM_PER  # Commit once per COMMIT_PER lines
BUFFER_PER = 4096              # Copy read buffer size


def _create_indexes(engine, schema, collection_name, model):
    """
    Create default indexes for the given collection

    :param engine:
    :param schema:
    :param collection_name:
    :param model:
    :return:
    """
    indexes = []
    for field, spec in get_field_specifications(model).items():
        if field == model['entity_id'] or re.compile(r"(^|.+_)ref$").match(field):
            indexes.append({'field': field})  # Plain entity id, full entity id (ref) or rel. foreign key (eg dst_ref)
        elif "GOB.Geo" in spec['type']:
            indexes.append({'field': field, 'method': "gist"})  # Spatial index
        elif spec['type'] == "GOB.Reference" and "ref" in get_reference_fields(spec):
            indexes.append({'field': f"{field}_ref"})           # Foreign key index

    for index in indexes:
        yield f"Create index on {index['field']}\n"
        result = engine.execute(_create_index(schema, collection_name, **index))
        result.close()


def _dump_to_db(schema, catalog_name, collection_name, entities, model, config):

    engine = config['engine']

    yield f"Create schema {schema} if not exists\n"
    create_schema = _create_schema(schema)
    result = engine.execute(create_schema)
    result.close()

    # Collect new data in temporary table
    tmp_collection_name = f"tmp_{collection_name}"

    yield f"Create table {collection_name}\n"
    create_table = _create_table(schema, catalog_name, tmp_collection_name, model)
    result = engine.execute(create_table)
    result.close()

    connection = engine.raw_connection()
    stream = CSVStream(csv_entities(entities, model), STREAM_PER)

    with connection.cursor() as cursor:
        yield "Export data"
        commit = COMMIT_PER
        while stream.has_items():
            stream.reset_count()
            cursor.copy_expert(
                sql=f"COPY {schema}.{tmp_collection_name} FROM STDIN DELIMITER ';' CSV HEADER;",
                file=stream,
                size=BUFFER_PER
            )

            if stream.total_count >= commit:
                connection.commit()
                commit += COMMIT_PER

                yield f"\n{collection_name}: {stream.total_count:,}"
            else:
                # Let client know we're still working.
                yield "."

    yield(f"\nExported {stream.total_count} rows\n")
    connection.commit()

    yield f"Finalize table {collection_name}\n"
    rename_table = _rename_table(schema, current_name=tmp_collection_name, new_name=collection_name)
    result = engine.execute(rename_table)
    result.close()

    yield "Create default indexes\n"
    yield from _create_indexes(engine, schema, collection_name, model)

    connection.close()


def dump_to_db(catalog_name, collection_name, config):
    engine = create_engine(URL(**config['db']))
    config['engine'] = engine

    schema = config.get("schema", catalog_name)
    yield f"Export {catalog_name} {collection_name} in schema {schema}\n"

    try:
        entities, model = dump_entities(catalog_name, collection_name)
        yield from _dump_to_db(schema, catalog_name, collection_name, entities, model, config)

        if config.get("include_relations", True):
            # Then process all relations in the given collection
            for relation in [k for k in model['references'].keys()]:
                relation_name = get_relation_name(GOBModel(), catalog_name, collection_name, relation)

                if not relation_name or relation_name in SKIP_RELATIONS:
                    # relation_name is None when relation does not exist (yet)
                    yield f"Skipping {catalog_name} {collection_name} {relation}\n"
                    continue

                yield f"Export {catalog_name} {collection_name} {relation}\n"

                entities, model = dump_entities("rel", relation_name)
                yield from _dump_to_db(schema, "rel", relation_name, entities, model, config)

        yield "Export completed\n"
    except Exception as e:
        yield f"ERROR: Export failed - {str(e)}\n"

    engine.dispose()
