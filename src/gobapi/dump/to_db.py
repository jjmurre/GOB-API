from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from gobapi.storage import dump_entities

from gobcore.model import GOBModel
from gobcore.model.relations import get_relation_name

from gobapi.dump.sql import _create_schema, _create_table
from gobapi.dump.csv import csv_entities
from gobapi.dump.csv_stream import CSVStream

STREAM_PER = 5000             # Stream per STREAM_PER lines
COMMIT_PER = 10 * STREAM_PER  # Commit once per COMMIT_PER lines


def _dump_to_db(schema, catalog_name, collection_name, entities, model, config):

    engine = config['engine']

    yield f"Create schema if not exists\n"
    create_schema = _create_schema(schema)
    result = engine.execute(create_schema)
    result.close()

    yield f"Create table\n"
    create_table = _create_table(schema, catalog_name, collection_name, model)
    result = engine.execute(create_table)
    result.close()

    connection = engine.raw_connection()
    stream = CSVStream(csv_entities(entities, model), STREAM_PER)

    with connection.cursor() as cursor:
        yield "Export data\n"
        commit = COMMIT_PER
        while stream.has_items():
            stream.reset_count()
            cursor.copy_expert(
                sql=f"COPY {schema}.{collection_name} FROM STDIN DELIMITER ';' CSV HEADER;",
                file=stream,
                size=40960
            )
            yield f"{stream.total_count}\n"
            if stream.total_count >= commit:
                connection.commit()
                commit += COMMIT_PER

    yield(f"Exported {stream.total_count} rows\n")
    connection.commit()
    connection.close()


def dump_to_db(catalog_name, collection_name, config):
    engine = create_engine(URL(**config['db']))
    config['engine'] = engine

    yield f"Export {catalog_name} {collection_name}\n"
    schema = catalog_name

    try:
        # Start with catalog - collection
        entities, model = dump_entities(catalog_name, collection_name)
        yield from _dump_to_db(schema, catalog_name, collection_name, entities, model, config)

        # Then process all relations in the given collection
        for relation in [k for k in model['references'].keys()] + [k for k in model['very_many_references'].keys()]:
            yield f"Export {catalog_name} {collection_name} {relation}\n"

            relation_name = get_relation_name(GOBModel(), catalog_name, collection_name, relation)
            entities, model = dump_entities("rel", relation_name)
            yield from _dump_to_db(schema, "rel", relation_name, entities, model, config)

        yield "Export completed\n"
    except Exception as e:
        yield f"ERROR: Export failed - {str(e)}\n"

    engine.dispose()
