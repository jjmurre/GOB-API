import traceback

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from typing import Tuple, List

from gobapi.storage import dump_entities

from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.model.relations import get_relation_name

from gobapi.dump.config import SKIP_RELATIONS, UNIQUE_ID, UNIQUE_REL_ID
from gobapi.dump.sql import _create_schema, _create_table, _rename_table
from gobapi.dump.sql import _create_indexes, _create_index, get_max_eventid
from gobapi.dump.csv import csv_entities
from gobapi.dump.csv_stream import CSVStream
from gobapi.storage import get_entity_refs_after, get_table_and_model, get_max_eventid as get_src_max_eventid

from gobapi.dump.sql import to_sql_string_value

STREAM_PER = 10000              # Stream per STREAM_PER lines
COMMIT_PER = 10 * STREAM_PER    # Commit once per COMMIT_PER lines
BUFFER_PER = 50000              # Copy read buffer size


class DbDumper:
    """Dumps given collection and possibly relations owned by the collection to the database passed in config.

    By default the collection is dumped in a table with collection_name in a schema named after the catalog_name.

    Config should have at least key 'db' with a dict that can be passed to SQLAlchemy's URL.
    Optional keys in config:
    - schema: alternative schema name for export, defaults to catalog_name

    Usage: Instantiate object and call dump_to_db() on instance

    dumper = DbDumper('catalog_name', 'collection_name', {'db': (db config here)})
    dumper.dump_to_db()

    dump_to_db always tries to synchronise, except when full_dump=True is passed to dump_to_db.
    """

    def __init__(self, catalog_name: str, collection_name: str, config: dict):
        self.catalog_name = catalog_name
        self.collection_name = collection_name
        self.tmp_collection_name = f"tmp_{collection_name}"
        self.engine = create_engine(URL(**config['db']))
        self.config = config
        self.config['engine'] = self.engine
        self.schema = config.get('schema', catalog_name)

        _, self.model = get_table_and_model(catalog_name, collection_name)

        # Set attributes for create_table
        self.model['catalog'] = catalog_name
        self.model['collection'] = collection_name

    def _table_exists(self, table_name: str) -> bool:
        """Checks if table_name exist"""

        query = f"SELECT EXISTS (" \
                f" SELECT 1" \
                f" FROM information_schema.tables" \
                f" WHERE table_schema='{self.schema}' AND table_name='{table_name}'" \
                f")"
        result = self.engine.execute(query)
        return next(result)[0]

    def _get_columns(self, table_name: str) -> List[Tuple[str, str]]:
        """Returns a list of tuples (column_name, column_type) for given table_name. """

        query = f"SELECT column_name, udt_name " \
                f"FROM information_schema.columns " \
                f"WHERE table_schema='{self.schema}' AND table_name='{table_name}'"

        result = self.engine.execute(query)
        return [tuple(row) for row in result]

    def _table_columns_equal(self, table_a: str, table_b: str) -> bool:
        """Returns boolean value indicating whether the columns of table_a and table_b are matching based on names,
        ordering and type.

        Only checks names and SQL types. Does not include column length, precision etc.

        (Not that a different ordering returns False. In our use case this is not a problem, as this simplifies the
        rest of the code. We don't have to supply columns explicitly when copying data between tables. Normally it
        would not occur for two tables here to have the same columns in a different order. If that so happens, it
        is probably a good idea to clear the tables anyway.)
        """
        return self._get_columns(table_a) == self._get_columns(table_b)

    def _copy_table_into(self, src_table: str, dst_table: str) -> None:
        """Copies rows of src_table into dst_table

        """
        query = f'INSERT INTO "{self.schema}"."{dst_table}" SELECT * FROM "{self.schema}"."{src_table}"'
        self.engine.execute(query)

    def _get_max_eventid(self, table_name: str):
        """Get max eventid from table_name

        """
        result = self.engine.execute(get_max_eventid(self.schema, table_name))
        max_eventid = next(result)[0]

        return max_eventid

    def _delete_dst_entities(self, table_name: str, refs: list):
        refs_sql = ",".join([to_sql_string_value(ref) for ref in refs])
        if self.catalog_name == "rel":
            unique_id = UNIQUE_REL_ID
        else:
            unique_id = UNIQUE_ID
        query = f'DELETE FROM "{self.schema}"."{table_name}" WHERE {unique_id} IN ({refs_sql})'

        result = self.engine.execute(query)
        return result

    def _max_eventid_dst(self):
        if self._table_exists(self.collection_name):
            return self._get_max_eventid(self.collection_name)
        return None

    def _max_eventid_src(self):
        return get_src_max_eventid(self.catalog_name, self.collection_name)

    def _prepare_destination(self):
        yield f"Create schema {self.schema} if not exists\n"
        create_schema = _create_schema(self.schema)
        result = self.engine.execute(create_schema)
        result.close()

        yield f"Create tmp table {self.tmp_collection_name}\n"
        create_table = _create_table(self.schema, self.catalog_name, self.tmp_collection_name, self.model)
        result = self.engine.execute(create_table)
        result.close()

    def _rename_tmp_table(self):
        yield f"Rename {self.tmp_collection_name} to {self.collection_name}\n"
        rename_table = _rename_table(self.schema, current_name=self.tmp_collection_name, new_name=self.collection_name)
        result = self.engine.execute(rename_table)
        result.close()

    def _create_indexes(self, model):
        """
        Create default indexes for the given collection

        :param model:
        :return:
        """
        for index in _create_indexes(model):
            yield f"Create index on {index['field']}\n"
            result = self.engine.execute(_create_index(self.schema, self.collection_name, **index))
            result.close()

    def _dump_entities_to_table(self, entities, model):
        connection = self.engine.raw_connection()
        stream = CSVStream(csv_entities(entities, model), STREAM_PER)

        with connection.cursor() as cursor:
            yield "Export data"
            commit = COMMIT_PER
            while stream.has_items():
                stream.reset_count()
                cursor.copy_expert(
                    sql=f"COPY {self.schema}.{self.tmp_collection_name} FROM STDIN DELIMITER ';' CSV HEADER;",
                    file=stream,
                    size=BUFFER_PER
                )

                if stream.total_count >= commit:
                    connection.commit()
                    commit += COMMIT_PER

                    yield f"\n{self.collection_name}: {stream.total_count:,}"
                else:
                    # Let client know we're still working.
                    yield "."

        yield f"\nExported {stream.total_count} rows\n"
        connection.commit()

    def _filter_last_events_lambda(self, max_eventid):
        return lambda table: getattr(table, FIELD.LAST_EVENT) > max_eventid

    def dump_to_db(self, full_dump=False):
        """Runs dump for this instance. Tries to synchronise based on last event by default. Set full_dump=True to
        replace all existing data and ignore synchronisation based on events.
        """
        yield from self._prepare_destination()

        try:
            dst_max_eventid = None if full_dump else self._max_eventid_dst()
            src_max_eventid = self._max_eventid_src()
        except Exception:
            yield "No last event id found. Forcing full dump\n"
            dst_max_eventid = None
            src_max_eventid = None

        if src_max_eventid is not None and dst_max_eventid is not None and src_max_eventid < dst_max_eventid:
            yield "Max event id in dst table is greater than max eventid in src. Forcing full dump\n"

            dst_max_eventid = None

        if dst_max_eventid is not None and self._table_columns_equal(self.collection_name, self.tmp_collection_name):
            yield "Have earlier dump and columns have not changed. Do sync dump\n"
            # Already have records in destination table, and model has not changed. Do sync dump

            self._copy_table_into(self.collection_name, self.tmp_collection_name)

            source_ids_to_update = get_entity_refs_after(self.catalog_name, self.collection_name, dst_max_eventid)

            if source_ids_to_update:
                yield f"Delete {len(source_ids_to_update)} entities from dst database that are going to be updated\n"
                self._delete_dst_entities(self.tmp_collection_name, source_ids_to_update)

            entities, model = dump_entities(
                self.catalog_name,
                self.collection_name,
                filter=self._filter_last_events_lambda(dst_max_eventid),
                order_by=FIELD.LAST_EVENT
            )
        else:
            yield "Do full dump\n"
            entities, model = dump_entities(self.catalog_name, self.collection_name, order_by=FIELD.LAST_EVENT)

        yield from self._dump_entities_to_table(entities, model)
        yield from self._rename_tmp_table()
        yield from self._create_indexes(model)


def _dump_relations(catalog_name, collection_name, config):
    """Dumps relations for catalog_name, collection_name """
    config['schema'] = catalog_name

    _, model = get_table_and_model(catalog_name, collection_name)

    for relation in [k for k in model['references'].keys()]:
        relation_name = get_relation_name(GOBModel(), catalog_name, collection_name, relation)

        if not relation_name or relation_name in SKIP_RELATIONS:
            # relation_name is None when relation does not exist (yet)
            yield f"Skipping {catalog_name} {collection_name} {relation}\n"
            continue

        yield f"Export {catalog_name} {collection_name} {relation}\n"

        rel_dumper = DbDumper('rel', relation_name, config)
        yield from rel_dumper.dump_to_db(full_dump=True)


def dump_to_db(catalog_name, collection_name, config):
    try:
        dumper = DbDumper(catalog_name, collection_name, config)
        yield from dumper.dump_to_db()

        if config.get('include_relations', True):
            yield from _dump_relations(catalog_name, collection_name, config)

        yield "Export completed\n"
    except Exception as e:
        print("Dump failed", traceback.format_exc(limit=-5))
        yield f"ERROR: Dump failed - {str(e)}\n"
