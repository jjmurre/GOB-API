import traceback

from gobcore.typesystem import fully_qualified_type_name, GOB
from typing import Tuple, List

from gobapi.storage import dump_entities

from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.model.relations import get_relation_name
from gobcore.datastore.factory import DatastoreFactory

from gobapi.dump.config import SKIP_RELATIONS, UNIQUE_ID, UNIQUE_REL_ID
from gobapi.dump.sql import _create_schema, _create_table, _rename_table, _delete_table
from gobapi.dump.sql import _create_indexes, _create_index, get_max_eventid, get_count as get_dst_count
from gobapi.dump.csv import csv_entities
from gobapi.dump.csv_stream import CSVStream
from gobapi.storage import get_entity_refs_after, get_table_and_model, get_max_eventid as get_src_max_eventid, \
    get_count as get_src_count

from gobapi.dump.sql import to_sql_string_value

STREAM_PER = 10000              # Stream per STREAM_PER lines
COMMIT_PER = 10 * STREAM_PER    # Commit once per COMMIT_PER lines
BUFFER_PER = 50000              # Copy read buffer size

MAX_SYNC_ITEMS = 500000         # Maximum number of items to sync before switching to full dump


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

        self.datastore = DatastoreFactory.get_datastore(config['db'])
        self.datastore.connect()

        self.schema = self._get_dst_schema(config, catalog_name, collection_name)

        _, self.model = get_table_and_model(catalog_name, collection_name)

        # Set attributes for create_table
        self.model['catalog'] = catalog_name
        self.model['collection'] = collection_name

    def _get_dst_schema(self, config: dict, catalog_name: str, collection_name: str):
        """Returns schema from config if set, otherwise catalog_name. If catalog_name is 'rel', return the catalog_name
        that owns the relation.

        :param config:
        :param catalog_name:
        :param collection_name:
        :return:
        """
        schema = config.get('schema', catalog_name)

        if schema == 'rel':
            # Schema is the catalog name from the catalog that owns this relation
            return GOBModel().get_catalog_from_abbr(collection_name.split('_')[0])['name']

        return schema

    def _table_exists(self, table_name: str) -> bool:
        """Checks if table_name exist"""

        query = f"SELECT EXISTS (" \
                f" SELECT 1" \
                f" FROM information_schema.tables" \
                f" WHERE table_schema='{self.schema}' AND table_name='{table_name}'" \
                f")"
        result = self._query(query)
        return next(result)[0]

    def _table_empty(self, table_name: str) -> bool:
        query = f"SELECT * FROM {self.schema}.{table_name} LIMIT 1"
        result = self._query(query)

        try:
            next(result)
        except StopIteration:
            return True
        return False

    def _get_columns(self, table_name: str) -> List[Tuple[str, str]]:
        """Returns a list of tuples (column_name, column_type) for given table_name. """

        query = f"SELECT column_name, udt_name " \
                f"FROM information_schema.columns " \
                f"WHERE table_schema='{self.schema}' AND table_name='{table_name}'"

        result = self._query(query)
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

    def _copy_table_into(self, src_table: str, dst_table: str, ids_to_skip) -> None:
        """Copies rows of src_table into dst_table

        """
        where = ""
        if ids_to_skip:
            # Only copy the rows that have not changed
            if self.catalog_name == "rel":
                unique_id = UNIQUE_REL_ID
            else:
                unique_id = UNIQUE_ID
            ids_to_skip_sql = ",".join([to_sql_string_value(id) for id in ids_to_skip])
            where = f"WHERE {unique_id} NOT IN ({ids_to_skip_sql})"

        query = f'INSERT INTO "{self.schema}"."{dst_table}" SELECT * FROM "{self.schema}"."{src_table}" {where}'
        self._execute(query)

    def _get_max_eventid(self, table_name: str):
        """Get max eventid from table_name

        """
        result = self._query(get_max_eventid(self.schema, table_name))
        max_eventid = next(result)[0]

        return max_eventid

    def _count_src(self):
        """
        Get number of rows in source table

        """
        return get_src_count(self.catalog_name, self.collection_name)

    def _count_dst(self):
        """
        Get number of rows in destination table

        """
        result = self._query(get_dst_count(self.schema, self.collection_name))
        return next(result)[0]

    def _max_eventid_dst(self):
        if self._table_exists(self.collection_name):
            return self._get_max_eventid(self.collection_name)
        return None

    def _max_eventid_src(self):
        return get_src_max_eventid(self.catalog_name, self.collection_name)

    def _prepare_destination(self):
        yield f"Create schema {self.schema} if not exists\n"
        create_schema = _create_schema(self.schema)
        self._execute(create_schema)

        yield f"Create tmp table {self.tmp_collection_name}\n"
        create_table = _create_table(self.schema, self.catalog_name, self.tmp_collection_name, self.model)
        self._execute(create_table)

    def _rename_tmp_table(self):
        yield f"Rename {self.tmp_collection_name} to {self.collection_name}\n"
        rename_table = _rename_table(self.schema, current_name=self.tmp_collection_name, new_name=self.collection_name)
        self._execute(rename_table)

    def _delete_tmp_table(self):
        delete_table = _delete_table(self.schema, self.tmp_collection_name)
        self._execute(delete_table)

    def _create_indexes(self, model):
        """
        Create default indexes for the given collection

        :param model:
        :return:
        """
        for index in _create_indexes(model):
            yield f"Create index on {index['field']}\n"
            self._execute(_create_index(self.schema, self.collection_name, **index))

    def _dump_entities_to_table(self, entities, model):
        connection = self.datastore.connection
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

        if not full_dump:
            # Try sync dump
            dst_max_eventid = yield from self._get_dst_max_eventid()
            if dst_max_eventid:
                # Get all source ids that have been updated or added lately
                source_ids_to_update = get_entity_refs_after(self.catalog_name, self.collection_name, dst_max_eventid)
                if source_ids_to_update:
                    yield "Have earlier dump, sync dump\n"
                else:
                    count_src = self._count_src()
                    count_dst = self._count_dst()
                    yield f"Compare counts: src {count_src:,}, dst {count_dst:,}\n"
                    if count_src == count_dst:
                        yield f"Collection is up-to-date, no actions necessary\n"
                        self._delete_tmp_table()
                        return
                    else:
                        yield "Collection counts don't match. Forcing full dump\n"
                        full_dump = True
            else:
                # No remote event id, force full dump
                full_dump = True

        if full_dump:
            # Full write of all entities
            entities, model = yield from self._full_dump()
        else:
            # Sync updated and new entities
            entities, model = yield from self._sync_dump(dst_max_eventid, source_ids_to_update)

        yield from self._dump_entities_to_table(entities, model)
        yield from self._rename_tmp_table()
        yield from self._create_indexes(model)

    def _ref(self, rel_alias: str, with_seqnr: bool):
        """Returns ref expression for a relation with alias :rel_alias: with or without seqnr

        :param rel_alias:
        :param dst_has_states:
        :return:
        """
        return f"{rel_alias}.dst_id || '_' || {rel_alias}.dst_volgnummer" \
            if with_seqnr else f"{rel_alias}.dst_id"

    def create_utility_view(self):
        """Creates view with utility columns for relating without relation table

        View contains all columns from the main table, plus the RELATION_id, RELATION_VOLGNUMMER, RELATION_ref
        and RELATION_bronwaarde columns for each RELATION (for example ligt_in_buurt_id, ligt_in_buurt_ref and
        ligt_in_buurt_bronwaarde)

        :return:
        """
        yield f"Creating view\n"

        main_alias = self.model['abbreviation'].lower()
        src_has_states = self.model.get('has_states', False)

        # Collect all necessary joins and select statements
        joins = []
        selects = [f'{main_alias}.*']

        for relation in self.model['references'].keys():
            # Add a join and selects for each relation
            relation_name = get_relation_name(GOBModel(), self.catalog_name, self.collection_name, relation)

            if not relation_name:
                # Undefined relation
                continue

            if not self._table_exists(relation_name) or self._table_empty(relation_name):
                yield f"Excluding relation {relation_name} from view because table does not exist or empty\n"
                continue

            relation_table = f'{self.catalog_name}.{relation_name}'

            # Determine if ManyReference and if destination has states
            src_field = self.model['all_fields'].get(relation)
            dst_catalog_name, dst_collection_name = GOBModel().split_ref(src_field['ref'])
            dst_has_states = GOBModel().has_states(dst_catalog_name, dst_collection_name)
            is_many = src_field['type'] == fully_qualified_type_name(GOB.ManyReference)

            on = f'{relation}.src_id = {main_alias}.{FIELD.ID}' + (
                f' and {relation}.src_volgnummer = {main_alias}.{FIELD.SEQNR}' if src_has_states else ''
            )

            if is_many:
                # For a ManyReference, we need to aggregate the values in an array
                join = f"""
left join (
    -- Aggregates id, volgnummer and ref for {relation} per src object. bronwaarde is already in the src table
    select
        rel.src_id,
        {'rel.src_volgnummer,' if src_has_states else ''}
        array_agg(rel.dst_id) dst_id,
        {'array_agg(rel.dst_volgnummer) dst_volgnummer,' if dst_has_states else ''}
        array_agg({self._ref('rel', dst_has_states)}) "ref"
    from {relation_table} rel
    group by rel.src_id{', rel.src_volgnummer' if src_has_states else ''}
) {relation} on {on}
"""
                selects.append(f'{relation}.ref {relation}_ref')
            else:
                # For a single Reference we expect one row from the relation table
                join = f"left join {relation_table} {relation} on {on}"
                selects.append(f'{self._ref(relation, dst_has_states)} {relation}_ref')

            joins.append(join)
            selects += [
                f'{relation}.dst_id {relation}_id',
            ]

            if dst_has_states:
                selects += [f'{relation}.dst_volgnummer {relation}_volgnummer']

        # Build query based on collected joins and selects
        NEWLINE = '\n'
        query = f"""
select {f',{NEWLINE}       '.join(selects)}
from {self.catalog_name}.{self.collection_name} {main_alias}
{f'{NEWLINE}'.join(joins)}
"""
        # Create the view
        viewname = f'{self.catalog_name}.v_{self.collection_name}'
        self._execute(f"drop view if exists {viewname}")
        self._execute(f"create view {viewname} as {query}")

        yield f"Utility view {viewname} created\n"

    def _get_dst_max_eventid(self):
        """
        Get the max event id in the destination database

        The dst max-event-id is used for synchronisation
        if synchronisation is not possible, None is returned

        :return:
        """
        try:
            dst_max_eventid = self._max_eventid_dst()
            src_max_eventid = self._max_eventid_src()
        except Exception:
            dst_max_eventid = None
            src_max_eventid = None

        yield f"Compare last event id's, src {src_max_eventid}, dst {dst_max_eventid}\n"
        if None in [dst_max_eventid, src_max_eventid]:
            yield "No last event id(s) found. Forcing full dump\n"
        elif src_max_eventid < dst_max_eventid:
            yield f"Max event id dst {dst_max_eventid} is greater than src {src_max_eventid}. Forcing full dump\n"
        elif not self._table_columns_equal(self.collection_name, self.tmp_collection_name):
            yield "Columns have changed. Forcing full dump\n"
        else:
            return dst_max_eventid

    def _dump_entities(self, **kwargs):
        """
        Dump entities ordered by last_event ascending

        Optionally provide a filter argument to partial dump entities
        :param kwargs:
        :return:
        """
        return dump_entities(self.catalog_name, self.collection_name, **kwargs, order_by=FIELD.LAST_EVENT)

    def _full_dump(self):
        """
        Full copy of the source data, ordered by last_event
        :return:
        """
        yield "Do full dump\n"
        return self._dump_entities()

    def _sync_dump(self, dst_max_eventid, source_ids_to_update):
        """
        Sync data with updated and new source data.
        If too many items need to be synced, fallback to full dump

        :param dst_max_eventid:
        :param source_ids_to_update:
        :return:
        """
        nr_items_to_sync = len(source_ids_to_update)
        if nr_items_to_sync > MAX_SYNC_ITEMS:
            yield f"Collection is too far behind: {nr_items_to_sync}, do full dump\n"
            # Add all items
            filter = None
        else:
            # Sync outdated or new items
            yield f"Collection is behind, sync {len(source_ids_to_update)} items\n"

            # Copy existing data in tmp table and skip all outdated entities
            self._copy_table_into(self.collection_name, self.tmp_collection_name, source_ids_to_update)

            # Add new and updated entities
            filter = self._filter_last_events_lambda(dst_max_eventid)

        return self._dump_entities(filter=filter)

    def _query(self, query: str):
        return self.datastore.query(query)

    def _execute(self, query: str):
        self.datastore.execute(query)


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

        if catalog_name != 'rel':
            yield from dumper.create_utility_view()

        yield "Export completed\n"
    except Exception as e:
        print("Dump failed", traceback.format_exc(limit=-5))
        yield f"ERROR: Dump failed - {str(e)}\n"
