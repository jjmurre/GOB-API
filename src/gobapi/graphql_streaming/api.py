from flask import request, Response
from gobapi.session import get_session
from sqlalchemy.sql import text

from gobcore.exceptions import GOBException
from gobcore.model.metadata import FIELD

from gobapi.graphql_streaming.graphql2sql.graphql2sql import GraphQL2SQL
from gobapi.response import stream_response, _dict_to_camelcase

import json


class GraphQLStreamingResponseBuilder:
    """GraphQLStreamingResponseBuilder builds a GraphQL-like response from the database result.

    The GraphQL-like response omits the encapsulating object so that a list of objects suitable for streaming remains.

    For example:

    "meetboutenMetingen": {
      "edges": [
        {
          "node": {
            "identificatie": "A"
          }
        },
        {
          "node": {
            "identificatie": "B"
          }
        }
      ]
    }

    would be a normal GraphQL result. This response build only returns the list of edges, so that the response looks
    like this:

    {
        "node": {
            "identificatie": "A"
        }
    }
    {
        "node": {
            "identificatie": "B"
        }
    }

    This response builder combines multiple database result rows with the same gobid into one entity (api result).
    Joins on the database level may create multiple result rows for one entity; this is undone. The resulting objects
    are nested with nested objects positioned under the correct parent objects.

    Class is meant to be used as an iterator.
    """

    def __init__(self, rows, relations_hierarchy: dict, selections: dict):
        self.rows = rows
        self.relations_hierarchy = relations_hierarchy
        self.selections = selections
        self.last_id = None

    def _to_node(self, obj: dict):
        return {
            "node": obj
        }

    def _add_relation(self, insert_position: dict, relation_name: str):
        if relation_name not in insert_position:
            insert_position[relation_name] = {"edges": []}

    def _create_broninfo(self, src_values: dict):
        return {k: v for k, v in src_values.items() if k not in [FIELD.REFERENCE_ID, FIELD.SEQNR, FIELD.SOURCE_VALUE]}

    def _add_sourcevalues_to_row(self, row: dict):
        """Merges the source values as returned from the query with the actual relations, so the source values
        (bronwaarde, broninfo dict) show up at the appropriate place in the output.

        :param row:
        :return:
        """

        for relation, requested in self.requested_sourcevalues.items():
            src_key = '_src' + relation[0].upper() + relation[1:]
            relation_key = '_' + relation

            if FIELD.SOURCE_VALUE in requested:
                row[relation_key][FIELD.SOURCE_VALUE] = row[src_key][FIELD.SOURCE_VALUE]

            if FIELD.SOURCE_INFO in requested:
                row[relation_key][FIELD.SOURCE_INFO] = self._create_broninfo(row[src_key])

            del row[src_key]

    def _add_row_to_entity(self, row: dict, entity: dict):
        """Adds the data from a result row to entity

        :param row:
        :param entity:
        :return:
        """

        # Dict containing references to the relations contained in this row, so that when we are trying to insert
        # nested related objects in the entity, we don't have to look up the correct parent anymore.
        row_relations = {}
        for relation_name in self.evaluation_order:
            parent = self.relations_hierarchy[relation_name]

            row_relation = row['_' + relation_name]

            if parent == self.root_relation:
                insert_position = entity
            elif parent in row_relations:
                insert_position = row_relations[parent]
            else:
                insert_position = None
                continue

            self._add_relation(insert_position, relation_name)

            if not row_relation:
                continue

            # Find correct parent for this relation, only if GOBID is set (otherwise this relation can't be identified
            # and a new item should be added)
            item = [rel for rel in insert_position[relation_name]['edges']
                    if row_relation[FIELD.GOBID] is not None and rel['node'][FIELD.GOBID] == row_relation[FIELD.GOBID]]

            if item:
                row_relations[relation_name] = item[0]['node']
            else:
                insert_position[relation_name]['edges'].append(self._to_node(row_relation))
                row_relations[relation_name] = row_relation

    def _build_entity(self, collected_rows: list):
        """Builds entity iteratively out of the collected result rows.

        :return:
        """
        if len(collected_rows) == 0:
            return

        # Fill result with everything except relations and technical attributes
        result = {k: v for k, v in collected_rows[0].items() if not k.startswith('_')}

        for row in collected_rows:
            self._add_sourcevalues_to_row(row)
            self._add_row_to_entity(row, result)

        # Clear gobids from result set
        self._clear_gobids(collected_rows)

        return self._to_node(result)

    def _clear_gobids(self, collected_rows: list):
        """Clears gobids from collected_rows

        :return:
        """
        for row in collected_rows:
            relations = {k[1:]: v for k, v in row.items() if k[1:] in self.relations_hierarchy.keys()}

            for relation in relations.values():
                if relation and FIELD.GOBID in relation:
                    del relation[FIELD.GOBID]

    def _get_requested_sourcevalues(self):
        """Determines per relation which sourcevalues (bronwaarde or broninfo, or both) are requested.

        :return:
        """
        result = {}
        for relation, selection in self.selections.items():
            result[relation] = [v for v in [FIELD.SOURCE_VALUE, FIELD.SOURCE_INFO] if v in selection['fields']]
        return result

    def __iter__(self):
        """Main method. Use class as iterator.

        Loops through database result rows (as passed in the constructor), collects all result rows belonging to the
        same object (entity) and merges these rows back into one object with nested relations.

        :return:
        """
        self.evaluation_order, self.root_relation = self._determine_relation_evaluation_order()
        self.requested_sourcevalues = self._get_requested_sourcevalues()

        collected_rows = []
        for row in self.rows:
            # Collect entities with the same FIELD.GOBID in collected_rows. These entities together represent
            # one entity, but were returned from the database as multiple rows as a result of joins.
            # When all entities with the same GOBID are collected, self.build_entity() is called to merge the rows
            # back into one entity.

            row = _dict_to_camelcase(dict(row))
            built_entity = None

            if row[FIELD.GOBID] != self.last_id and self.last_id is not None:
                # Build entity when all rows of same GOBID are collected
                built_entity = self._build_entity(collected_rows)
                collected_rows = []

            collected_rows.append(row)
            self.last_id = row[FIELD.GOBID]

            if built_entity:
                yield stream_response(built_entity) + "\n"

        # Return last entity in pipeline
        built_entity = self._build_entity(collected_rows)

        if built_entity:
            yield stream_response(built_entity) + "\n"

    def _determine_relation_evaluation_order(self):
        """Determines the order in which we should evaluate relations from the root of the entity.

        :return:
        """
        relations = list(self.relations_hierarchy.keys())
        root_relation = [k for k, v in self.relations_hierarchy.items() if v is None][0]

        order = [root_relation]
        relations.remove(root_relation)

        while len(relations):
            extract = [k for k, v in self.relations_hierarchy.items() if v in order and k in relations]

            if len(extract) == 0:
                raise GOBException("This should not be possible. Not sure what you want me to do now?")

            order.extend(extract)
            relations = [relation for relation in relations if relation not in extract]

        order.remove(root_relation)

        return order, root_relation


class GraphQLStreamingApi():

    def entrypoint(self):
        request_data = json.loads(request.data.decode('utf-8'))
        graphql2sql = GraphQL2SQL(request_data['query'])
        sql = graphql2sql.sql()
        session = get_session()
        result_rows = session.execute(text(sql))

        response_builder = GraphQLStreamingResponseBuilder(result_rows, graphql2sql.relations_hierarchy,
                                                           graphql2sql.selections)

        return Response(response_builder, mimetype='application/x-ndjson')
