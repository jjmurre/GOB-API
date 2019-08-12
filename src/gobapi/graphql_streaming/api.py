from flask import request, Response
from gobapi.session import get_session
from sqlalchemy.sql import text

from gobapi.graphql_streaming.graphql2sql.graphql2sql import GraphQL2SQL
from gobapi.response import ndjson_entities

import json
import re


class GraphQLStreamingApi():

    def snake_to_camelcase(self, s: str) -> str:
        """Returns camelcase variant of snakecase string. Leaves leading underscore in place

        :param s:
        :return:
        """
        return re.sub(r'(\w)_([a-z])', lambda x: x.group(1) + x.group(2).upper(), s)

    def dict_keys_to_camelcase(self, d: dict) -> dict:
        """Changes all dict keys to camelcase recursively. Keeps leading underscores in dicct keys.

        :param d:
        :return:
        """
        if not isinstance(d, dict):
            return d

        result = {}
        for k, v in d.items():
            if isinstance(v, dict):
                val = self.dict_keys_to_camelcase(v)
            elif isinstance(v, list):
                val = [self.dict_keys_to_camelcase(item) for item in v]
            else:
                val = v
            result[self.snake_to_camelcase(k)] = val
        return result

    def transform_graphql_streaming_result(self, entity: dict):
        """Transforms result from GraphQL streaming. Changes dict keys from snake case to camel case and moves
        relations to _embedded object.

        :param entity:
        :return:
        """
        entity = self.dict_keys_to_camelcase(dict(entity))

        result = {'_embedded': {}}

        for k, v in entity.items():
            if k.startswith('_'):
                result['_embedded'][k[1:]] = v
            else:
                result[k] = v

        return result

    def entrypoint(self):
        unfold = request.args.get('unfold') == 'true'
        request_data = json.loads(request.data.decode('utf-8'))
        sql = GraphQL2SQL.graphql2sql(request_data['query'], unfold)

        session = get_session()
        entities = session.execute(text(sql))

        return Response(ndjson_entities(entities, self.transform_graphql_streaming_result),
                        mimetype='application/x-ndjson')
