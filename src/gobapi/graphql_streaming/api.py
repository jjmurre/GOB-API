from flask import request

from gobapi.session import get_session
from sqlalchemy.sql import text

from gobapi.graphql_streaming.graphql2sql.graphql2sql import GraphQL2SQL, NoAccessException
from gobapi.graphql_streaming.response_custom import GraphQLCustomStreamingResponseBuilder
from gobapi.worker.response import WorkerResponse

import json


class GraphQLStreamingApi():

    def entrypoint(self):
        # Compatible with plain GraphQL endpoint
        query = request.args.get('query')
        if not query:
            # Compatible with existing GOB export code
            request_data = json.loads(request.data.decode('utf-8'))
            query = request_data['query']
        graphql2sql = GraphQL2SQL(query)
        try:
            sql = graphql2sql.sql()
        except NoAccessException as e:
            return "Forbidden", 403
        session = get_session()
        # use an ad-hoc Connection and stream results (instead of pre-buffered)
        result_rows = session.connection().execution_options(stream_results=True).execute(text(sql))

        response_builder = \
            GraphQLCustomStreamingResponseBuilder(result_rows,
                                                  graphql2sql.relations_hierarchy,
                                                  graphql2sql.selections,
                                                  request_args=request.args)

        return WorkerResponse.stream_with_context(response_builder, mimetype='application/x-ndjson')
