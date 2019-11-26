from antlr4 import InputStream, CommonTokenStream
from gobapi.graphql_streaming.graphql2sql.grammar.GraphQLLexer import GraphQLLexer
from gobapi.graphql_streaming.graphql2sql.grammar.GraphQLParser import GraphQLParser
from gobapi.graphql_streaming.graphql2sql.grammar.GraphQLVisitor import GraphQLVisitor as BaseVisitor
from gobapi.graphql_streaming.resolve import CATALOG_NAME, COLLECTION_NAME
from gobapi.graphql_streaming.utils import to_snake

from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.model.relations import get_relation_name
from gobcore.typesystem import gob_types, is_gob_geo_type


class GraphQLVisitor(BaseVisitor):
    """Visitor for ANTLR4 GraphQL parse tree, generated with the GraphQL.g4 grammar.

    Visitor uses a relationStack to keep track of to which relation the visited selects and arguments belong. Visitor
    also puts the parent of each relation in the relationParents dict.

    Note: This visitor does not implement the full parse tree. The current implementation is the minimal implmentation
    to support parsing of the GraphQL queries GOB currently sends to the endpoint. NotImplementedErrors are raised
    to avoid unexpected behaviour in not-implemented parts of the parse tree.
    """

    def __init__(self):
        self.relationStack = []
        self.selects = {}
        self.relationParents = {}
        self.relationAliases = {}
        self.arguments = {}

    def pushRelationStack(self, relation: str, alias: str):
        """Pushes relation to stack.

        :param relation:
        :return:
        """
        self.relationParents[alias] = self.relationStack[-1] if len(self.relationStack) else None
        self.relationStack.append(alias)
        self.relationAliases[alias] = relation

        if relation not in self.selects:
            self.selects[alias] = {
                'fields': [],
                'arguments': self.arguments,
            }
            self.arguments = {}

    def popRelationStack(self):
        return self.relationStack.pop()

    def addSelectField(self, fieldname: str):
        self.selects[self.relationStack[-1]]['fields'].append(fieldname)

    def addArgument(self, key, value):
        self.arguments[key] = value

    def visitField(self, ctx: GraphQLParser.FieldContext):
        if ctx.arguments():
            self.visitArguments(ctx.arguments())

        if ctx.directives():
            raise NotImplementedError()

        # Alias is ignored for now
        field_name, alias = self.visitFieldName(ctx.fieldName())

        if ctx.selectionSet():
            # Relation
            if field_name in ['edges', 'node']:
                # Ignore stack, just visit
                self.visitSelectionSet(ctx.selectionSet())
            elif field_name == 'pageInfo':
                # Ignore pageInfo
                pass
            else:
                self.pushRelationStack(field_name, alias)
                self.visitSelectionSet(ctx.selectionSet())
                self.popRelationStack()
        else:
            # Normal field
            self.addSelectField(field_name)

    def visitArgument(self, ctx: GraphQLParser.ArgumentContext):
        value = self.visitValueOrVariable(ctx.valueOrVariable())
        name = str(ctx.NAME())
        self.addArgument(name, value)

    def visitValueOrVariable(self, ctx: GraphQLParser.ValueOrVariableContext):
        if ctx.value():
            if isinstance(ctx.value(), GraphQLParser.StringValueContext):
                return self.visitStringValue(ctx.value())
            elif isinstance(ctx.value(), GraphQLParser.NumberValueContext):
                return self.visitNumberValue(ctx.value())
            elif isinstance(ctx.value(), GraphQLParser.BooleanValueContext):
                return self.visitBooleanValue(ctx.value())
            else:
                raise NotImplementedError("Not implemented value type")
        elif ctx.variable():
            return self.visitVariable(ctx.variable())

    def visitStringValue(self, ctx: GraphQLParser.StringValueContext):
        return str(ctx.STRING())

    def visitNumberValue(self, ctx: GraphQLParser.NumberValueContext):
        return str(ctx.NUMBER())

    def visitBooleanValue(self, ctx: GraphQLParser.BooleanValueContext):
        return str(ctx.BOOLEAN()) != 'false'

    def visitFieldName(self, ctx: GraphQLParser.FieldNameContext):
        """

        :param ctx:
        :return: fieldname, alias
        """
        if ctx.alias():
            return self.visitAlias(ctx.alias())

        return ctx.NAME().getText(), ctx.NAME().getText()

    def visitAlias(self, ctx: GraphQLParser.AliasContext):
        """

        :param ctx:
        :return: fieldname, alias
        """
        return ctx.NAME(1).getText(), ctx.NAME(0).getText()


class SqlGenerator:
    """SqlGenerator generates SQL from the the GraphQLVisitor output.

    """
    CURSOR_ID = "cursor"

    # Attributes to ignore in the query on attributes.
    srcvalues_attributes = [FIELD.SOURCE_VALUE, FIELD.SOURCE_INFO]

    def __init__(self, visitor: GraphQLVisitor):
        """

        :param visitor:
        """
        self.visitor = visitor
        self.selects = visitor.selects
        self.relation_parents = visitor.relationParents
        self.relation_aliases = visitor.relationAliases
        self.relation_info = {}
        self.model = GOBModel()

    def _get_arguments_with_defaults(self, arguments: dict) -> dict:
        args = {
            'active': True,
        }
        args.update(arguments)
        return args

    def _get_filter_arguments(self, arguments: dict) -> dict:
        """Returns filter arguments from arguments dict

        Changes GraphQL strings with double quotes to single quotes for Postgres

        :param arguments:
        :return:
        """
        ignore = ['first', 'last', 'before', 'after', 'sort', 'active']

        def change_quotation(value):
            strval = str(value)
            double_quote = '"'
            if strval[0] == double_quote and strval[-1] == double_quote:
                return "'" + strval[1:-1] + "'"
            return value

        return {to_snake(k): change_quotation(v) for k, v in arguments.items() if
                k not in ignore and
                not k.endswith('_desc') and
                not k.endswith('_asc')
                }

    def _reset(self):
        self.select_expressions = []
        self.joins = []
        self.relation_info = {}
        self.where_filter = []

    def _resolve_schema_collection_name(self, schema_collection_name: str):
        """
        Resolve catalog and collection from schema collection name

        :param schema_collection_name:
        :return:
        """
        names = to_snake(schema_collection_name).split('_')
        for n in range(1, len(names)):
            catalog_name = '_'.join(names[:-n])
            collection_name = '_'.join(names[-n:])
            catalog = self.model.get_catalog(catalog_name)
            collection = self.model.get_collection(catalog_name, collection_name)
            if catalog and collection:
                return catalog_name, collection_name
        return None, None

    def _collect_relation_info(self, relation_name: str, schema_collection_name: str):
        catalog_name, collection_name = self._resolve_schema_collection_name(schema_collection_name)
        assert catalog_name and collection_name, f"{schema_collection_name} error"

        collection = self.model.get_collection(catalog_name, collection_name)

        abbr = collection['abbreviation'].lower()
        abbr_cnt = len([item for item in self.relation_info.values() if item['abbr'] == abbr])

        self.relation_info[relation_name] = {
            'abbr': abbr,
            'collection_name': collection_name,
            'catalog_name': catalog_name,
            'tablename': self.model.get_table_name(catalog_name, collection_name),
            'alias': f'{abbr}_{abbr_cnt}',
            'has_states': collection.get('has_states', False),
            'collection': collection,
            'attributes': collection['attributes'],
        }

        return self.relation_info[relation_name]

    def _get_relation_info(self, relation_alias: str):
        return self.relation_info[relation_alias]

    def _select_expression(self, relation: dict, field: str):
        if field == self.CURSOR_ID:
            return f"{relation['alias']}.{FIELD.GOBID} AS {self.CURSOR_ID}"

        field_snake = to_snake(field)
        expression = f"{relation['alias']}.{field_snake}"

        # If geometry field, transform to WKT
        if field_snake in relation['attributes'] and is_gob_geo_type(relation['attributes'][field_snake]['type']):
            return f"ST_AsText({expression}) {field_snake}"

        return expression

    def _current_filter_expression(self, table_id: str = None):
        table = f"{table_id}." if table_id else ""

        return f"({table}{FIELD.EXPIRATION_DATE} IS NULL OR {table}{FIELD.EXPIRATION_DATE} > NOW())"

    def _build_from_table(self, arguments: dict, table_name: str, table_alias: str):
        """Builds from table expression for base relation with :table_name: and :arguments:

        :param arguments:
        :param table_name:
        :return:
        """
        conditions = []

        if arguments['active']:
            conditions.append(self._current_filter_expression())

        if 'after' in arguments:
            conditions.append(f"{FIELD.GOBID} > {arguments['after']}")

        # Add non-keyword filter arguments
        filter_args = self._get_filter_arguments(arguments)
        conditions.extend([f"{k} = {v}" for k, v in filter_args.items()])
        conditions.append(f"{FIELD.DATE_DELETED} IS NULL")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        limit = f"LIMIT {arguments['first']}" if 'first' in arguments else ""

        return f"""FROM (
    SELECT *
    FROM {table_name}
    {where}
    ORDER BY {FIELD.GOBID}
    {limit}
) {table_alias}"""

    def sql(self):
        self._reset()

        # Relation without parent is main relation
        base_collection = [k for k, v in self.relation_parents.items() if v is None][0]

        self._collect_relation_info(base_collection, base_collection)
        base_info = self._get_relation_info(base_collection)

        select_fields = [self._select_expression(base_info, field)
                         for field in [FIELD.GOBID] + self.selects[base_collection]['fields']]

        self.select_expressions.extend(select_fields)
        # Add catalog and collection to allow for value resolution
        self.select_expressions.extend([
          f"'{base_info['catalog_name']}' AS {CATALOG_NAME}",
          f"'{base_info['collection_name']}' AS {COLLECTION_NAME}",
        ])

        arguments = self._get_arguments_with_defaults(self.selects[base_collection]['arguments'])

        self.joins.append(self._build_from_table(arguments, base_info['tablename'], base_info['alias']))

        del self.selects[base_collection]

        self._join_relations(self.selects)

        select = ',\n'.join(self.select_expressions)
        table_select = '\n'.join(self.joins)
        order_by = f"ORDER BY {base_info['alias']}.{FIELD.GOBID}"
        where = "\nAND ".join(self.where_filter)

        if where:
            where = "WHERE " + where
        query = f"SELECT\n{select}\n{table_select}\n{where}\n{order_by}"

        return query

    def _get_formatted_filter_arguments(self, arguments: dict, base_alias: str):
        result = []
        filter_args = self._get_filter_arguments(arguments)

        for k, v in filter_args.items():
            result.append(f"{base_alias}.{k} = {v}")
        return result

    def _is_many(self, gobtype: str):
        return gobtype == f"GOB.{gob_types.ManyReference.name}"

    def _join_relations(self, relations: dict):
        self.relcnt = 0
        for relation_alias, select in relations.items():
            arguments = self._get_arguments_with_defaults(select['arguments'])
            select_fields = [FIELD.GOBID] + select['fields']

            if relation_alias.startswith('inv'):
                self._join_inverse_relation(relation_alias, select_fields, arguments)
            else:
                self._join_relation(relation_alias, select_fields, arguments)
            self.relcnt += 1

    def _add_srcvalue_selection(self, src_relation: dict, src_attr_name: str, is_many: bool):
        """Add _src_* selection to query. Returns the field containing the bronwaarde for this row as string so that
        the remainder of the query can match on this bronwaarde.

        :param src_relation:
        :param src_attr_name:
        :param is_many:
        :return:
        """
        src_alias = f"_src_{src_attr_name}"
        jsonb_alias = f"rel_bw_{self.relcnt}"

        if is_many:
            src_values_join = f"LEFT JOIN jsonb_array_elements({src_relation['alias']}.{src_attr_name}) " \
                f"{jsonb_alias}(item) ON {jsonb_alias}.item->>'{FIELD.SOURCE_VALUE}' IS NOT NULL"

            match_src_value = f"{jsonb_alias}.item->>'{FIELD.SOURCE_VALUE}'"

            self.joins.append(src_values_join)
            self.select_expressions.append(f"{jsonb_alias}.item {src_alias}")
        else:
            match_src_value = f"{src_relation['alias']}.{src_attr_name}->>'{FIELD.SOURCE_VALUE}'"
            self.select_expressions.append(f"{src_relation['alias']}.{src_attr_name} {src_alias}")

        return match_src_value

    def _join_relation_table(self, src_relation: dict, relation_name: str, rel_table_alias: str, arguments: dict,
                             src_value_requested: bool, src_attr_name: str, is_many: bool, is_inverse: bool):
        """Generates the SQL for the relation table join, see _add_relation_joins

        :param src_relation:
        :param relation_name:
        :param rel_table_alias:
        :param arguments:
        :param src_value_requested:
        :param src_attr_name:
        :param is_many:
        :param is_inverse:
        :return:
        """
        rel_left = 'src' if not is_inverse else 'dst'
        relation_table = f"mv_{relation_name}"

        def join_filters(table_alias: str):
            filters = [
                f"{table_alias}.{rel_left}_id = {src_relation['alias']}.{FIELD.ID}"
            ]

            if not is_inverse and src_value_requested:
                match_src_value_with = self._add_srcvalue_selection(src_relation, src_attr_name, is_many)
                filters.append(f"{table_alias}.{FIELD.SOURCE_VALUE} = {match_src_value_with}")

            if src_relation['has_states']:
                filters.append(f"{table_alias}.{rel_left}_volgnummer = {src_relation['alias']}.{FIELD.SEQNR}")

            return " AND ".join(filters)

        if arguments.get('first'):
            join_relation_table = f"""
LEFT JOIN {relation_table} {rel_table_alias} ON {rel_table_alias}.{FIELD.GOBID} IN (
    SELECT {FIELD.GOBID}
    FROM {relation_table} rel
    WHERE {join_filters('rel')}
    LIMIT {arguments['first']}
)
"""
        else:
            join_relation_table = f"LEFT JOIN {relation_table} {rel_table_alias} " \
                                  f"ON {join_filters(rel_table_alias)}"

        return join_relation_table

    def _join_dst_table(self, dst_relation: dict, rel_table_alias: str, arguments: dict, is_inverse: bool):
        """Generates the SQL for the destination table join part of a relation:
        A -> B -> C, where A is the src_relation, B the relation_table join and C the dst_relation. See
        _add_relation_joins

        :param dst_relation:
        :param rel_table_alias:
        :param arguments:
        :param is_inverse:
        :return:
        """
        filter_args = self._get_formatted_filter_arguments(arguments, dst_relation['alias'])
        rel_right = 'dst' if not is_inverse else 'src'

        join_dst_table = f"LEFT JOIN {dst_relation['tablename']} {dst_relation['alias']} " \
                         f"ON {rel_table_alias}.{rel_right}_id = {dst_relation['alias']}.{FIELD.ID}"

        if dst_relation['has_states']:
            join_dst_table += f" AND {rel_table_alias}.{rel_right}_volgnummer = {dst_relation['alias']}.{FIELD.SEQNR}"

        if filter_args:
            join_dst_table += f" AND ({') AND ('.join(filter_args)})"

        return join_dst_table

    def _add_relation_joins(self, src_relation: dict, dst_relation: dict, relation_name: str, arguments: dict,
                            src_value_requested: bool=False, src_attr_name: str=None,
                            is_many: bool=False, is_inverse=False):
        """Joins dst_relation to src_relation using relation_table

        Resulting SQL will create a join of the form A -> B -> C, where
        A is the src_relation
        B is the relation_table, and
        C is the dst_relation

        If A contains the attribute pointing to C, A is the owner of the relation, and A will be referred to as 'src'
        in the relation_table B. Otherwise, C is the owner of the relation and C will be referred to as 'src' in the
        relation_table B.

        In case the dst_relation C is the owner of the relation (C has a relation defined to A), this join is said to
        be 'inversed'. Inversed relations never have a requested src_value (bronwaarde), as the src_value is always
        part of the 'owner' of the relation. This is also true for the src_attr_name, as the src_attr_name is always
        defined on the owning side of the relation. That same way, is_many is also only necessary to define for the
        when src_relation A is the owning side of the relation, as is_many is only important when src_value is
        requested (and we will have to unpack the json containing the src_value)

        :param src_relation: The main relation
        :param dst_relation: The relation to join
        :param relation_name:
        :param arguments: A dict with arguments passed in GraphQL to this relation
        :param src_value_requested: boolean. Only applicable if is_inverse == False
        :param src_attr_name: The name of the attribute in the src relation. Only applicable if is_inverse == False
        :param is_many: boolean. Only applicable if is_inverse == False
        :param is_inverse: boolean value indicating if src_relation is the owner of the relation (is_inverse = False),
        or that the relation is owned by dst_relation (is_inverse = True).
        :return:
        """
        rel_table_alias = f"rel_{self.relcnt}"

        join_relation_table = self._join_relation_table(src_relation, relation_name, rel_table_alias, arguments,
                                                        src_value_requested, src_attr_name, is_many, is_inverse)
        join_dst_table = self._join_dst_table(dst_relation, rel_table_alias, arguments, is_inverse)

        if arguments['active']:
            self.where_filter.append(self._current_filter_expression(dst_relation['alias']))

        self.where_filter.append(f"{dst_relation['alias']}.{FIELD.DATE_DELETED} IS NULL")

        self.joins.append(join_relation_table)
        self.joins.append(join_dst_table)

    def _json_build_attrs(self, attributes: list, relation_name: str):
        """Create the list of attributes to be used in json_build_object( ) for attributes in relation_name

        :param attributes:
        :param relation_name:
        :return:
        """
        return ",".join([f"'{to_snake(attr)}', {relation_name}.{to_snake(attr)}" for attr in attributes
                         if attr not in self.srcvalues_attributes])

    def _is_srcvalue_requested(self, attributes: list):
        return any([attr in self.srcvalues_attributes for attr in attributes])

    def _join_relation(self, relation_name: str, attributes: list, arguments: dict):
        parent = self.relation_parents[relation_name]
        parent_info = self._get_relation_info(parent)
        relation_attr_name = to_snake(self.relation_aliases[relation_name])

        dst_catalog_name, dst_collection_name = self.model.get_catalog_collection_names_from_ref(
            parent_info['collection']['attributes'][relation_attr_name]['ref']
        )

        dst_info = self._collect_relation_info(relation_name, f'{dst_catalog_name}_{dst_collection_name}')

        alias = f"_{to_snake(relation_name)}"
        json_attrs = self._json_build_attrs(attributes, dst_info['alias'])
        json_attrs = f"{json_attrs}, '_catalog', '{dst_catalog_name}', '_collection', '{dst_collection_name}'"

        relation_name = get_relation_name(
            self.model,
            parent_info['catalog_name'],
            parent_info['collection_name'],
            relation_attr_name
        )

        self._add_relation_joins(parent_info, dst_info, relation_name, arguments,
                                 self._is_srcvalue_requested(attributes), relation_attr_name,
                                 self._is_many(parent_info['collection']['attributes'][relation_attr_name]['type']))
        self.select_expressions.append(f"json_build_object({json_attrs}) {alias}")

    def _join_inverse_relation(self, relation_name: str, attributes: list, arguments: dict):
        parent = self.relation_parents[relation_name]
        parent_info = self._get_relation_info(parent)

        relation_name_snake = to_snake(relation_name).split('_')

        assert relation_name_snake[0] == 'inv'

        relation_attr_name = '_'.join(relation_name_snake[1:-2])
        dst_catalog_name = relation_name_snake[-2]
        dst_collection_name = relation_name_snake[-1]
        dst_model_name = self.model.get_table_name(dst_catalog_name, dst_collection_name)
        dst_info = self._collect_relation_info(relation_name, f'{dst_model_name}')

        json_attrs = self._json_build_attrs(attributes, dst_info['alias'])
        json_attrs = f"{json_attrs}, '_catalog', '{dst_catalog_name}', '_collection', '{dst_collection_name}'"
        alias = f"_inv_{relation_attr_name}_{dst_info['catalog_name']}_{dst_info['collection_name']}"

        relation_name = get_relation_name(
            self.model,
            dst_info['catalog_name'],
            dst_info['collection_name'],
            relation_attr_name
        )

        self._add_relation_joins(parent_info, dst_info, relation_name, arguments, is_inverse=True)
        self.select_expressions.append(f"json_build_object({json_attrs}) {alias}")


class GraphQL2SQL:
    """GraphQL2SQL class. Parses the input graphql_query and outputs an SQL-equivalent for Postgres.

    Current implementation does not implement the full GraphQL grammar and a this implementation is very specific to
    the GOB use and data model.
    """

    def __init__(self, graphql_query: str):
        self.query = graphql_query
        self.relations_hierarchy = None
        self.selections = None

    def sql(self):
        """Returns a tuple (sql, relation_parents), where sql is the generated sql and relation_parents is a dict
        containing the hierarchy of the relations in this query, so that the result set can be reconstructed as one
        object with nested relations.

        :param graphql_query:
        :return:
        """
        input_stream = InputStream(self.query)
        lexer = GraphQLLexer(input_stream)
        stream = CommonTokenStream(lexer)
        parser = GraphQLParser(stream)
        tree = parser.document()
        visitor = GraphQLVisitor()
        visitor.visit(tree)

        generator = SqlGenerator(visitor)
        self.relations_hierarchy = visitor.relationParents
        self.selections = visitor.selects

        return generator.sql()
