import re
from antlr4 import InputStream, CommonTokenStream
from gobapi.graphql_streaming.graphql2sql.grammar.GraphQLLexer import GraphQLLexer
from gobapi.graphql_streaming.graphql2sql.grammar.GraphQLParser import GraphQLParser
from gobapi.graphql_streaming.graphql2sql.grammar.GraphQLVisitor import GraphQLVisitor as BaseVisitor

from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
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

    def to_snake(self, camel: str):
        return re.sub('([A-Z])', r'_\1', camel).lower()

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

        return {self.to_snake(k): change_quotation(v) for k, v in arguments.items() if
                k not in ignore and
                not k.endswith('_desc') and
                not k.endswith('_asc')
                }

    def _reset(self):
        self.select_expressions = []
        self.joins = []
        self.filter_conditions = []
        self.relation_info = {}

    def _get_active(self, tablename: str):
        return f"({tablename}.{FIELD.EXPIRATION_DATE} IS NULL OR {tablename}.{FIELD.EXPIRATION_DATE} > NOW())" \
               f"AND {tablename}.{FIELD.DATE_DELETED} IS NULL"

    def _collect_relation_info(self, relation_name: str, schema_collection_name: str):
        catalog_name, collection_name = self.to_snake(schema_collection_name).split('_')

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
        field_snake = self.to_snake(field)
        expression = f"{relation['alias']}.{field_snake}"

        # If geometry field, transform to WKT
        if field_snake in relation['attributes'] and is_gob_geo_type(relation['attributes'][field_snake]['type']):
            return f"ST_AsText({expression}) {field_snake}"

        return expression

    def _join_filter_arguments(self, filter_arguments: list):
        return f"({') AND ('.join(filter_arguments)})"

    def _where_clause(self, filter_conditions: list):
        return f"WHERE {self._join_filter_arguments(filter_conditions)}" if len(filter_conditions) else ""

    def sql(self):
        self._reset()

        # Relation without parent is main relation
        base_collection = [k for k, v in self.relation_parents.items() if v is None][0]

        self._collect_relation_info(base_collection, base_collection)
        base_info = self._get_relation_info(base_collection)

        select_fields = [self._select_expression(base_info, field)
                         for field in [FIELD.GOBID] + self.selects[base_collection]['fields']]

        self.select_expressions.extend(select_fields)

        arguments = self._get_arguments_with_defaults(self.selects[base_collection]['arguments'])

        self.joins.append(f"FROM {base_info['tablename']} {base_info['alias']}")

        if arguments['active']:
            self.filter_conditions.append(self._get_active(base_info['alias']))

        filter_args = self._get_formatted_filter_arguments(arguments, base_info['alias'])
        self.filter_conditions.extend(filter_args)

        del self.selects[base_collection]

        self._join_relations(self.selects)

        select = ',\n'.join(self.select_expressions)
        table_select = '\n'.join(self.joins)
        where = self._where_clause(self.filter_conditions)
        limit = self._get_limit_expression(arguments)
        order_by = f"ORDER BY {base_info['alias']}.{FIELD.GOBID}"
        query = f"SELECT\n{select}\n{table_select}\n{where}\n{order_by}\n{limit}"

        return query

    def _get_formatted_filter_arguments(self, arguments: dict, base_alias: str):
        result = []
        filter_args = self._get_filter_arguments(arguments)

        for k, v in filter_args.items():
            result.append(f"{base_alias}.{k} = {v}")
        return result

    def _get_limit_expression(self, arguments: dict):
        if 'first' in arguments:
            return f"LIMIT {arguments['first']}"
        return ""

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

    def _join_relation_many(self, src_relation_name: str, src_attr_name: str, dst_relation: dict, filter_active: bool,
                            src_value_requested: bool):
        """Joins relation src_relation with

        :param src_relation_name:
        :param src_attr_name:
        :param dst_relation: Expected keys: tablename, alias, has_states
        :param filter_active:

        :return:
        """
        jsonb_alias = f"rel_{src_attr_name}{self.relcnt}"
        jsonb_join = f"LEFT JOIN jsonb_array_elements({src_relation_name}.{src_attr_name}) " \
            f"{jsonb_alias}(item) ON TRUE"

        left_join = f"LEFT JOIN {dst_relation['tablename']} {dst_relation['alias']} " \
            f"ON {jsonb_alias}.item->>'{FIELD.REFERENCE_ID}' IS NOT NULL " \
            f"AND {dst_relation['alias']}.{FIELD.ID} = {jsonb_alias}.item->>'{FIELD.REFERENCE_ID}'"

        if dst_relation['has_states']:
            left_join += f" AND {jsonb_alias}.item->>'{FIELD.SEQNR}' IS NOT NULL " \
                f"AND {dst_relation['alias']}.{FIELD.SEQNR} = {jsonb_alias}.item->>'{FIELD.SEQNR}'"

        if filter_active:
            left_join += f" AND ({self._get_active(dst_relation['alias'])})"

        if src_value_requested:
            src_alias = f"_src_{src_attr_name}"
            self.select_expressions.append(f"{jsonb_alias}.item {src_alias}")

        self.joins.append(jsonb_join)
        self.joins.append(left_join)

    def _join_relation_single(self, src_relation_name: str, src_attr_name: str, dst_relation: dict,
                              filter_active: bool, src_value_requested: bool):
        join = f"LEFT JOIN {dst_relation['tablename']} {dst_relation['alias']} " \
            f"ON {src_relation_name}.{src_attr_name}->>'{FIELD.REFERENCE_ID}' IS NOT NULL " \
            f"AND {src_relation_name}.{src_attr_name}->>'{FIELD.REFERENCE_ID}' = {dst_relation['alias']}.{FIELD.ID}"

        if dst_relation['has_states']:
            join += f" AND {src_relation_name}.{src_attr_name}->>'{FIELD.SEQNR}' IS NOT NULL " \
                f"AND {src_relation_name}.{src_attr_name}->>'{FIELD.SEQNR}' " \
                f"= {dst_relation['alias']}.{FIELD.SEQNR}"

        if filter_active:
            join += f" AND ({self._get_active(dst_relation['alias'])})"

        if src_value_requested:
            src_alias = f"_src_{src_attr_name}"
            self.select_expressions.append(f"{src_relation_name}.{src_attr_name} {src_alias}")

        self.joins.append(join)

    def _json_build_attrs(self, attributes: list, relation_name: str):
        """Create the list of attributes to be used in json_build_object( ) for attributes in relation_name

        :param attributes:
        :param relation_name:
        :return:
        """
        return ",".join([f"'{self.to_snake(attr)}', {relation_name}.{self.to_snake(attr)}" for attr in attributes
                         if attr not in self.srcvalues_attributes])

    def _is_srcvalue_requested(self, attributes: list):
        return any([attr in self.srcvalues_attributes for attr in attributes])

    def _join_relation(self, relation_name: str, attributes: list, arguments: dict):
        parent = self.relation_parents[relation_name]
        parent_info = self._get_relation_info(parent)
        relation_attr_name = self.to_snake(self.relation_aliases[relation_name])

        dst_catalog_name, dst_collection_name = self.model.get_catalog_collection_names_from_ref(
            parent_info['collection']['attributes'][relation_attr_name]['ref']
        )

        dst_info = self._collect_relation_info(relation_name, f'{dst_catalog_name}_{dst_collection_name}')

        alias = f"_{self.to_snake(relation_name)}"
        json_attrs = self._json_build_attrs(attributes, dst_info['alias'])

        is_many = self._is_many(parent_info['collection']['attributes'][relation_attr_name]['type'])

        if is_many:
            self._join_relation_many(parent_info['alias'], relation_attr_name, dst_info, arguments['active'],
                                     self._is_srcvalue_requested(attributes))
        else:
            self._join_relation_single(parent_info['alias'], relation_attr_name, dst_info, arguments['active'],
                                       self._is_srcvalue_requested(attributes))

        self.filter_conditions.extend(self._get_formatted_filter_arguments(arguments, dst_info['alias']))

        self.select_expressions.append(f"json_build_object({json_attrs}) {alias}")

    def _join_inverse_relation_many(self, src_relation: dict, dst_relation: dict, dst_attr_name: str, attrs: str,
                                    alias: str, arguments: dict):
        """Creates an inverse join for dst_relation. Dst_relation has an attr dst_attr_name that has a many relation
        to src_relation.

        :param src_relation:
        :param dst_relation:
        :param dst_attr_name:
        :param attrs:
        :param alias:
        :return:
        """
        joinalias = f"invrel_{self.relcnt}"
        selects = [f"{src_relation['alias']}.{FIELD.ID} {src_relation['alias']}{FIELD.ID}"]

        if src_relation['has_states']:
            selects.append(f"\t{src_relation['alias']}.{FIELD.SEQNR} {src_relation['alias']}_volgnummer")

        selects.append(f"json_build_object({attrs}) {alias}")

        s = ",".join(selects)

        on_clause = f"{joinalias}.{src_relation['alias']}{FIELD.ID} = {src_relation['alias']}.{FIELD.ID}"

        if src_relation['has_states']:
            on_clause += f" AND {joinalias}.{src_relation['alias']}_volgnummer = " \
                f"{src_relation['alias']}.{FIELD.SEQNR}"

        filter_arguments = self._get_formatted_filter_arguments(arguments, dst_relation['alias'])
        where_clause = self._where_clause(filter_arguments)

        self.joins.append(f'''
LEFT JOIN (
    SELECT
        {s}
    FROM {dst_relation['tablename']} {dst_relation['alias']}
    LEFT JOIN jsonb_array_elements({dst_relation['alias']}.{dst_attr_name}) rel_{dst_attr_name}{self.relcnt}(item)
    ON rel_{dst_attr_name}{self.relcnt}.item->>'id' IS NOT NULL
    LEFT JOIN {src_relation['tablename']} {src_relation['alias']}
    ON {src_relation['alias']}.{FIELD.ID} = rel_{dst_attr_name}{self.relcnt}.item->>'id'
    {where_clause}
) {joinalias}
ON {on_clause}''')
        self.select_expressions.append(f"{joinalias}.{alias}")

    def _join_inverse_relation_single(self, src_relation: dict, dst_relation: dict, dst_attr_name: str,
                                      filter_active: bool, arguments: dict):
        join = f"LEFT JOIN {dst_relation['tablename']} {dst_relation['alias']} " \
            f"ON {dst_relation['alias']}.{dst_attr_name}->>'{FIELD.REFERENCE_ID}' IS NOT NULL " \
            f"AND {dst_relation['alias']}.{dst_attr_name}->>'{FIELD.REFERENCE_ID}' = " \
            f"{src_relation['alias']}.{FIELD.ID}"

        if src_relation['has_states']:
            join += f" AND {dst_relation['alias']}.{dst_attr_name}->>'{FIELD.SEQNR}' IS NOT NULL " \
                f"AND {dst_relation['alias']}.{dst_attr_name}->>'{FIELD.SEQNR}' " \
                f"= {src_relation['alias']}.{FIELD.SEQNR}"

        if filter_active:
            join += f" AND ({self._get_active(dst_relation['alias'])})"

        filter_arguments = self._get_formatted_filter_arguments(arguments, dst_relation['alias'])
        join += f" AND {self._join_filter_arguments(filter_arguments)}" if len(filter_arguments) else ""

        self.joins.append(join)

    def _join_inverse_relation(self, relation_name: str, attributes: list, arguments: dict):
        parent = self.relation_parents[relation_name]
        parent_info = self._get_relation_info(parent)

        relation_name_snake = self.to_snake(relation_name).split('_')

        assert relation_name_snake[0] == 'inv'

        relation_attr_name = '_'.join(relation_name_snake[1:-2])
        dst_catalog_name = relation_name_snake[-2]
        dst_collection_name = relation_name_snake[-1]
        dst_model_name = self.model.get_table_name(dst_catalog_name, dst_collection_name)
        dst_info = self._collect_relation_info(relation_name, f'{dst_model_name}')

        json_attrs = self._json_build_attrs(attributes, dst_info['alias'])
        alias = f"_inv_{relation_attr_name}_{dst_info['catalog_name']}_{dst_info['collection_name']}"

        is_many = self._is_many(dst_info['collection']['attributes'][relation_attr_name]['type'])

        if is_many:
            self._join_inverse_relation_many(parent_info, dst_info, relation_attr_name, json_attrs, alias, arguments)
        else:
            self._join_inverse_relation_single(parent_info, dst_info, relation_attr_name, arguments['active'],
                                               arguments)
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
