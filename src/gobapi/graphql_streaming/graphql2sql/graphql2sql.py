import re
from antlr4 import InputStream, CommonTokenStream
from gobapi.graphql_streaming.graphql2sql.grammar.GraphQLLexer import GraphQLLexer
from gobapi.graphql_streaming.graphql2sql.grammar.GraphQLParser import GraphQLParser
from gobapi.graphql_streaming.graphql2sql.grammar.GraphQLVisitor import GraphQLVisitor as BaseVisitor

from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.typesystem import gob_types


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
        self.arguments = {}

    def pushRelationStack(self, relation: str):
        """Pushes relation to stack.

        :param relation:
        :return:
        """
        self.relationParents[relation] = self.relationStack[-1] if len(self.relationStack) else None
        self.relationStack.append(relation)

        if relation not in self.selects:
            self.selects[relation] = {
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
                self.pushRelationStack(field_name)
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
        else:
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
    JSON_SEQ_NR = 'volgnummer'
    JSON_ID = 'id'

    def __init__(self, visitor: GraphQLVisitor, unfold: bool):
        self.visitor = visitor
        self.selects = visitor.selects
        self.relation_parents = visitor.relationParents
        self.relation_info = {}
        self.model = GOBModel()
        self.unfold = unfold

    def to_snake(self, camel: str):
        return re.sub('([A-Z])', r'_\1', camel).lower()

    def _format_select_exprs(self, fields: list, prefix: str):
        return [f'{prefix}.{self.to_snake(field)}' for field in fields]

    def _get_arguments_with_defaults(self, arguments: dict) -> dict:
        args = {
            'active': True,
        }
        args.update(arguments)
        return args

    def _reset(self):
        self.select_expressions = []
        self.joins = []
        self.filter_conditions = []
        self.relation_info = {}
        self.subjoins = []
        self.subjoin_select_list = []

    def _get_active(self, tablename: str):
        return f"{tablename}.{FIELD.EXPIRATION_DATE} IS NULL OR {tablename}.{FIELD.EXPIRATION_DATE} > NOW()"

    def _collect_relation_info(self, relation_name: str, collection_name: str):

        catalog_name, collection = self.model.get_collection_by_name(collection_name)

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
        }

        return self.relation_info[relation_name]

    def _get_relation_info(self, relation_name: str):
        return self.relation_info[relation_name]

    def sql(self):
        self._reset()

        # Relation without parent is main relation
        base_collection = {k: v for k, v in self.relation_parents.items() if v is None}
        base_collection = list(base_collection.keys())[0]

        self._collect_relation_info(base_collection, base_collection)
        base_info = self._get_relation_info(base_collection)

        self.select_expressions.extend(
            self._format_select_exprs(self.selects[base_collection]['fields'], base_info['alias']))

        arguments = self._get_arguments_with_defaults(self.selects[base_collection]['arguments'])

        self.joins.append(f"FROM {base_info['tablename']} {base_info['alias']}")

        if arguments['active']:
            self.filter_conditions.append(self._get_active(base_info['alias']))

        del self.selects[base_collection]

        self._join_relations(self.selects)

        if len(self.subjoins):
            select_general = [f"\t{base_info['alias']}.{FIELD.ID} {base_info['alias']}_id"]

            if base_info['has_states']:
                select_general.append(f"\t{base_info['alias']}.{FIELD.SEQNR} {base_info['alias']}_{FIELD.SEQNR}")

            join_selects_str = ",\n".join(select_general + self.subjoin_select_list)
            joins_str = "\n".join(self.subjoins)

            on_clause = f"rels.{base_info['alias']}_id = {base_info['alias']}.{FIELD.ID}"

            if base_info['has_states']:
                on_clause += f" AND rels.{base_info['alias']}_{FIELD.SEQNR} = {base_info['alias']}.{FIELD.SEQNR}"

            if not self.unfold:
                group_by = self._default_group_by(base_info['alias'], base_info['has_states'])
            else:
                group_by = ""

            self.joins.append(f'''
LEFT JOIN (
    SELECT
{join_selects_str}
    FROM {base_info['tablename']} {base_info['alias']}
{joins_str}
    {group_by}
) rels
ON {on_clause}''')

        select = ',\n'.join(self.select_expressions)
        table_select = ''.join(self.joins)
        where = f"WHERE ({') AND ('.join(self.filter_conditions)})" if len(self.filter_conditions) > 0 else ""
        query = f"SELECT\n{select}\n{table_select}\n{where}"
        return query

    def _default_group_by(self, relation_name: str, has_states: bool):
        """Returns default GROUP BY expression for relation_name, grouping by ID and optionally by SEQNR

        :param relation_name:
        :param has_states:
        :return:
        """
        group_by = f"GROUP BY {relation_name}.{FIELD.ID}"

        if has_states:
            group_by += f", {relation_name}.{FIELD.SEQNR}"

        return group_by

    def _is_many(self, gobtype: str):
        return gobtype == f"GOB.{gob_types.ManyReference.name}"

    def _join_relations(self, relations: dict):
        self.relcnt = 0
        for relation_name, select in relations.items():
            arguments = self._get_arguments_with_defaults(select['arguments'])

            if relation_name.startswith('inv'):
                self._join_inverse_relation(relation_name, select['fields'], arguments)
            else:
                self._join_relation(relation_name, select['fields'], arguments)
            self.relcnt += 1

    def _join_relation_many(self, src_relation_name: str, src_attr_name: str, dst_relation: dict, filter_active: bool):
        """Joins relation src_relation with

        :param src_relation_name:
        :param src_attr_name:
        :param dst_relation: Expected keys: tablename, alias, has_states
        :param filter_active:

        :return:
        """
        jsonb_join = f"LEFT JOIN jsonb_array_elements({src_relation_name}.{src_attr_name}) " \
            f"rel_{src_attr_name}(item)" \
            f" ON rel_{src_attr_name}.item->>'{self.JSON_ID}' IS NOT NULL"

        left_join = f"LEFT JOIN {dst_relation['tablename']} {dst_relation['alias']} " \
            f"ON {dst_relation['alias']}.{FIELD.ID} = rel_{src_attr_name}.item->>'{self.JSON_ID}'"

        if dst_relation['has_states']:
            jsonb_join += f" AND rel_{src_attr_name}.item->>'{self.JSON_SEQ_NR}' IS NOT NULL"
            left_join += f" AND {dst_relation['alias']}.{FIELD.SEQNR} = " \
                f"rel_{src_attr_name}.item->>'{self.JSON_SEQ_NR}'"

        if filter_active:
            left_join += f" AND ({dst_relation['alias']}.{FIELD.EXPIRATION_DATE} IS NULL " \
                f"OR {dst_relation['alias']}.{FIELD.EXPIRATION_DATE} > NOW())"

        self.subjoins.append(jsonb_join)
        self.subjoins.append(left_join)

    def _join_relation_single(self, src_relation_name: str, src_attr_name: str, dst_relation: dict,
                              filter_active: bool):
        join = f"LEFT JOIN {dst_relation['tablename']} {dst_relation['alias']} " \
            f"ON {src_relation_name}.{src_attr_name}->>'{self.JSON_ID}' IS NOT NULL " \
            f"AND {src_relation_name}.{src_attr_name}->>'{self.JSON_ID}' = {dst_relation['alias']}.{FIELD.ID}"

        if dst_relation['has_states']:
            join += f" AND {src_relation_name}.{src_attr_name}->>'{self.JSON_SEQ_NR}' IS NOT NULL " \
                f"AND {src_relation_name}.{src_attr_name}->>'{self.JSON_SEQ_NR}' " \
                f"= {dst_relation['alias']}.{FIELD.SEQNR}"

        if filter_active:
            join += f" AND ({dst_relation['alias']}.{FIELD.EXPIRATION_DATE} IS NULL " \
                f"OR {dst_relation['alias']}.{FIELD.EXPIRATION_DATE} > NOW())"

        self.subjoins.append(join)

    def _join_relation(self, relation_name: str, attributes: list, arguments: dict):
        parent = self.relation_parents[relation_name]
        parent_info = self._get_relation_info(parent)
        relation_attr_name = self.to_snake(relation_name)

        dst_catalog_name, dst_collection_name = self.model.get_catalog_collection_names_from_ref(
            parent_info['collection']['attributes'][relation_attr_name]['ref']
        )

        dst_info = self._collect_relation_info(relation_name, dst_collection_name)

        alias = f"_{relation_attr_name}"
        json_attrs = ",".join(
            [f"'{self.to_snake(attr)}', {dst_info['alias']}.{self.to_snake(attr)}" for attr in attributes])

        is_many = self._is_many(parent_info['collection']['attributes'][relation_attr_name]['type'])

        if is_many:
            self._join_relation_many(parent_info['alias'], relation_attr_name, dst_info, arguments['active'])
        else:
            self._join_relation_single(parent_info['alias'], relation_attr_name, dst_info, arguments['active'])

        subjoin_select = f"json_build_object({json_attrs})"

        if not self.unfold:
            # Aggregate if no unfold
            subjoin_select = f"json_agg( {subjoin_select} )"

        subjoin_select += f" {alias}"

        self.subjoin_select_list.append(subjoin_select)
        self.select_expressions.append(f"rels.{alias}")

    def _join_inverse_relation_many(self, src_relation: dict, dst_relation: dict, dst_attr_name: str, attrs: str,
                                    alias: str):
        """Creates an inverse join for dst_relation. Dst_relation has an attr dst_attr_name that has a many relation
        to src_relation.

        :param src_relation:
        :param src_attr_name:
        :param dst_relation:
        :param attrs:
        :param alias:
        :return:
        """
        joinalias = f"invrel_{self.relcnt}"
        selects = [f"{src_relation['alias']}.{FIELD.ID} {src_relation['alias']}{FIELD.ID}"]

        if src_relation['has_states']:
            selects.append(f"\t{src_relation['alias']}.{FIELD.SEQNR} {src_relation['alias']}_volgnummer")

        select = f"json_build_object({attrs})"

        if not self.unfold:
            select = f"json_agg( {select} )"
        select += f" {alias}"
        selects.append(select)

        s = ",".join(selects)

        on_clause = f"{joinalias}.{src_relation['alias']}{FIELD.ID} = {src_relation['alias']}.{FIELD.ID}"

        if not self.unfold:
            group_by = self._default_group_by(src_relation['alias'], src_relation['has_states'])
        else:
            group_by = ""

        if src_relation['has_states']:
            on_clause += f" AND {joinalias}.{src_relation['alias']}_volgnummer = " \
                f"{src_relation['alias']}.{FIELD.SEQNR}"

        self.joins.append(f'''
LEFT JOIN (
    SELECT
        {s}
    FROM {dst_relation['tablename']} {dst_relation['alias']}
    LEFT JOIN jsonb_array_elements({dst_relation['alias']}.{dst_attr_name}) rel_{dst_attr_name}(item)
    ON rel_{dst_attr_name}.item->>'id' IS NOT NULL
    LEFT JOIN {src_relation['tablename']} {src_relation['alias']}
    ON {src_relation['alias']}.{FIELD.ID} = rel_{dst_attr_name}.item->>'id'
    {group_by}
) {joinalias}
ON {on_clause}''')
        self.select_expressions.append(f"{joinalias}.{alias}")

    def _join_inverse_relation_single(self, src_relation: dict, dst_relation: dict, dst_attr_name: str,
                                      filter_active: bool, alias: str):
        join = f"LEFT JOIN {dst_relation['tablename']} {dst_relation['alias']} " \
            f"ON {dst_relation['alias']}.{dst_attr_name}->>'{self.JSON_ID}' IS NOT NULL " \
            f"AND {dst_relation['alias']}.{dst_attr_name}->>'{self.JSON_ID}' = {src_relation['alias']}.{FIELD.ID}"

        if src_relation['has_states']:
            join += f" AND {dst_relation['alias']}.{dst_attr_name}->>'{self.JSON_SEQ_NR}' IS NOT NULL " \
                f"AND {dst_relation['alias']}.{dst_attr_name}->>'{self.JSON_SEQ_NR}' " \
                f"= {src_relation['alias']}.{FIELD.SEQNR}"

        if filter_active:
            join += f" AND ({dst_relation['alias']}.{FIELD.EXPIRATION_DATE} IS NULL " \
                f"OR {dst_relation['alias']}.{FIELD.EXPIRATION_DATE} > NOW())"

        self.subjoins.append(join)
        self.select_expressions.append(f"rels.{alias}")

    def _join_inverse_relation(self, relation_name: str, attributes: list, arguments: dict):  # noqa: C901
        parent = self.relation_parents[relation_name]
        parent_info = self._get_relation_info(parent)

        relation_name_snake = self.to_snake(relation_name).split('_')

        assert relation_name_snake[0] == 'inv'

        relation_attr_name = '_'.join(relation_name_snake[1:-2])
        dst_collection_name = relation_name_snake[-1]
        dst_info = self._collect_relation_info(relation_name, dst_collection_name)

        json_attrs = ",".join([f"'{self.to_snake(attr)}', {dst_info['alias']}.{self.to_snake(attr)}"
                               for attr in attributes])
        alias = f"_inv_{relation_attr_name}_{dst_info['catalog_name']}_{dst_info['collection_name']}"

        is_many = self._is_many(dst_info['collection']['attributes'][relation_attr_name]['type'])

        if is_many:
            self._join_inverse_relation_many(parent_info, dst_info, relation_attr_name, json_attrs, alias)
        else:
            self._join_inverse_relation_single(parent_info, dst_info, relation_attr_name, arguments['active'], alias)

        subjoin_select = f"json_build_object({json_attrs})"

        if not self.unfold:
            # Aggregate if no unfold
            subjoin_select = f"json_agg( {subjoin_select} )"

        subjoin_select += f" {alias}"

        self.subjoin_select_list.append(subjoin_select)


class GraphQL2SQL:
    """GraphQL2SQL class. Parses the input graphql_query and outputs an SQL-equivalent for Postgres.

    Current implementation does not implement the full GraphQL grammar and a this implementation is very specific to
    the GOB use and data model.
    """

    @staticmethod
    def graphql2sql(graphql_query: str, unfold: bool):
        input_stream = InputStream(graphql_query)
        lexer = GraphQLLexer(input_stream)
        stream = CommonTokenStream(lexer)
        parser = GraphQLParser(stream)
        tree = parser.document()
        visitor = GraphQLVisitor()
        visitor.visit(tree)

        generator = SqlGenerator(visitor, unfold)
        return generator.sql()
