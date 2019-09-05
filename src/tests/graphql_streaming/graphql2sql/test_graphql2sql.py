import re

from unittest import TestCase
from unittest.mock import MagicMock, patch, call

from gobapi.graphql_streaming.graphql2sql.graphql2sql import GraphQL2SQL, GraphQLVisitor, GraphQLParser


class MockModel:
    model = {
        'catalog': {
            'collectiona': {
                'abbreviation': 'cola',
                'has_states': False,
                'attributes': {
                    'identificatie': {
                        'type': 'GOB.String',
                    },
                    'some_nested_relation': {
                        'type': 'GOB.Reference',
                        'ref': 'catalog:collectionb',
                    },
                    'some_nested_many_relation': {
                        'type': 'GOB.ManyReference',
                        'ref': 'catalog:collectionb',
                    }
                }
            },
            'collectionb': {
                'abbreviation': 'colb',
                'has_states': True,
                'attributes': {
                    'identificatie': {
                        'type': 'GOB.String',
                    }
                }
            },
            'collectionwithgeometry': {
                'abbreviation': 'geocoll',
                'has_states': False,
                'attributes': {
                    'geofield': {
                        'type': 'GOB.Geo.Polygon'
                    }
                }
            }
        }
    }

    def get_collection(self, catalog_name, collection_name):
        collections = self.model[catalog_name]
        return collections[collection_name] if collections and collection_name in collections else None

    def get_table_name(self, catalog_name: str, collection_name: str):
        return f"{catalog_name}_{collection_name}"

    def get_catalog_collection_names_from_ref(self, ref: str):
        split = ref.split(':')
        return split[0], split[1]


@patch("gobapi.graphql_streaming.graphql2sql.graphql2sql.GOBModel")
class TestGraphQL2SQL(TestCase):
    """Tests the GraphQL2SQL functionality as a whole. Includes large parts of GraphQLVisitor, SqlGenerator
    and GraphQL2SQL.

    Validates input GraphQL query with expected output SQL
    """


    test_cases = [
        (
            '''
{
  catalogCollectiona {
    edges {
      node {
        identificatie
      }
    }
  }
}
''', '''
SELECT cola_0._gobid, cola_0.identificatie
FROM catalog_collectiona cola_0
WHERE (cola_0._expiration_date IS NULL OR cola_0._expiration_date > NOW())
ORDER BY cola_0._gobid
'''
        ),
        (
            '''
{
  catalogCollectionwithgeometry {
    edges {
      node {
        identificatie
        geofield
      }
    }
  }
}
''', '''
SELECT geocoll_0._gobid, geocoll_0.identificatie, ST_AsText(geocoll_0.geofield) geofield
FROM catalog_collectionwithgeometry geocoll_0
WHERE (geocoll_0._expiration_date IS NULL OR geocoll_0._expiration_date > NOW())
ORDER BY geocoll_0._gobid
'''
        ),
        (
            '''
{
  catalogCollectiona(filterarg: 3, filterarg2: "strval") {
    edges {
      node {
        identificatie
      }
    }
  }
}
''', '''
SELECT cola_0._gobid, cola_0.identificatie
FROM catalog_collectiona cola_0
WHERE (cola_0._expiration_date IS NULL OR cola_0._expiration_date > NOW())
AND (cola_0.filterarg = 3) AND (cola_0.filterarg2 = 'strval')
ORDER BY cola_0._gobid
'''
        ),
        (
            '''
{
  catalogCollectiona(first: 20) {
    edges {
      node {
        identificatie
      }
    }
  }
}
''', '''
SELECT cola_0._gobid, cola_0.identificatie
FROM catalog_collectiona cola_0
WHERE (cola_0._expiration_date IS NULL OR cola_0._expiration_date > NOW())
ORDER BY cola_0._gobid
LIMIT 20
'''
        ),
        (
            '''
{
  catalogCollectiona(active: false) {
    edges {
      node {
        identificatie
      }
    }
  }
}
''', '''
SELECT cola_0._gobid, cola_0.identificatie
FROM catalog_collectiona cola_0
ORDER BY cola_0._gobid
'''),
        (

            '''
{
  catalogCollectiona(active: false) {
    edges {
      node {
        identificatie

        someNestedRelation(someProperty: "someval") {
            edges {
                node {
                    nestedIdentificatie
                    bronwaarde
                    broninfo
                }
            }
        }
      }
    }
  }
}''',
        # bronwaarde and broninfo are added as special case, they change the query by adding the _src selection
            '''
        SELECT
            cola_0._gobid,
            cola_0.identificatie,
            rels._src_some_nested_relation,
            rels._some_nested_relation
        FROM catalog_collectiona cola_0
        LEFT JOIN (
            SELECT
                cola_0._id cola_0_id,
                cola_0.some_nested_relation _src_some_nested_relation,
                json_build_object (
                    '_gobid', colb_0._gobid,
                    'nested_identificatie', colb_0.nested_identificatie ) _some_nested_relation
            FROM catalog_collectiona cola_0
            LEFT JOIN catalog_collectionb colb_0
            ON cola_0.some_nested_relation->>'id' IS NOT NULL
            AND cola_0.some_nested_relation->>'id' = colb_0._id
            AND cola_0.some_nested_relation->>'volgnummer' IS NOT NULL
            AND cola_0.some_nested_relation->>'volgnummer' = colb_0.volgnummer
            AND (colb_0._expiration_date IS NULL OR colb_0._expiration_date > NOW())
            WHERE (colb_0.some_property = 'someval')
        ) rels
        ON rels.cola_0_id = cola_0._id
        ORDER BY cola_0._gobid
         '''

        ),
        (
            '''
{
  catalogCollectiona(active: false) {
    edges {
      node {
        identificatie

        someNestedManyRelation {
            edges {
                node {
                    nestedIdentificatie
                    bronwaarde
                    broninfo
                }
            }
        }
      }
    }
  }
}''',
            # bronwaarde and broninfo are added as special case, they change the query by adding the _src selection
            '''
        SELECT
            cola_0._gobid,
            cola_0.identificatie,
            rels._src_some_nested_many_relation,
            rels._some_nested_many_relation
        FROM catalog_collectiona cola_0
        LEFT JOIN (
            SELECT 
                cola_0._id cola_0_id,
                rel_some_nested_many_relation.item _src_some_nested_many_relation,
                json_build_object (
                    '_gobid', colb_0._gobid,
                    'nested_identificatie', colb_0.nested_identificatie ) _some_nested_many_relation
            FROM catalog_collectiona cola_0
            LEFT JOIN jsonb_array_elements(cola_0.some_nested_many_relation) rel_some_nested_many_relation(item) ON TRUE
            LEFT JOIN catalog_collectionb colb_0 ON rel_some_nested_many_relation.item->>'id' IS NOT NULL 
            AND colb_0._id = rel_some_nested_many_relation.item->>'id'
            AND rel_some_nested_many_relation.item->>'volgnummer' IS NOT NULL
            AND colb_0.volgnummer = rel_some_nested_many_relation.item->>'volgnummer'
            AND (colb_0._expiration_date IS NULL OR colb_0._expiration_date > NOW())
        ) rels
        ON rels.cola_0_id = cola_0._id
        ORDER BY cola_0._gobid
         '''
        ),
        (
            '''
{
  catalogCollectionb {
    edges {
      node {
        identificatie

        invSomeNestedManyRelationCatalogCollectiona {
            edges {
                node {
                   identificatie
                }
            }
        }
      }
    }
  }
}''',
            '''
        SELECT
            colb_0._gobid,
            colb_0.identificatie,
            invrel_0._inv_some_nested_many_relation_catalog_collectiona
        FROM catalog_collectionb colb_0
        LEFT JOIN (
            SELECT
                colb_0._id colb_0_id,
                colb_0.volgnummer colb_0_volgnummer,
                json_build_object (
                    '_gobid', cola_0._gobid,
                    'identificatie', cola_0.identificatie ) _inv_some_nested_many_relation_catalog_collectiona
            FROM catalog_collectiona cola_0
            LEFT JOIN jsonb_array_elements(cola_0.some_nested_many_relation) rel_some_nested_many_relation(item)
            ON rel_some_nested_many_relation.item->>'id' IS NOT NULL
            LEFT JOIN catalog_collectionb colb_0 ON colb_0._id = rel_some_nested_many_relation.item->>'id'
        ) invrel_0
        ON invrel_0.colb_0_id = colb_0._id AND invrel_0.colb_0_volgnummer = colb_0.volgnummer
        WHERE ( colb_0._expiration_date IS NULL OR colb_0._expiration_date > NOW ( ) )
        ORDER BY colb_0._gobid
         '''
        ),
        (
            '''
{
  catalogCollectionb {
    edges {
      node {
        identificatie

        invSomeNestedRelationCatalogCollectiona {
            edges {
                node {
                   identificatie
                }
            }
        }
      }
    }
  }
}''',
            '''
SELECT
    colb_0._gobid,
	colb_0.identificatie,
	rels._inv_some_nested_relation_catalog_collectiona
FROM catalog_collectionb colb_0
LEFT JOIN (
	SELECT
		colb_0._id colb_0_id,
		colb_0.volgnummer colb_0_volgnummer,
		json_build_object (
		    '_gobid', cola_0._gobid,
		    'identificatie' , cola_0.identificatie) _inv_some_nested_relation_catalog_collectiona
		FROM catalog_collectionb colb_0
		LEFT JOIN catalog_collectiona cola_0
		ON cola_0.some_nested_relation->>'id' IS NOT NULL
		AND cola_0.some_nested_relation->>'id' = colb_0._id
		AND cola_0.some_nested_relation->>'volgnummer' IS NOT NULL
		AND cola_0.some_nested_relation->>'volgnummer' = colb_0.volgnummer
		AND ( cola_0._expiration_date IS NULL OR cola_0._expiration_date > NOW ())
	) rels
ON rels.colb_0_id = colb_0._id
AND rels.colb_0_volgnummer = colb_0.volgnummer
WHERE ( colb_0._expiration_date IS NULL OR colb_0._expiration_date > NOW ())
ORDER BY colb_0._gobid
         '''
        ),
    ]

    def normalise_whitespace(self, string: str):
        whitespacechars = re.sub(r'([,(,)])', ' \g<1> ', string)
        return re.sub(r'\s+', ' ', whitespacechars).strip()

    def assertResult(self, expected_result, result):
        self.assertEqual(
            self.normalise_whitespace(expected_result),
            self.normalise_whitespace(result)
        )

    def test_graphql2sql(self, mock_model):
        mock_model.return_value = MockModel()
        self.maxDiff = None

        for inp, outp in self.test_cases:
            graphql2sql = GraphQL2SQL(inp)
            self.assertResult(outp, graphql2sql.sql())


class TestGraphQLVisitor(TestCase):

    def setUp(self) -> None:
        self.visitor = GraphQLVisitor()

    def test_visitAlias(self):
        arg = MagicMock()
        self.visitor.visitAlias(arg)
        arg.NAME.assert_has_calls([call(1), call().getText(), call(0), call().getText()])

    def test_visitFieldName(self):
        self.visitor.visitAlias = MagicMock()
        arg = MagicMock()
        arg.alias.return_value = MagicMock()

        self.visitor.visitFieldName(arg)
        self.visitor.visitAlias.assert_called_with(arg.alias.return_value)

    def test_visitBooleanValue(self):
        arg = MagicMock()
        arg.BOOLEAN.return_value = 'false'

        self.assertFalse(self.visitor.visitBooleanValue(arg))

        arg.BOOLEAN.return_value = 'true'
        self.assertTrue(self.visitor.visitBooleanValue(arg))

    def test_visitNumberValue(self):
        arg = MagicMock()
        arg.NUMBER.return_value = 248042
        self.assertEqual(str(arg.NUMBER.return_value), self.visitor.visitNumberValue(arg))

    def test_visitStringValue(self):
        arg = MagicMock()
        arg.STRING.return_value = 'somestringvalue'
        self.assertEqual(arg.STRING.return_value, self.visitor.visitStringValue(arg))

    def test_visitField_arguments_not_implemented(self):
        arg = MagicMock()
        arg.directives.return_value = True

        with self.assertRaises(NotImplementedError):
            self.visitor.visitField(arg)

    def test_visitField_ignore_page_info(self):
        arg = MagicMock()
        arg.selectionSet.return_value = True
        arg.directives.return_value = False
        self.visitor.visitSelectionSet = MagicMock()
        self.visitor.visitFieldName = MagicMock(return_value=('pageInfo', 'pageInfo'))
        self.visitor.visitField(arg)
        self.visitor.visitSelectionSet.assert_not_called()

    def test_visitValueOrVariable(self):
        cases = [
            (GraphQLParser.StringValueContext, 'visitStringValue'),
            (GraphQLParser.NumberValueContext, 'visitNumberValue'),
            (GraphQLParser.BooleanValueContext, 'visitBooleanValue'),
        ]

        for context, callmethod in cases:
            arg = MagicMock()
            arg.value.return_value = context(MagicMock(), MagicMock())
            mock_visit_value = MagicMock()
            self.visitor.__setattr__(callmethod, mock_visit_value)

            res = self.visitor.visitValueOrVariable(arg)
            mock_visit_value.assert_called_with(arg.value.return_value)
            self.assertEqual(mock_visit_value.return_value, res)

        # Test not implemented value type
        arg = MagicMock()
        arg.value.return_value = 'some other value'

        with self.assertRaises(NotImplementedError):
            self.visitor.visitValueOrVariable(arg)

        # Test no value
        arg = MagicMock()
        arg.value.return_value = False
        self.visitor.visitVariable = MagicMock()
        self.visitor.visitValueOrVariable(arg)
        self.visitor.visitVariable.assert_called_with(arg.variable.return_value)
