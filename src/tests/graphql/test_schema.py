from gobapi.graphql.schema import _get_sorted_references, get_inverse_references, get_inverse_connection_field, \
    get_inverse_relation_resolvers, get_connection_field

from unittest import TestCase
from unittest.mock import patch, call

from graphene.types.generic import GenericScalar


class TestGraphqlSchema(TestCase):
    def test_sorted_references_empty_model(self):
        class MockModel():
            def get_catalogs(self):
                return {}

        assert(_get_sorted_references(MockModel()) == [])

    def test_sorted_references(self):
        class MockModel():
            def get_catalogs(self):
                return {"catalog": {}}

            def ref_to(self, other):
                return {
                    "type": "GOB.Reference",
                    "ref": other
                }

            def get_collections(self, catalog_name):
                refs_1 = {"attr1" : self.ref_to("catalog:collection3")}
                refs_2 = {"attr1" : self.ref_to("catalog:collection1")}
                refs_3 = {}
                refs_4 = {"attr1" : self.ref_to("catalog:collection2")}
                return {
                    "collection1": {
                        "references": refs_1,
                        "attributes": refs_1
                    },
                    "collection2": {
                        "references": refs_2,
                        "attributes": refs_2
                    },
                    "collection3": {
                        "references": refs_3,
                        "attributes": refs_3
                    },
                    "collection4": {
                        "references": refs_4,
                        "attributes": refs_4
                    },
                }

        sorted_refs = _get_sorted_references(MockModel())
        # 1 => 3 implies 3 before 1
        assert(sorted_refs.index('catalog:collection3') < sorted_refs.index('catalog:collection1'))
        # 2 => 1 implies 1 before 2
        assert(sorted_refs.index('catalog:collection1') < sorted_refs.index('catalog:collection2'))
        # 4 => 2 implies 2 before 4
        assert(sorted_refs.index('catalog:collection2') < sorted_refs.index('catalog:collection4'))

    @patch("gobapi.graphql.schema.inverse_connection_fields", {"fielda": "vala", "fieldb": "valb"})
    def test_get_inverse_connection_field_keyerror(self):
        self.assertEqual("vala", get_inverse_connection_field("fielda")())
        self.assertEqual("valb", get_inverse_connection_field("fieldb")())
        self.assertEqual(GenericScalar, get_inverse_connection_field("fieldc")())

    class MockModels():
        def __getitem__(self, item):
            # Return key
            return item

    @patch("gobapi.graphql.schema.get_resolve_inverse_attribute")
    @patch("gobapi.graphql.schema.model")
    @patch("gobapi.graphql.schema.models", MockModels())
    def test_get_inverse_relation_resolvers(self, mock_model, mock_resolve):
        connections = [
            {'src_catalog': 'cata', 'src_collection': 'cola', 'src_relation_name': 'relation_a', 'field_name': 'field_a'},
            {'src_catalog': 'catb', 'src_collection': 'colb', 'src_relation_name': 'relation_b', 'field_name': 'field_b'},
        ]
        mock_model.get_table_name.side_effect = ['table_name_a', 'table_name_b']
        mock_model._data = {
            'cata': { 'collections': { 'cola': { 'attributes': { 'relation_a': { 'type': 'GOB.ManyReference'}}}}},
            'catb': { 'collections': { 'colb': { 'attributes': { 'relation_b': { 'type': 'GOB.Reference'}}}}},

        }
        mock_resolve.side_effect = ['resolver a', 'resolver b']

        result = get_inverse_relation_resolvers(connections)
        expected_result = {
            'resolve_field_a': 'resolver a',
            'resolve_field_b': 'resolver b',
        }
        self.assertEqual(expected_result, result)

        mock_resolve.assert_has_calls([
            call('table_name_a', 'relation_a'),
            call('table_name_b', 'relation_b'),
        ])

    @patch("gobapi.graphql.schema.model")
    def test_get_inverse_references(self, mock_model):
        mock_model.get_inverse_relations.return_value = {
            "cat1": {
                "col1": "catcol1",
            }
        }

        self.assertEqual("catcol1", get_inverse_references("cat1", "col1"))
        self.assertEqual({}, get_inverse_references("cat1", "nonexistent_col"))

    @patch("gobapi.graphql.schema.connection_fields", {'key': 'value'})
    def test_get_connection_field(self):
        self.assertEqual('value', get_connection_field('key')())
        self.assertEqual(GenericScalar, get_connection_field('non_existing_key')())
