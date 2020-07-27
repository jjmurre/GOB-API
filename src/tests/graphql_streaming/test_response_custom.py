from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobapi.graphql_streaming.response_custom import GraphQLCustomStreamingResponseBuilder


class TestGraphQLCustomStreamingResponseBuilder(TestCase):

    def test_init(self):
        rb = GraphQLCustomStreamingResponseBuilder(None, None, None)
        self.assertIsInstance(rb, GraphQLCustomStreamingResponseBuilder)
        self.assertEqual(rb.condens, [])
        self.assertEqual(rb.flatten, False)

    @patch('gobapi.graphql_streaming.response_custom.GraphQLStreamingResponseBuilder._build_entity')
    def test_build_entity(self, mock_super_build_entity):
        rb = GraphQLCustomStreamingResponseBuilder(None, None, None)
        rb._customized_entity = MagicMock()
        result = rb._build_entity('any entity')
        mock_super_build_entity.assert_called_with('any entity')
        rb._customized_entity.assert_not_called()
        self.assertEqual(result, mock_super_build_entity.return_value)

        request = {
            'condens': 'any condens'
        }
        rb = GraphQLCustomStreamingResponseBuilder(None, None, None, request_args=request)
        rb._customized_entity = MagicMock()
        result = rb._build_entity('any entity')
        rb._customized_entity.assert_called_with(mock_super_build_entity.return_value)
        self.assertEqual(result, rb._customized_entity.return_value)

        request = {
            'flatten': 'true'
        }
        rb = GraphQLCustomStreamingResponseBuilder(None, None, None, request_args=request)
        rb._customized_entity = MagicMock()
        rb._build_entity('any entity')
        rb._customized_entity.assert_called()

    def test_flatten(self):
        entity = {
            "identificatie": "10281154",
            "status": {
                "code": 1,
                "omschrijving": "Actueel"
            }
        }
        request = {
            'flatten': 'true'
        }
        rb = GraphQLCustomStreamingResponseBuilder(None, None, None, request_args=request)
        self.assertTrue(rb.has_options)
        result = rb._customized_entity(entity)
        expect = {
            'identificatie': '10281154',
            'statuscode': 1,
            'statusomschrijving': 'Actueel'
        }
        self.assertEqual(result, expect)

    def test_lowercase(self):
        entity = {
            'identificatie': '10281154',
            "ligtInBouwblok": {
                "edges": [
                    {
                        "node": {
                            "identificatie": "03630012101200",
                            "volgnummer": 1
                        }
                    }
                ]
            }
        }
        request = {
            'lowercase': 'true'
        }
        rb = GraphQLCustomStreamingResponseBuilder(None, None, None, request_args=request)
        self.assertTrue(rb.has_options)
        result = rb._customized_entity(entity)
        expect = {
            'identificatie': '10281154',
            "ligtinbouwblok": {
                "edges": [
                    {
                        "node": {
                            "identificatie": "03630012101200",
                            "volgnummer": 1
                        }
                    }
                ]
            }
        }
        self.assertEqual(result, expect)

    def test_condens(self):
        entity = {
            'identificatie': '10281154',
            "ligtInBouwblok": {
                "edges": [
                    {
                        "node": {
                            "identificatie": "03630012101200",
                            "volgnummer": 1
                        }
                    }
                ]
            }
        }
        request = {
            'condens': 'edges,node'
        }
        rb = GraphQLCustomStreamingResponseBuilder(None, None, None, request_args=request)
        self.assertTrue(rb.has_options)
        result = rb._customized_entity(entity)
        expect = {
            'identificatie': '10281154',
            'ligtInBouwblok': {
                'identificatie': '03630012101200',
                'volgnummer': 1
            }
        }
        self.assertEqual(result, expect)

    def test_id(self):
        entity = {
            'identificatie': '10281154',
            "ligtInBouwblok": {
                "edges": [
                    {
                        "node": {
                            "identificatie": "03630012101200",
                            "volgnummer": 1
                        }
                    }
                ]
            }
        }
        request = {
            'id': 'identificatie,volgnummer'
        }
        rb = GraphQLCustomStreamingResponseBuilder(None, None, None, request_args=request)
        self.assertTrue(rb.has_options)
        result = rb._customized_entity(entity)
        expect = {
            'id': '10281154',
            'identificatie': '10281154',
            "ligtInBouwblok": {
                "edges": [
                    {
                        "node": {
                            "id": "03630012101200.1",
                            "identificatie": "03630012101200",
                            "volgnummer": 1
                        }
                    }
                ]
            }
        }
        self.assertEqual(result, expect)

    def test_id_condens(self):
        entity = {
            'identificatie': '10281154',
            "ligtInBouwblok": {
                "edges": [
                    {
                        "node": {
                            "identificatie": "03630012101200",
                            "volgnummer": 1
                        }
                    }
                ]
            }
        }
        request = {
            'id': 'identificatie,volgnummer',
            'condens': 'edges,node,id'
        }
        rb = GraphQLCustomStreamingResponseBuilder(None, None, None, request_args=request)
        self.assertTrue(rb.has_options)
        result = rb._customized_entity(entity)
        expect = {
            'id': '10281154',
            'identificatie': '10281154',
            "ligtInBouwblok": "03630012101200.1"
        }
        self.assertEqual(result, expect)

    def test_geojson(self):
        entity = {
            "identificatie": "10281154",
            "geometrie1": "POINT(119411.7 487201.6)",
            "geometrie2": "POINT(119411.7 487201.6)",
            "geometrie3": "POINT(119411.7 487201.6)"
        }
        request = {
            'geojson': 'geometrie1,geometrie3'
        }
        rb = GraphQLCustomStreamingResponseBuilder(None, None, None, request_args=request)
        self.assertTrue(rb.has_options)
        result = rb._customized_entity(entity)
        expect = {
            'identificatie': '10281154',
            'geometrie1': {
                "type": "Point",
                "coordinates": [119411.7, 487201.6]
            },
            'geometrie2': 'POINT(119411.7 487201.6)',
            'geometrie3': {
                "type": "Point",
                "coordinates": [119411.7, 487201.6]
            },
        }
        self.assertEqual(result, expect)

    @patch('gobapi.graphql_streaming.response_custom.GraphQLStreamingResponseBuilder._build_entity')
    def test_schema(self, mock_super_build_entity):
        entity = {
            "identificatie": "10281154"
        }
        request = {
            'schema': 'schema name'
        }
        rb = GraphQLCustomStreamingResponseBuilder(None, None, None, request_args=request)
        self.assertTrue(rb.has_options)
        mock_super_build_entity.return_value = entity
        result = rb._build_entity(entity)
        expect = {
            'identificatie': '10281154',
            'schema': 'schema name'
        }
        self.assertEqual(result, expect)
