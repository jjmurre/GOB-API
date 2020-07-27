"""
Utility to convert GOB Data to Amsterdam Schema

"""
import argparse
import re
import json
import os
import sys

# Suppress any output from GOBModel class (otherwise GOB Model messages can appear in the schema output)
sys.stdout = open(os.devnull, 'w')
from gobcore.model import GOBModel  # noqa: E402, module level import not at top of file
from gobcore.model.metadata import FIELD  # noqa: E402, module level import not at top of file
from gobcore.sources import GOBSources  # noqa: E402, module level import not at top of file
model = GOBModel()
sources = GOBSources()
sys.stdout = sys.__stdout__


def get_schema(catalog_name, collection_name=None):
    """
    Get a Amsterdam Schema for the given catalog

    If a collection is specified only the schema for the given collection is returned

    :param catalog_name:
    :param collection_name:
    :return:
    """
    if collection_name:
        schema = _get_collection_schema(catalog_name, collection_name)
    else:
        tables = []
        collections = model.get_collections(catalog_name)
        for collection_name, collection in collections.items():
            collection_schema = _get_collection_schema(catalog_name, collection_name)
            tables.append({
                "id": f"{collection_name}",
                "type": "table",
                "schema": collection_schema
            })
        schema = {
            "type": "dataset",
            "id": catalog_name,
            "title": catalog_name,
            "status": "beschikbaar",
            "version": "0.0.1",
            "crs": "EPSG:28992",
            "tables": tables
        }
    return json.dumps(schema, indent=2, ensure_ascii=False)


def _get_collection_schema(catalog_name, collection_name):
    """
    Get the schema for a single collection

    :param catalog_name:
    :param collection_name:
    :return:
    """
    collection = GOBModel().get_collection(catalog_name, collection_name)
    fields = collection['all_fields']
    required = ["schema", "id", "identificatie"]
    has_states = model.has_states(catalog_name, collection_name)
    if has_states:
        required.append(FIELD.SEQNR)
    properties = {
        "schema": {
            "$ref": "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/schema"
        },
        "id": {
            "type": "string",
            "description": f"Unieke identificatie voor dit object{', inclusief volgnummer' if has_states else ''}"
        }
    }
    for field_name, field in fields.items():
        if re.match(r'^_', field_name):
            # Skip GOB metadata fields
            continue
        if field['type'] == 'GOB.JSON':
            # JSON fields are split into separate properties
            for sub_field_name, sub_field in field['attributes'].items():
                description = f"{field.get('description', '')} {sub_field_name}".strip()
                sub_field_name = f'{field_name}_{sub_field_name}'
                property_name, property = _get_field_property(sub_field_name, sub_field, description)
                properties[property_name] = property
        else:
            property_name, property = _get_field_property(field_name, field)
            properties[property_name] = property
    return {
        "$id": f"https://github.com/Amsterdam/schemas/{catalog_name}/{collection_name}.json",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "additionalProperties": False,
        "required": required,
        "display": "id",
        "properties": properties
    }


def _get_field_property(field_name, field, description=None):
    """
    Convert a field into a table property

    :param field_name:
    :param field:
    :param description:
    :return:
    """
    field_type = field['type']
    if re.match(r'^GOB\.Geo\.', field_type):
        # Any GOB Geometry
        property = {"$ref": "https://geojson.org/schema/Geometry.json"}
    elif re.match(r'^GOB\..*ManyReference', field_type):
        # Any GOB (Very)ManyReference
        property = {"type": "array", "items": {"type": "string"}, 'relation': field['ref']}
    else:
        # Any GOB base type
        property = {
            'GOB.String': lambda: {'type': "string"},
            'GOB.SecureString': lambda: {'type': "string"},
            'GOB.Character': lambda: {'type': "string"},
            'GOB.Decimal': lambda: {'type': "number"},
            'GOB.Integer': lambda: {'type': "integer"},
            'GOB.Boolean': lambda: {'type': "boolean"},
            'GOB.Date': lambda: {'type': "string", 'format': "date"},
            'GOB.SecureDate': lambda: {'type': "string", 'format': "date"},
            'GOB.DateTime': lambda: {'type': "string", 'format': "datetime"},
            'GOB.Reference': lambda: {'type': "string", 'relation': field['ref']}
        }.get(field_type, lambda: None)()

    if not property:
        # Fail on unrecognized type
        raise NotImplementedError(f"Conversion for {field_type} not yet implemented")

    # Include description
    property['description'] = description or field.get('description', '')
    # Property names are lowercase strings without underscores
    field_name = field_name.replace('_', '').lower()
    return field_name, property


def to_camel_case(snake_str):
    """
    Convert a camel case string to snake case

    :param snake_str:
    :return:
    """
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def get_graphql_query(catalog_name, collection_name):    # noqa: C901, too complex
    """
    Get the GraphQL query for the given catalog collection

    Build a dict with '' values and then afterwards convert it to JSON
    and remove the '' values to get a nicely formatted GraphQL query

    :param catalog_name:
    :param collection_name:
    :return:
    """
    collection = model.get_collection(catalog_name, collection_name)
    node = {
        collection['entity_id']: '',
        'cursor': ''
    }
    if model.has_states(catalog_name, collection_name):
        node[FIELD.SEQNR] = ''
    fields = collection['all_fields']
    for field_name, field in fields.items():
        field_type = field['type']
        cc_field_name = to_camel_case(field_name)  # GraphQL field names are camelCase
        if re.match(r'^_', field_name):
            # Skip meta fields
            continue
        elif re.match(r'^GOB\.(?!Very).*Reference', field_type):
            # Build an edges-nodes structure for any reference field (except VeryManyReference)
            ref = field['ref']
            ref_catalog_name, ref_collection_name = ref.split(':')
            ref_collection = model.get_collection(ref_catalog_name, ref_collection_name)

            if ref_collection:
                ref_node = {ref_collection['entity_id']: ''}
                if model.has_states(ref_catalog_name, ref_collection_name):
                    ref_node[FIELD.SEQNR] = ''
                node[cc_field_name] = {
                    'edges': {
                        'node': ref_node
                    }
                }
            else:
                # Undefined relation
                node[cc_field_name] = ''
        else:
            node[cc_field_name] = ''

    name = to_camel_case(f'{catalog_name}_{collection_name}')
    filter = 'active: false'
    if 'publiceerbaar' in fields.keys():
        filter += ', publiceerbaar: true'
    query = {
        '__NAME__': {
            'edges': {
                'node': node
            }
        }
    }
    # Use json.dumps to nicely format the GraphQL query
    json_schema = json.dumps(query, indent=2)
    json_schema = re.sub(r'[":,]', '', json_schema)
    return json_schema.replace('__NAME__', f'{name}({filter})')


def get_url(catalog_name, collection_name, path):
    """
    Get the URL, including all required parameters, to use when issueing a POST request to retrieve the data

    :param catalog_name:
    :param collection_name:
    :return:
    """
    collection = model.get_collection(catalog_name, collection_name)
    fields = collection['all_fields']
    # geojson parameter
    geojson = []
    for field_name, field in fields.items():
        field_type = field['type']
        if re.match(r'^GOB\.Geo\.', field_type):
            geojson.append(field_name)

    args = {
        'condens': "node,edges,id",
        'lowercase': "true",
        'flatten': "true",
        'id': "identificatie,volgnummer",  # volgnummer is ignored automatically for collections without state
        'schema': f"{catalog_name}_{collection_name}"
    }
    if geojson:
        args['geojson'] = ",".join(geojson)

    # Construct the query string
    args = '&'.join([f'{arg}={args[arg]}' for arg in args.keys()])
    return f'{path}?{args}'


def get_curl(catalog_name, collection_name, path):
    """
    Get the curl statement to retrieve the dataset in the Amsterdam Schema format

    :param catalog_name:
    :param collection_name:
    :return:
    """
    query = get_graphql_query(catalog_name, collection_name).replace('\n', '')
    query = '{"query":"%s"}' % query
    header = 'Content-Type: application/x-ndjson'
    url = get_url(catalog_name, collection_name, path)
    return f"curl -s --location --request POST '{url}' --header '{header}' --data-raw '{query}'"


if __name__ == "__main__":   # noqa: C901, too complex
    class CollectionAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            if namespace.format in ['query', 'curl'] and values is None:
                parser.error('query or curl require a collection')
            elif values and not GOBModel().get_collection(namespace.catalog, values):
                parser.error(f"GOB Collection '{values}' does not exist within GOB Catalog '{namespace.catalog}'")
            else:
                namespace.collection = values

    class CatalogAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            if not GOBModel().get_catalog(values):
                parser.error(f"GOB Catalog '{values}' does not exist")
            else:
                namespace.catalog = values

    parser = argparse.ArgumentParser(
        description='Amsterdam Schema and GraphQL-Query Generator.',
        usage='python %(prog)s [-h] (schema catalog [collection] | (query | curl) catalog collection)')
    parser.add_argument('format', type=str,
                        choices=['schema', 'query', 'curl'], help='schema, query or curl')
    parser.add_argument('catalog', type=str, action=CatalogAction, help='catalog name, eg meetbouten')
    parser.add_argument('collection', nargs="?", action=CollectionAction, default=None, type=str,
                        help='collection name, eg metingen')
    parser.add_argument('--path', default='https://acc.api.data.amsterdam.nl/gob/graphql/streaming/', type=str,
                        help='url path to use for the curl statement, default: %(default)s')

    args = parser.parse_args()

    result = {
        'schema': lambda: get_schema(args.catalog, args.collection),
        'query': lambda: get_graphql_query(args.catalog, args.collection),
        'curl': lambda: get_curl(args.catalog, args.collection, path=args.path)
    }[args.format]()

    print(result)

    if args.format in ('query', 'curl'):
        print("")
        print("\033[33mNOTE: For large collections, use pagination in the query (using first, after and cursor)"
              "\033[0m")
