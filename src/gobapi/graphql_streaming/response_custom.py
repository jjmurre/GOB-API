from distutils.util import strtobool
import geojson
import shapely.wkt

from gobapi.graphql_streaming.response import GraphQLStreamingResponseBuilder


class GraphQLCustomStreamingResponseBuilder(GraphQLStreamingResponseBuilder):

    def __init__(self, *args, **kwargs):
        """
        Initialize a custom reponse builder

        If no request args are present this response builder behaves identical to the base class response builder.
        Only the _build_entity method has been overridden.

        :param args:
        :param kwargs:
        """
        request_args = kwargs.pop('request_args', {})
        self.parse_options(request_args)
        super().__init__(*args, **kwargs)

    def parse_options(self, request_args):
        """
        Parse any request arguments

        :param request_args:
        :return:
        """
        def parse_list(l):
            """
            Convert a comma separated list of values into an array of strings

            :param l:
            :return:
            """
            items = (l or "").split(",")
            items = [s.strip() for s in items]
            return [s for s in items if s]

        self.flatten = strtobool(request_args.get('flatten') or 'false')
        self.lowercase = strtobool(request_args.get('lowercase') or 'false')

        self.condens = parse_list(request_args.get('condens'))
        self.id = parse_list(request_args.get('id'))
        self.geojson = parse_list(request_args.get('geojson'))

        self.schema = request_args.get('schema')

        options = ['flatten', 'lowercase', 'condens', 'id', 'geojson', 'schema']
        self.has_options = any([request_args.get(opt) for opt in options])

    def _build_entity(self, collected_rows: list):
        """
        Customized build entity method

        Uses the build entity from the base class
        and return the customized result

        :param collected_rows:
        :return:
        """
        entity = super()._build_entity(collected_rows)
        if self.has_options:
            # Only customize the behaviour if any options have been supplied
            entity = self._customized_entity(entity)
            self._add_schema(entity)
        return entity

    def to_geojson(self, geometrie):
        """
        Convert a WKT geometry to GEOJson

        :param geometrie:
        :return:
        """
        g1 = shapely.wkt.loads(geometrie)
        g2 = geojson.Feature(geometry=g1, properties={})
        return g2.geometry

    def key(self, *args):
        """
        Get the key value for any given combination of key values

        Example: key('status', 'code') => 'statuscode'

        :param args:
        :return:
        """
        def keyvalue(k):
            return k.lower() if self.lowercase else k

        return "".join([keyvalue(arg) for arg in args])

    def _customized_entity(self, entity):
        """
        Returns a customized entity

        Any 1-item dictionary that have a key that is in the condens list are replaced by a single value
        Any dictionary are replaced by individual values, eg {a: {b: 1, c: 2}} => {a_b: 1, a_c: 2}

        :param entity:
        :return:
        """
        if self.id and isinstance(entity, dict):
            # Create an id out of the id fields (self.id)
            self._auto_id(entity)

        if self.condens and isinstance(entity, dict):
            # Condense a dictionary, for example to remove edges and node
            condense_key = self._condense_key(entity)
            if condense_key:
                return self._condense_dict(entity, condense_key)

        if isinstance(entity, dict):
            return self._customized_dict(entity)
        elif isinstance(entity, list):
            return [self._customized_entity(v) for v in entity]
        else:
            return entity

    def _customized_dict(self, entity):
        """
        Customized entity

        Convert any WKT to GEOJson, flatten any sub dicts
        Standardize key names

        :param entity:
        :return:
        """
        result = {}
        for key, value in entity.items():
            keyvalue = self.key(key)
            if key in self.geojson:
                result[keyvalue] = self.to_geojson(entity[key]) if entity[key] else None
            elif self.flatten:
                flat_value = self._customized_entity(value)
                if isinstance(flat_value, dict):
                    for subkey, subvalue in flat_value.items():
                        subkeyvalue = self.key(key, subkey)
                        result[subkeyvalue] = subvalue
                else:
                    result[keyvalue] = flat_value
            else:
                result[keyvalue] = self._customized_entity(value)
        return result

    def _add_schema(self, entity):
        """
        If any schema name is specified then add this key-value to the entity
        :param entity:
        :return:
        """
        if self.schema and isinstance(entity, dict):
            entity['schema'] = self.schema

    def _auto_id(self, entity):
        """
        Derive an id from a combination of fields, normally identification + volgnummer

        :param entity:
        :return:
        """
        if self.id[0] in entity:
            fields = [entity.get(id) for id in self.id]
            id = ".".join([str(f) for f in fields if f])
            entity['id'] = id

    def _condense_key(self, entity):
        """
        If the entity can be condensed to a single value then return the condense key

        :param entity:
        :return:
        """
        for key in self.condens:
            if key in entity:
                ids_in_key = [id for id in self.id if entity.get(id)]
                max_size = len(ids_in_key) + 1 if key == 'id' else 1
                if len(entity.keys()) <= max_size:
                    return key

    def _condense_dict(self, entity, key):
        """
        Condense a dictionary to a single value

        If the value is a list with 0 or 1 elements then also condense this list to a single value

        :param entity:
        :param key:
        :return:
        """
        result = self._customized_entity(entity[key])
        if isinstance(result, list):
            return self.condens_list(result)
        return result

    def condens_list(self, entity):
        """
        Condens a 0-1-item list to its value

        Empty list values are converted to None: [] => None
        Single-item list values are converted to the single-item value: [a] => a

        :param entity:
        :return:
        """
        return {
            0: lambda: None,
            1: lambda: entity[0]
        }.get(len(entity), lambda: entity)()
