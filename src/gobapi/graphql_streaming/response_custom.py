import geojson
import shapely.wkt

from gobapi.graphql_streaming.response import GraphQLStreamingResponseBuilder


class GraphQLCustomStreamingResponseBuilder(GraphQLStreamingResponseBuilder):

    def __init__(self, *args, **kwargs):
        request_args = kwargs.pop('request_args', {})
        self.parse_options(request_args)
        super().__init__(*args, **kwargs)

    def parse_options(self, request_args):
        def parse_list(l):
            items = (l or "").split(",")
            items = [s.strip() for s in items]
            return [s for s in items if s]

        self.flatten = request_args.get('flatten') or False
        self.lowercase = request_args.get('lowercase') or False

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
            entity = self._customized_entity(entity)
            self._add_schema(entity)
        return entity

    def to_geojson(self, geometrie):
        g1 = shapely.wkt.loads(geometrie)
        g2 = geojson.Feature(geometry=g1, properties={})
        return g2.geometry

    def key(self, *args):
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
            self._auto_id(entity)

        if self.condens and isinstance(entity, dict):
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
        result = {}
        for key, value in entity.items():
            if key in self.geojson:
                result[self.key(key)] = self.to_geojson(entity[key])
            elif self.flatten:
                flat_value = self._customized_entity(value)
                if isinstance(flat_value, dict):
                    for subkey, subvalue in flat_value.items():
                        result[self.key(key, subkey)] = subvalue
                else:
                    result[self.key(key)] = flat_value
            else:
                result[self.key(key)] = self._customized_entity(value)
        return result

    def _add_schema(self, entity):
        if self.schema and isinstance(entity, dict):
            entity['schema'] = self.schema

    def _auto_id(self, entity):
        if self.id[0] in entity:
            fields = [entity.get(id) for id in self.id]
            id = ".".join([str(f) for f in fields if f])
            entity['id'] = id

    def _condense_key(self, entity):
        for key in self.condens:
            if key in entity:
                ids_in_key = [id for id in self.id if entity.get(id)]
                max_size = len(ids_in_key) + 1 if key == 'id' else 1
                if len(entity.keys()) <= max_size:
                    return key

    def _condense_dict(self, entity, key):
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
