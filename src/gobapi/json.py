from gobcore.typesystem.json import GobTypeJSONEncoder
from gobcore.typesystem.gob_types import DateTime
from gobcore.typesystem import GOB

from gobapi import serialize
from gobapi.utils import object_to_camelcase


class APIGobTypeJSONEncoder(GobTypeJSONEncoder):
    """Extension of the GobTypeJSONEncoder to help turn the internal format of
     datetime in to isoformat

    Use as follows:

        import json

        gob_type = DateTime.from_value('2019-01-01T12:00:00.123456')
        json.dumps(gob_type, cls=APIGobTypeJSONEncoder)
    """
    def default(self, obj):
        if isinstance(obj, GOB.JSON):
            return object_to_camelcase(obj.to_value)

        if isinstance(obj, DateTime):
            dt = obj.to_value
            return None if dt is None else serialize.datetime_value(obj.to_value)

        return super().default(obj)
