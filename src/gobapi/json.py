import datetime

from gobcore.typesystem.json import GobTypeJSONEncoder
from gobcore.typesystem.gob_types import DateTime


class APIGobTypeJSONEncoder(GobTypeJSONEncoder):
    """Extension of the GobTypeJSONEncoder to help turn the internal format of
     datetime in to isoformat

    Use as follows:

        import json

        gob_type = DateTime.from_value('2019-01-01T12:00:00.123456')
        json.dumps(gob_type, cls=APIGobTypeJSONEncoder)
    """

    def default(self, obj):
        if isinstance(obj, DateTime):
            return datetime.datetime.strptime(str(obj), DateTime.internal_format).isoformat() \
                    if obj._string is not None else obj.json

        return super().default(obj)
