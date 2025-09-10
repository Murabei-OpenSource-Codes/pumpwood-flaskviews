"""Module for serialization of Json for complex fields."""
import orjson
from flask.json.provider import DefaultJSONProvider
from pumpwood_communication.serializers import pumpJsonDump


class PumpWoodFlaskJSONProvider(DefaultJSONProvider):
    """PumpWood default serializer.

    Treat not simple python types to facilitate at serialization of
    pandas, numpy, data, datetime and other data types.
    """
    def dumps(self, obj, **kwargs):
        """Dumps a Python object to a JSON string using orjson."""
        # orjson.dumps returns a bytes object, so we decode it.
        return pumpJsonDump(obj).decode('utf-8')

    def loads(self, s, **kwargs):
        """Loads a JSON string to a Python object using orjson."""
        # orjson.loads expects a bytes-like object.
        return orjson.loads(s)
