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
        """Serialize a value to a JSON string.

        Args:
            obj:
                Object to serialize (Pumpwood extended types supported).
            **kwargs:
                Ignored; kept for Flask ``JSONProvider`` compatibility.

        Returns:
            str:
                UTF-8 JSON text.
        """
        # orjson.dumps returns a bytes object, so we decode it.
        return pumpJsonDump(obj).decode('utf-8')

    def loads(self, s, **kwargs):
        """Deserialize a JSON string to a Python object.

        Args:
            s (str | bytes):
                JSON text or bytes to parse.
            **kwargs:
                Ignored; kept for Flask ``JSONProvider`` compatibility.

        Returns:
            object:
                Parsed JSON value.
        """
        # orjson.loads expects a bytes-like object.
        return orjson.loads(s)
