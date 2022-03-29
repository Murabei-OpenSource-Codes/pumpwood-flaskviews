from geoalchemy2.shape import from_shape, to_shape
from shapely import geometry
from marshmallow import fields


# Robado de @om-henners om-henners/serializer_utils.py
class GeometryField(fields.Field):
    """
    Create a marshmallow field to recieve geometry data.

    Use shapely and geoalchemy2 to serialize / deserialize a point
    Does make a big assumption about the data being spat back out as
    JSON, but what the hey.
    """

    def _serialize(self, value, attr, obj):
        if value is None:
            return None
        return geometry.mapping(to_shape(value))

    def _deserialize(self, value, attr, data):
        if value is None:
            return None
        return from_shape(geometry.shape(value), srid=4326)


# Choice field
class ChoiceField(fields.Field):
    """Create a marshmallow field to serialize ChoiceFields."""

    def _serialize(self, value, attr, obj):
        if value is not None:
            return value.code
        return None

    def _deserialize(self, value, attr, data):
        return value
