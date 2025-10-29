"""Pumpwood Marshmellow general fields."""
from geoalchemy2.shape import from_shape, to_shape
from shapely import geometry
from marshmallow import fields
from sqlalchemy import inspect as sqlalchemy_inspect
from pumpwood_communication import exceptions
from pumpwood_communication.serializers import CompositePkBase64Converter


# Based on @om-henners om-henners/serializer_utils.py
class GeometryField(fields.Field):
    """Create a marshmallow field to recieve geometry data.

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


class ChoiceField(fields.Field):
    """Create a marshmallow field to serialize ChoiceFields."""

    def __init__(self, *args, choices=None, **kwargs):
        """__init__."""
        self.choices = choices
        validators = kwargs.pop("validate", [])
        if self.choices:
            validators.append(self._validate_choice)
        super().__init__(validate=validators, *args, **kwargs)

    def _validate_choice(self, value):
        """Validate choices at the field."""
        val_choices = [x[0] for x in self.choices]
        # Add None to possible choices if allow_none is True
        if self.allow_none:
            val_choices.append(None)

        check_value = None
        if isinstance(value, str):
            check_value = value
        else:
            check_value = getattr(value, "code", None)

        if check_value not in val_choices:
            msg = (
                "'{value}' is not a valid choice. "
                "Must be one of {choices}")
            raise exceptions.PumpWoodObjectSavingException(
                msg, payload={'value': check_value, 'choices': val_choices})

    def _serialize(self, value, attr, obj):
        if value is not None:
            return value.code
        return None

    def _deserialize(self, value, attr, data):
        # Not checking if value is a string breaks saving the object.
        if type(value) is str:
            return value
        else:
            return value.code


class PrimaryKeyField(fields.Function):
    """Create a marshmallow field to serialize ChoiceFields."""

    _primary_keys = None

    def _serialize(self, value, attr, obj, **kwargs):
        if self._primary_keys is None:
            mapper = sqlalchemy_inspect(obj.__table__)
            self._primary_keys = [
                col.name for col in list(mapper.c) if col.primary_key]
        return CompositePkBase64Converter.dump(
            obj=obj, primary_keys=self._primary_keys)

    def _deserialize(self, value, attr, data, **kwargs):
        return CompositePkBase64Converter.load(value=value)
