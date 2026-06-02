"""Pumpwood Marshmallow general fields."""
from geoalchemy2.shape import from_shape, to_shape
from shapely import geometry
from marshmallow import fields
from sqlalchemy import inspect as sqlalchemy_inspect
from pumpwood_communication import exceptions
from pumpwood_communication.serializers import CompositePkBase64Converter


# Based on @om-henners om-henners/serializer_utils.py
class GeometryField(fields.Field):
    """Marshmallow field to handle geometry data serialization.

    Utilizes Shapely and GeoAlchemy2 to manage point data. Assumes
    output format is compatible with GeoJSON mappings.
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
    """Marshmallow field to serialize and validate ChoiceFields."""

    def __init__(self, *args, choices: list[tuple] = None, **kwargs):
        """Initialize the choice field.

        Args:
            choices (list[tuple]):
                A list of (code, human_readable) tuples or lists used to
                validate the field values.
            *args:
                Positional arguments for the base Marshmallow Field.
            **kwargs:
                Keyword arguments for the base Marshmallow Field.

        Example:
            >>> field_choice = ChoiceField(
            >>>     choices=[
            >>>         ('choice_1', 'Choice One'),
            >>>         ('choice_2', 'Choice Two'),
            >>>     ])
        """
        self.choices = choices
        validators = kwargs.pop("validate", [])

        # Add choice validation if
        if self.choices is not None:
            validators.append(self._validate_choice)
        super().__init__(validate=validators, *args, **kwargs)

    def _validate_choice(self, value):
        """Validate if the provided value is among the allowed choices.

        Args:
            value (Any):
                The value to validate. Can be a string or an object with
                a 'code' attribute.

        Raises:
            PumpWoodObjectSavingException:
                If the value is not found within the allowed choices.
        """
        if self.allow_none and value is None:
            return None
        check_value = None
        val_choices = [x[0] for x in self.choices]
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
        """Serialize the field value.

        If the value is a string, it returns it directly. Otherwise, it
        attempts to return its 'code' attribute.

        Args:
            value (Any):
                The field value to serialize.
            attr (str):
                The name of the attribute.
            obj (Any):
                The object owning the attribute.

        Returns:
            str:
                The serialized string value.
        """
        if value is not None:
            if isinstance(value, str):
                return value

            return value.code
        return None

    def _deserialize(self, value, attr, data):
        """Deserialize the input value.

        Args:
            value (Any):
                The input value to deserialize.
            attr (str):
                The name of the attribute.
            data (dict):
                The raw input data dictionary.

        Returns:
            str:
                The deserialized string value.

        Raises:
            PumpWoodObjectSavingException:
                If the value is not a string.
        """
        # It is necessary to treat the string cases, at some implementation
        # the value of the attribute will be set direcly on Python.
        # It is necessary on those since the attribute will be a string
        # on serialization.
        if isinstance(value, str):
            return value
        else:
            value_type = type(value).__name__
            msg = (
                "Value type of `ChoiceField` must be str, it was passed "
                "`{value_type}`. Value: `{value}`")
            raise exceptions.PumpWoodObjectSavingException(
                msg, payload={
                    'value_type': value_type,
                    'value': value})


class PrimaryKeyField(fields.Function):
    """Marshmallow field to serialize primary keys as Base64 strings.

    Supports composite primary keys by encoding them into a single
    Base64 string.
    """

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
