"""Pumpwood Marshmallow string fields."""
from marshmallow import fields
from slugify import slugify


class LowerCaseStringField(fields.String):
    """String field that normalizes values to lowercase on load only."""

    def _deserialize(self, value, attr, data, **kwargs):
        """Deserialize and normalize the value to lowercase."""
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is not None:
            return value.lower()
        return value


class UpperCaseStringField(fields.String):
    """String field that normalizes values to uppercase on load only."""

    def _deserialize(self, value, attr, data, **kwargs):
        """Deserialize and normalize the value to uppercase."""
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is not None:
            return value.upper()
        return value


class SlugFieldStringField(fields.String):
    """String field that normalizes values to a slug on load only."""

    def _deserialize(self, value, attr, data, **kwargs):
        """Deserialize and normalize the value to a slug."""
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is not None:
            return slugify(value)
        return value
