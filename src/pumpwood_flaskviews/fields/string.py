"""Marshmallow string fields with load-time normalization.

Provides ``LowerCaseStringField``, ``UpperCaseStringField``, and
``SlugFieldStringField``. Each transforms incoming payload values on
deserialize (save); dump output reflects the value stored on the model.
"""
from marshmallow import fields
from slugify import slugify


class LowerCaseStringField(fields.String):
    """String field that normalizes values to lowercase on load only.

    Serialization uses the parent ``fields.String`` behavior without
    transforming values read from the model.
    """

    def _deserialize(
            self, value, attr, data, **kwargs) -> str | None:
        """Deserialize and normalize the value to lowercase.

        Args:
            value:
                Raw input for this attribute.
            attr (str):
                Attribute name on the schema.
            data (dict):
                Full input data dictionary.
            **kwargs:
                Extra arguments passed to the parent deserializer.

        Returns:
            str | None:
                Lowercase string, or None when the parent returns None.
        """
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is not None:
            return value.lower()
        return value


class UpperCaseStringField(fields.String):
    """String field that normalizes values to uppercase on load only.

    Serialization uses the parent ``fields.String`` behavior without
    transforming values read from the model.
    """

    def _deserialize(
            self, value, attr, data, **kwargs) -> str | None:
        """Deserialize and normalize the value to uppercase.

        Args:
            value:
                Raw input for this attribute.
            attr (str):
                Attribute name on the schema.
            data (dict):
                Full input data dictionary.
            **kwargs:
                Extra arguments passed to the parent deserializer.

        Returns:
            str | None:
                Uppercase string, or None when the parent returns None.
        """
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is not None:
            return value.upper()
        return value


class SlugFieldStringField(fields.String):
    """String field that normalizes values to a slug on load only.

    Uses ``python-slugify`` on deserialize. Serialization uses the
    parent ``fields.String`` behavior without transforming stored
    values.
    """

    def _deserialize(
            self, value, attr, data, **kwargs) -> str | None:
        """Deserialize and normalize the value to a URL slug.

        Args:
            value:
                Raw input for this attribute.
            attr (str):
                Attribute name on the schema.
            data (dict):
                Full input data dictionary.
            **kwargs:
                Extra arguments passed to the parent deserializer.

        Returns:
            str | None:
                Slug string, or None when the parent returns None.
        """
        value = super()._deserialize(value, attr, data, **kwargs)
        if value is not None:
            return slugify(value)
        return value
