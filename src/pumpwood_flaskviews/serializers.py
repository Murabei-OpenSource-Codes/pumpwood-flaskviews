"""Set base serializers for PumpWood systems."""
import os
from marshmallow import (
    validates, fields, ValidationError)
from marshmallow_sqlalchemy import ModelSchema


def get_model_class(obj):
    """Get model's name and add a suffix if ENDPOINT_SUFFIX is set."""
    suffix = os.getenv('ENDPOINT_SUFFIX', '')
    model_name = obj.__class__.__name__
    return suffix + model_name


class PumpWoodSerializer(ModelSchema):
    """Default PumpWood Serializer."""

    pk = fields.Integer(required=False, attribute="id", allow_none=True)
    model_class = fields.Function(get_model_class, dump_only=True)

    @validates('model_class')
    def validate_model_class(self, value):
        """Check if the model_class is correct."""
        if value != self.Meta.model.__name__:
            raise ValidationError(
                'model_class value (%s) must be iqual to model name (%s).' % (
                    value, self.model.__name__))


def validate_categorical_value(n):
    """Check if categorical value is valid. Greater than zero and Integer."""
    if n < 0:
        raise ValidationError('Quantity must be greater than 0.')
    if type(n) != int:
        if not n.is_integer():
            raise ValidationError(
                'Categorical values must be integers.')
