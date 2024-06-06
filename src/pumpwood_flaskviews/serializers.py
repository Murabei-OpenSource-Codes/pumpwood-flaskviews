"""Set base serializers for PumpWood systems."""
import os
from marshmallow import validates, fields, ValidationError
from marshmallow_sqlalchemy import ModelSchema
from pumpwood_flaskviews.fields import (
    PrimaryKeyField, MicroserviceForeignKeyField, MicroserviceRelatedField)


def get_model_class(obj):
    """Get model's name and add a suffix if ENDPOINT_SUFFIX is set."""
    suffix = os.getenv('ENDPOINT_SUFFIX', '')
    model_name = obj.__class__.__name__
    return suffix + model_name


class PumpWoodSerializer(ModelSchema):
    """Default PumpWood Serializer."""

    pk = PrimaryKeyField(allow_none=True, required=False, dump_only=True)
    model_class = fields.Function(get_model_class, dump_only=True)

    def __init__(self, fields: list = None, foreign_key_fields: bool = False,
                 related_fields: bool = False, many: bool = False,
                 default_fields: bool = False, *args, **kwargs) -> None:
        """
        Overide Schema init to restrict dump.

        Args:
            fields [list]: List of the fields that will be returned at the
                serializer.
            foreign_key_fields [bool]: If foreign key associated fields should
                be returned at the serializer.
            related_fields [bool]: If related fields M2M fields should
                be returned at the serializer.
        """
        kwargs["many"] = many
        super().__init__(**kwargs)

        if fields is None and default_fields:
            fields = self.get_list_fields()

        # Remove fields that are not on fields and are fk related to reduce #
        # requests to other microservices
        to_remove = []
        for key, item in self.fields.items():
            # If field are set then use fields that were sent by user to make
            # serialization
            if fields is not None:
                if key not in fields:
                    to_remove.append(key)
            else:
                # Keep related only if user ask to keep them
                is_related_micro = isinstance(
                    item, MicroserviceRelatedField)
                if is_related_micro and (many or not related_fields):
                    to_remove.append(key)
                    continue

                # Keep FK only if user ask for them
                is_foreign_key_micro = isinstance(
                    item, MicroserviceForeignKeyField)
                if is_foreign_key_micro and not foreign_key_fields:
                    to_remove.append(key)
                    continue
        self.only = [
            field_name for field_name in self.fields.keys()
            if field_name not in to_remove]

    def get_list_fields(self):
        """
        Get list fields from serializer.

        Args:
            No Args.
        Return [list]:
            Default fields to be used at list and retrive with
            default_fields=True.
        """
        list_fields = getattr(self.Meta, 'list_fields', None)
        if list_fields is None:
            return list(self.fields.keys())
        return list_fields

    def get_foreign_keys(self) -> dict:
        """
        Return a dictonary with all foreign_key fields.

        Args:
            No Args.
        Kwargs:
            No Kwargs.
        Return [dict]:
            Return a dictionary with field name as keys and relation
            information as value.
        """
        return_dict = {}
        for field_name, field in self.fields.items():
            is_micro_fk = isinstance(field, MicroserviceForeignKeyField)
            if is_micro_fk:
                return_dict[field.source] = field.to_dict()
        return return_dict

    def get_related_fields(self):
        """
        Return a dictionary with all related fields (M2M).

        Args:
            No Args.
        Kwargs:
            No Kwargs.
        Return [dict]:
            Return a dictionary with field name as keys and relation
            information as value.
        """
        return_dict = {}
        for field_name, field in self.fields.items():
            is_micro_rel = isinstance(field, MicroserviceRelatedField)
            if is_micro_rel:
                return_dict[field.name] = field.to_dict()
        return return_dict

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
    if type(n) is not int:
        if not n.is_integer():
            raise ValidationError('Categorical values must be integers.')
