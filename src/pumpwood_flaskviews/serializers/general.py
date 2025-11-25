"""Set base serializers for PumpWood systems."""
from marshmallow import validates, fields, ValidationError, EXCLUDE
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from pumpwood_flaskviews.fields import (
    PrimaryKeyField, MicroserviceForeignKeyField, MicroserviceRelatedField)
from pumpwood_communication.exceptions import PumpWoodQueryException


def get_model_class(obj):
    """Get model's name and add a suffix if ENDPOINT_SUFFIX is set."""
    model_name = obj.__class__.__name__
    return model_name


class PumpWoodSerializer(SQLAlchemyAutoSchema):
    """Default PumpWood Serializer."""

    pk = PrimaryKeyField(allow_none=True, required=False, dump_only=True)
    model_class = fields.Function(get_model_class, dump_only=True)

    def __init__(self, fields: list = None, foreign_key_fields: bool = False,
                 related_fields: bool = False, many: bool = False,
                 default_fields: bool = False,
                 only: list = None, *args, **kwargs) -> None:
        """Overide Schema init to restrict dump.

        Args:
            fields (list):
                List of the fields that will be returned at the
                serializer. Backward compatibylity, it will be migrated
                to only to be similar with marshmallow API.
            only (list):
                Will restrict the fields that will be serialized.
            foreign_key_fields (bool):
                If foreign key associated fields should be returned at the
                serializer.
            related_fields (bool):
                If related fields M2M fields should be returned at the
                serializer.
            default_fields (bool):
                With the default fields should be returned.
            many (bool):
                If it will be passed a list of instances or just one.
            *args:
                Compatibility with other versions.
            **kwargs:
                Compatibility with other versions.
        """
        kwargs["many"] = many

        # Backward compatibility
        if only is None and fields is not None:
            only = fields

        # Generate only fields according to list fields of default_fields
        if only is None and default_fields:
            only = self.get_list_fields()

        # Remove fields that are not on fields and are FK related to reduce #
        # requests to other micro services
        to_remove = []
        for key, item in self._declared_fields.items():
            # Keep all fields declared on fields, independent if it is
            # fk or related
            if only is not None:
                if key in only:
                    continue

            # Keep related only if user ask to keep them
            is_related_micro = isinstance(
                item, MicroserviceRelatedField)
            if is_related_micro and not related_fields:
                to_remove.append(key)
                continue

            # Keep FK only if user ask for them
            is_foreign_key_micro = isinstance(
                item, MicroserviceForeignKeyField)
            if is_foreign_key_micro and not foreign_key_fields:
                to_remove.append(key)
                continue

        # Guaranty that fields will no conflict with exclude
        if only is not None:
            only = list(set(only) - set(to_remove))

        # Validate if all only and exlcude fields are present on
        # Model or the serializer definition
        self._validate_fields(fields=only + to_remove)

        kwargs["only"] = only
        kwargs["exclude"] = to_remove

        print("Ok!!")
        # Adjusting compatibility with previous versions of
        # Marshmellow SQLAlchemy
        kwargs["unknown"] = EXCLUDE  # Default excluding not mapped fields
        kwargs['load_instance'] = True  # load_instance as default
        super().__init__(**kwargs)

    def _validate_fields(self, fields: list[str]) -> None:
        """Validate if fields are declared at Serializer or at Model.

        Raises:
            PumpWoodQueryException:
                Raise PumpWoodQueryException if fields are not present on
                model or serializer.
        """
        # Fetch fields defined at model and serializer and check if
        # fields are present, if not raise an PumpWoodQueryException

        model_field_names = set(self.opts.model.__table__.columns.keys())
        explicit_field_names = set(self._declared_fields.keys())
        valid_field_names = model_field_names | explicit_field_names

        not_present_fields = set(fields) - valid_field_names
        if len(not_present_fields):
            msg = (
                "Requested Fields {fields} are not present on model [{model}] "
                "definition")
            raise PumpWoodQueryException(
                message=msg, payload={
                    "fields": list(not_present_fields),
                    "model": self.opts.model.__name__
                })

    def get_list_fields(self) -> list:
        """Get list fields from serializer.

        Args:
            No Args.

        Return:
            Default fields to be used at list and retrive with
            default_fields=True.
        """
        list_fields = getattr(self.Meta, 'list_fields', None)
        if list_fields is None:
            return list(self.fields.keys())
        return list_fields

    def get_foreign_keys(self) -> dict:
        """Return a dictonary with all foreign_key fields.

        Args:
            No Args.

        Kwargs:
            No Kwargs.

        Return:
            Return a dictionary with field name as keys and relation
            information as value.
        """
        return_dict = {}
        for field_name, field in self._declared_fields.items():
            is_micro_fk = isinstance(field, MicroserviceForeignKeyField)
            if is_micro_fk:
                return_dict[field.source] = field.to_dict()
        return return_dict

    def get_related_fields(self) -> dict:
        """Return a dictionary with all related fields (M2M).

        Args:
            No Args.

        Kwargs:
            No Kwargs.

        Return:
            Return a dictionary with field name as keys and relation
            information as value.
        """
        return_dict = {}
        for field_name, field in self._declared_fields.items():
            is_micro_rel = isinstance(field, MicroserviceRelatedField)
            if is_micro_rel:
                return_dict[field_name] = field.to_dict()
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
