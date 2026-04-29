"""Set base serializers for PumpWood systems."""
import inspect
from marshmallow import validates, fields, ValidationError, EXCLUDE
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from pumpwood_flaskviews.fields import (
    PrimaryKeyField, MicroserviceForeignKeyField, MicroserviceRelatedField,
    LocalForeignKeyField, LocalRelatedField)
from pumpwood_communication.exceptions import PumpWoodQueryException


def get_model_class(obj: object | type) -> str:
    """Retrieve the class name of the given object or class.

    Args:
        obj (object | type):
            The instance or class to inspect.

    Returns:
        str:
            The name of the class.
    """
    if inspect.isclass(obj):
        return obj.__name__
    return obj.__class__.__name__


class PumpWoodSerializer(SQLAlchemyAutoSchema):
    """Default PumpWood Serializer."""

    pk = PrimaryKeyField(allow_none=True, required=False, dump_only=True)
    model_class = fields.Function(get_model_class, dump_only=True)

    def __init__(self, fields: list = None, foreign_key_fields: bool = False,
                 related_fields: bool = False, many: bool = False,
                 default_fields: bool = False,
                 only: list = None, *args, **kwargs) -> None:
        """Initialize the serializer with specific visibility constraints.

        Args:
            fields (list):
                Legacy parameter for specific fields. (Alias for `only`).
            only (list):
                Restricts the fields to be serialized.
            foreign_key_fields (bool):
                If True, includes expanded foreign key relations.
            related_fields (bool):
                If True, includes expanded M2M relations.
            default_fields (bool):
                If True, uses the default field set defined in `Meta`.
            many (bool):
                Whether serializing a collection or a single instance.
            *args:
                For compatibility with base class.
            **kwargs:
                For compatibility with base class.
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
                item, (MicroserviceRelatedField, LocalRelatedField))
            if is_related_micro and not related_fields:
                to_remove.append(key)
                continue

            # Keep FK only if user ask for them
            is_foreign_key_micro = isinstance(
                item, (MicroserviceForeignKeyField, LocalForeignKeyField))
            if is_foreign_key_micro and not foreign_key_fields:
                to_remove.append(key)
                continue

        # Guaranty that fields will no conflict with exclude
        if only is not None:
            only = list(set(only) - set(to_remove))

        # Validate if all only and exlcude fields are present on
        # Model or the serializer definition
        # use or when only and to_remove are not set
        self._validate_fields(fields=(
            (only or []) + (to_remove or [])))

        kwargs["only"] = only
        kwargs["exclude"] = to_remove

        # Adjusting compatibility with previous versions of
        # Marshmellow SQLAlchemy
        kwargs["unknown"] = EXCLUDE  # Default excluding not mapped fields
        kwargs['load_instance'] = True  # load_instance as default
        super().__init__(**kwargs)

    def _validate_fields(self, fields: list[str] | None) -> None:
        """Validate if the provided fields exist on the model or serializer.

        Args:
            fields (list[str] | None):
                The fields to validate.

        Returns:
            None

        Raises:
            PumpWoodQueryException:
                If any field is missing from the model and declaration.
        """
        if fields is None:
            return None

        # Fetch fields defined at model and serializer and check if
        # fields are present, if not raise an PumpWoodQueryException
        model_field_names = set(self.opts.model.__table__.columns.keys())
        explicit_field_names = set(self._declared_fields.keys())
        valid_field_names = model_field_names | explicit_field_names

        not_present_fields = set(fields) - valid_field_names
        if len(not_present_fields):
            msg = (
                "Requested fields {fields} are not present on model [{model}] "
                "definition")
            raise PumpWoodQueryException(
                message=msg, payload={
                    "fields": list(not_present_fields),
                    "model": self.opts.model.__name__})

    def get_list_fields(self) -> list:
        """Retrieve the default list fields from Meta or declared keys.

        Returns:
            list:
                The set of fields used for default serialization.
        """
        list_fields = getattr(self.Meta, 'list_fields', None)
        if list_fields is None:
            fields = getattr(self.Meta, 'fields', None)
            return list(fields.keys())
        return list_fields

    def get_gui_readonly(self) -> list:
        """Retrieve the list of fields marked as read-only for the GUI.

        Returns:
            list:
                Field names that should be read-only in frontend views.
        """
        gui_readonly = getattr(self.Meta, 'gui_readonly', None)
        if gui_readonly is None:
            gui_readonly = list()
        return gui_readonly

    def get_foreign_keys(self) -> dict:
        """Map all declared microservice or local foreign key fields.

        Returns:
            dict:
                A dictionary mapping field names to relation metadata.
        """
        return_dict = {}
        for field_name, field in self._declared_fields.items():
            is_micro_fk = getattr(field, '_PUMPWOOD_FK', False)
            if is_micro_fk:
                # Use the fist source which msut be the main fk associated
                # with the id from the other model class
                info_object = field.to_dict()
                return_dict[info_object.source_keys[0]] = info_object
        return return_dict

    def get_related_fields(self) -> dict:
        """Map all declared microservice or local related (M2M) fields.

        Returns:
            dict:
                A dictionary mapping field names to relation metadata.
        """
        return_dict = {}
        for field_name, field in self._declared_fields.items():
            is_micro_rel = getattr(field, '_PUMPWOOD_RELATED', False)
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


def validate_categorical_value(n: int | float):
    """Validate that a value is a non-negative integer.

    Args:
        n (int | float):
            The value to validate.

    Raises:
        ValidationError:
            If the value is negative or not an integer.
    """
    if n < 0:
        raise ValidationError('Quantity must be greater than 0.')
    if type(n) is not int:
        if not n.is_integer():
            raise ValidationError('Categorical values must be integers.')
