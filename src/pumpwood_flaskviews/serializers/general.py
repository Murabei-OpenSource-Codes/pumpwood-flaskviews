"""Base Marshmallow serializers for Pumpwood SQLAlchemy models."""
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
    """Default PumpWood SQLAlchemy serializer with FK/related toggles.

    Declares ``pk``, ``id``, and ``model_class`` on every subclass. They
    are included in output when present in ``only`` (for example through
    ``default_fields=True`` and ``Meta.list_fields``).

    Use ``Meta.list_fields`` for list-view defaults. Set
    ``foreign_key_fields`` or ``related_fields`` at instantiation to
    expand nested objects instead of stripping them from ``exclude``.
    """

    pk = PrimaryKeyField(allow_none=True, required=False, dump_only=True)
    id = fields.Integer(allow_none=True, required=False, dump_only=True)
    model_class = fields.Function(get_model_class, dump_only=True)

    def __init__(self, fields: list[str] | None = None, foreign_key_fields: bool = False,
            related_fields: bool = False, many: bool = False,
            default_fields: bool = False, only: list[str] | None = None,
            *args, **kwargs) -> None:
        """Initialize the serializer with specific visibility constraints.

        ``fields`` is a legacy alias for ``only``. When ``default_fields``
        is True, ``only`` is built from ``get_list_fields()``. Foreign-key
        and related serializer fields are removed unless the matching
        boolean flag is True.

        Args:
            fields (list[str] | None):
                Legacy parameter for specific fields. Alias for ``only``.
            only (list[str] | None):
                Restricts the fields to be serialized.
            foreign_key_fields (bool):
                If True, includes expanded foreign key relations.
            related_fields (bool):
                If True, includes expanded M2M relations.
            default_fields (bool):
                If True, uses ``get_list_fields()`` as ``only``.
            many (bool):
                Whether serializing a collection or a single instance.
            *args:
                For compatibility with base class.
            **kwargs:
                For compatibility with base class.

        Raises:
            PumpWoodQueryException:
                If any requested field is missing from the model or
                serializer declaration.
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
        # Marshmallow SQLAlchemy
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

    def get_list_fields(self) -> list[str]:
        """Retrieve default list fields from ``Meta``.

        Resolution order:

        1. ``Meta.list_fields`` when set.
        2. Otherwise keys of ``Meta.fields`` when it is a mapping.

        Include ``pk``, ``id``, and ``model_class`` in ``Meta.list_fields``
        when list responses must expose Pumpwood identity fields.

        Returns:
            list[str]:
                Field names used when ``default_fields=True``.
        """
        list_fields = getattr(self.Meta, 'list_fields', None)
        if list_fields is None:
            meta_fields = getattr(self.Meta, 'fields', None)
            return list(meta_fields.keys())
        return list(list_fields)

    def get_gui_readonly(self) -> list[str]:
        """Retrieve the list of fields marked as read-only for the GUI.

        Returns:
            list[str]:
                Field names that should be read-only in frontend views.
        """
        gui_readonly = getattr(self.Meta, 'gui_readonly', None)
        if gui_readonly is None:
            gui_readonly = list()
        return gui_readonly

    def get_foreign_keys(self) -> dict[str, object]:
        """Map declared microservice or local foreign key fields.

        Returns:
            dict[str, object]:
                Foreign-key source column names mapped to relation
                metadata from ``field.to_dict()``.
        """
        return_dict = {}
        for field_name, field in self._declared_fields.items():
            is_micro_fk = getattr(field, '_PUMPWOOD_FK', False)
            if is_micro_fk:
                # Use the first source which must be the main fk associated
                # with the id from the other model class
                info_object = field.to_dict()
                return_dict[info_object.source_keys[0]] = info_object
        return return_dict

    def get_related_fields(self) -> dict[str, object]:
        """Map declared microservice or local related (M2M) fields.

        Returns:
            dict[str, object]:
                Related field names mapped to relation metadata from
                ``field.to_dict()``.
        """
        return_dict = {}
        for field_name, field in self._declared_fields.items():
            is_micro_rel = getattr(field, '_PUMPWOOD_RELATED', False)
            if is_micro_rel:
                return_dict[field_name] = field.to_dict()
        return return_dict

    @validates('model_class')
    def validate_model_class(self, value: str) -> None:
        """Validate that ``model_class`` matches the bound SQLAlchemy model.

        Args:
            value (str):
                ``model_class`` value from the payload.

        Raises:
            ValidationError:
                If ``value`` differs from ``Meta.model.__name__``.
        """
        if value != self.Meta.model.__name__:
            raise ValidationError(
                'model_class value (%s) must be equal to model name (%s).' % (
                    value, self.model.__name__))


def validate_categorical_value(n: int | float) -> None:
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
