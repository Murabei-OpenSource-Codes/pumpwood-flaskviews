"""Pumpwood Marshmellow readonly fields."""
from marshmallow import fields, missing
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_flaskviews.fields.aux import _get_overwrite_audit
from pumpwood_communication.exceptions import PumpWoodObjectSavingException


class ReadOnlyChoiceField(fields.Field):
    """Create a marshmallow field to serialize ChoiceFields."""

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    def __init__(self, *args, choices: list = None, **kwargs):
        """__init__.

        Choices must be passed as a list of tuples/lists containing code,
        display name. The code will be used to save information on database.

        Ex.:
        ```
        [
            ("choice1", "Choice 1"),
            ("choice2", "Choice 2"),
            ("choice3", "Choice 3"),
        ]
        ```

        Args:
            choices (list):
                List of options that will be used for validation. It must
                be in choices format
            non_superuser_choices (list):
                List of options that non-superusers can use. Choices not
                in
            *args:
                Other positional arguments for Marshmellow fields.
            **kwargs:
                Other named arguments for Marshmellow fields.
        """
        self.choices = choices
        kwargs['allow_none'] = True
        kwargs['dump_only'] = False
        # kwargs['dump_only'] = True
        super().__init__(*args, **kwargs)

    def _validate_choice(self, value):
        """Validate choices at the field."""
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
            raise PumpWoodObjectSavingException(
                msg, payload={'value': check_value, 'choices': val_choices})

    def _serialize(self, value, attr, obj):
        if value is not None:
            return value.code
        return None

    def deserialize(self, value, attr, data, **kwargs):
        """Reimplement deserialize function."""
        current_user = AuthFactory.retrieve_authenticated_user()
        audit_value = _get_overwrite_audit(
            field=self, data=data, current_user=current_user)

        if audit_value is missing:
            return missing

        # Validate if value is in valid choices
        self._validate_choice(value=audit_value)
        if type(audit_value) is str:
            return audit_value
        else:
            return audit_value.code
