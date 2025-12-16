"""Pumpwood Marshmellow readonly fields."""
import datetime
from typing import Any
from marshmallow import fields, missing
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_flaskviews.fields.general import ChoiceField
from pumpwood_communication.exceptions import PumpWoodObjectSavingException


class ReadOnlyChoiceField(fields.Field):
    """Create a marshmallow field to serialize ChoiceFields."""

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    def __init__(self, *args, choices: list = None,
                 non_superuser_choices: list = None,
                 **kwargs):
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
        if non_superuser_choices is None:
            non_superuser_choices = list
        self.choices = choices
        self.non_superuser_choices = non_superuser_choices
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
            raise PumpWoodObjectSavingException(
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
