"""Pumpwood Marshmellow audit fields."""
import datetime
from typing import Any
from marshmallow import fields, missing
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_communication.exceptions import PumpWoodForbidden


def _get_overwrite_audit(field: fields.Field, data: dict,
                         current_user: dict) -> None | Any:
    """Get overwrite data for a field and check if user can overwrite.

    Only superusers can overwrite audit fields. Overwrite value will be
    fetched from `'__overwrite__' + json_key`.

    Args:
        field (fields.Field):
            Audit field that value will be overwrited.
        data (dict):
            Data associated with the request, field values will be fetched
            from `'__overwrite__' + json_key` at request data.
        current_user (dict):
            Current user associated with request

    Returns:
        Returns value associated with overwrite data or None if value not
        found.

    Raises:
        PumpWoodForbidden:
            Raise PumpWoodForbidden if overwrite key is setted, but
            user is not superuser.
    """
    # Check if overwrite key is setted on request dictionary
    json_key = (
        field.data_key if field.data_key is not None else field.name)
    overwrite_key = '__overwrite__' + json_key
    if overwrite_key not in data.keys():
        # Return missing to avoid None that can be a value
        return missing

    else:
        is_superuser = current_user.get('is_superuser', False)
        if not is_superuser:
            msg = (
                "User is truing to overwrite an audit field [{json_key}], "
                "using [{overwrite_key}] entry, but does not have associated "
                "permissions")
            raise PumpWoodForbidden(
                message=msg, payload={
                    'json_key': json_key,
                    'overwrite_key': overwrite_key
                })

    # Get overwrite information from key
    return data.get(overwrite_key)


class CreatedByIdField(fields.Integer):
    """Use auth class to retrieve autenticated user and set it's id.

    It will set the authenticated user at object creation.

    This field will alway consider `allow_none=True` and `dump_only=False`.
    Default is set as a message used at fill options.
    """

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    def __init__(self, *args, **kwargs):
        """__init__."""
        kwargs['allow_none'] = True
        kwargs['dump_only'] = False
        kwargs['default'] = 'Logged user at creation'
        super().__init__(*args, **kwargs)

    def deserialize(self, value, attr=None, data=None, **kwargs):
        """Remove field validation, missing not run.

        By default Marshmellow will skip deserialization i
        """
        current_user = AuthFactory.retrieve_authenticated_user()
        overwrited_data = _get_overwrite_audit(
            field=self, data=data, current_user=current_user)
        if overwrited_data is not missing:
            return overwrited_data

        parent_instance = getattr(self.parent, "instance", None)
        if parent_instance is None:
            return current_user['pk']
        else:
            return getattr(parent_instance, attr)


class ModifiedByIdField(fields.Integer):
    """Use auth class to retrieve autenticated user and set it's id.

    It will set the authenticated user at object update.

    This field will alway consider `allow_none=True` and `dump_only=False`.
    Default is set as a message used at fill options.
    """
    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    def __init__(self, *args, **kwargs):
        """__init__."""
        kwargs['allow_none'] = True
        kwargs['dump_only'] = False
        kwargs['default'] = 'Logged user at update'
        super().__init__(*args, **kwargs)

    def deserialize(self, value, attr=None, data=None, **kwargs):
        """Remove field validation, missing not run.

        By default Marshmellow will skip deserialization i
        """
        current_user = AuthFactory.retrieve_authenticated_user()
        overwrited_data = _get_overwrite_audit(
            field=self, data=data, current_user=current_user)
        print('overwrited_data:', overwrited_data)
        if overwrited_data is not missing:
            return overwrited_data

        # Return logged user
        print("current_user['pk']:", current_user['pk'])
        return current_user['pk']


class CreatedAtField(fields.DateTime):
    """Set the time the object was created.

    This field will alway consider `allow_none=True` and `dump_only=False`.
    Default is set as a message used at fill options.
    """

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    def __init__(self, *args, **kwargs):
        """__init__."""
        kwargs['allow_none'] = True
        kwargs['dump_only'] = False
        kwargs['default'] = 'Datetime at creation'
        super().__init__(*args, **kwargs)

    def deserialize(self, value, attr=None, data=None, **kwargs):
        """Overide the default behavior."""
        current_user = AuthFactory.retrieve_authenticated_user()
        overwrited_data = _get_overwrite_audit(
            field=self, data=data, current_user=current_user)
        if overwrited_data is not missing:
            return overwrited_data

        parent_instance = getattr(self.parent, "instance", None)
        if parent_instance is None:
            return datetime.datetime.now(datetime.UTC)
        else:
            return getattr(parent_instance, attr)


class ModifiedAtField(fields.DateTime):
    """Set the time the object was updated.

    This field will alway consider `allow_none=True` and `dump_only=False`.
    Default is set as a message used at fill options.
    """

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    def __init__(self, *args, **kwargs):
        """__init__."""
        kwargs['allow_none'] = True
        kwargs['dump_only'] = False
        kwargs['default'] = 'Datetime at update'
        super().__init__(*args, **kwargs)

    def deserialize(self, value, attr=None, data=None, **kwargs):
        """Overide the default behavior."""
        current_user = AuthFactory.retrieve_authenticated_user()
        overwrited_data = _get_overwrite_audit(
            field=self, data=data, current_user=current_user)
        if overwrited_data is not missing:
            return overwrited_data

        return datetime.datetime.now(datetime.UTC)
