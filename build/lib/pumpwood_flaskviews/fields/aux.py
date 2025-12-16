"""Module for auxiliary fuctions for fields."""
from typing import Any
from marshmallow import fields, missing


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
                "User is trying to overwrite an audit field [{json_key}], "
                "using [{overwrite_key}] entry, but does not have associated "
                "permissions")
            raise PumpWoodForbidden(
                message=msg, payload={
                    'json_key': json_key,
                    'overwrite_key': overwrite_key
                })

    # Get overwrite information from key
    return data.get(overwrite_key)
