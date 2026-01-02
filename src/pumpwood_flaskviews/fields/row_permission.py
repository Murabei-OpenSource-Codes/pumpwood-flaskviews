"""Pumpwood Marshmellow audit fields."""
from marshmallow import fields
from pumpwood_communication.exceptions import PumpWoodWrongParameters
from pumpwood_flaskviews.auth import AuthFactory


class RowPermissionField(fields.Integer):
    """Integer Field that validate if user has row_permission."""

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    def __init__(self, *args, **kwargs):
        """__init__.

        Since row_permission is allowed most of the cases to be None,
        indicating not restricted data, it will be set allow_none=True as
        default.
        """
        # Set allow_none to True by default if not explicitly provided
        kwargs.setdefault('allow_none', True)
        super().__init__(*args, **kwargs)

    def _validate_user_access(self, value, attr):
        """Validate if user has access to row permission."""
        if value is None:
            return value

        user_info = AuthFactory.retrieve_authenticated_user()
        if user_info['is_superuser']:
            return value

        # Check if user has access to row permission it is trying to
        # save information on
        row_permission_set = [
            x['pk'] for x in user_info['all_row_permisson_set']]
        if value not in row_permission_set:
            msg = (
                "The user does not have permission to use "
                "row policy [{row_permission_id}] for object creation: "
                "User's authorized permissions: {user_row_permission_set}; "
                "Field associated with row policy: [{attr}].")
            raise PumpWoodWrongParameters(
                message=msg, payload={
                    "row_permission_id": value,
                    "user_row_permission_set": row_permission_set,
                    "attr": attr})
        else:
            print('_validate_user_access')
            return value

    def _deserialize(self, value, attr=None, data=None, **kwargs):
        """Remove field validation, missing not run.

        By default Marshmellow will skip deserialization i
        """
        value_temp = super()._deserialize(value, attr, data, **kwargs)
        self._validate_user_access(value=value_temp, attr=attr)
        return value_temp
