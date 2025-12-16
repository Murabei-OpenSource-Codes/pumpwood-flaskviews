"""Pumpwood Marshmellow audit fields."""
import datetime
from marshmallow import fields, missing
from pumpwood_flaskviews.auth import AuthFactory
from pumpwood_flaskviews.fields.aux import _get_overwrite_audit


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
        if overwrited_data is not missing:
            return overwrited_data

        # Return logged user
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
