"""Pumpwood Marshmellow audit fields."""
import datetime
from marshmallow import fields
from pumpwood_flaskviews.auth import AuthFactory


class CreatedByIdField(fields.Integer):
    """Use auth class to retrieve autenticated user and set it's id.

    It will set the authenticated user at object creation.
    """

    def __init__(self, *args, **kwargs):
        """__init__."""
        kwargs['allow_none'] = True
        kwargs['dump_only'] = True
        kwargs['default'] = 'Logged user at creation'
        super().__init__(*args, **kwargs)

    def deserialize(self, value, attr=None, data=None, **kwargs):
        """Remove field validation, missing not run.

        By default Marshmellow will skip deserialization i
        """
        parent_instance = getattr(self.parent, "instance", None)
        if parent_instance is None:
            current_user = AuthFactory.retrieve_authenticated_user()
            return current_user['pk']
        else:
            return getattr(parent_instance, attr)


class UpdatedByIdField(fields.Integer):
    """Use auth class to retrieve autenticated user and set it's id.

    It will set the authenticated user at object update.
    """
    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    def __init__(self, *args, **kwargs):
        """__init__."""
        kwargs['allow_none'] = True
        kwargs['default'] = 'Logged user at update'
        super().__init__(*args, **kwargs)

    def deserialize(self, value, attr=None, data=None, **kwargs):
        """Remove field validation, missing not run.

        By default Marshmellow will skip deserialization i
        """
        current_user = AuthFactory.retrieve_authenticated_user()
        return current_user['pk']


class CreatedAtField(fields.DateTime):
    """Set the time the object was created."""

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    def __init__(self, *args, **kwargs):
        """__init__."""
        kwargs['allow_none'] = True
        kwargs['dump_only'] = True
        kwargs['default'] = 'Datetime at creation'
        super().__init__(*args, **kwargs)

    def deserialize(self, value, attr=None, data=None, **kwargs):
        """Overide the default behavior."""
        parent_instance = getattr(self.parent, "instance", None)
        if parent_instance is None:
            return datetime.datetime.now(datetime.UTC)
        else:
            return getattr(parent_instance, attr)


class UpdatedAtField(fields.DateTime):
    """Set the time the object was updated."""

    pumpwood_read_only = True
    """Used on view to retrieve if field is read only for pumpwood."""

    def __init__(self, *args, **kwargs):
        """__init__."""
        kwargs['allow_none'] = True
        kwargs['default'] = 'Datetime at update'
        super().__init__(*args, **kwargs)

    def deserialize(self, value, attr=None, data=None, **kwargs):
        """Overide the default behavior."""
        print('UpdatedAtField.deserialize')
        return datetime.datetime.now(datetime.UTC)
