"""Pumpwood Marshmellow audit fields."""
import datetime
from marshmallow import fields
from pumpwood_flaskviews.auth import AuthFactory


class CreatedByIdField(fields.Field):
    """Use auth class to retrieve autenticated user and set it's id.

    It will set the authenticated user at object creation.
    """

    def _serialize(self, value, attr, obj):
        current_user = AuthFactory.retrieve_authenticated_user()
        return current_user['pk']

    def _deserialize(self, value, attr, data):
        return value


class UpdatedByIdField(fields.Field):
    """Use auth class to retrieve autenticated user and set it's id.

    It will set the authenticated user at object update.
    """

    def _serialize(self, value, attr, obj):
        current_user = AuthFactory.retrieve_authenticated_user()
        return current_user['pk']

    def _deserialize(self, value, attr, data):
        return value


class CreatedAtField(fields.Field):
    """Set the time the object was created."""

    def _serialize(self, value, attr, obj):
        return datetime.datetime.now(datetime.UTC)

    def _deserialize(self, value, attr, data):
        return value


class UpdatedAtField(fields.Field):
    """Set the time the object was updated."""

    def _serialize(self, value, attr, obj):
        return datetime.datetime.now(datetime.UTC)

    def _deserialize(self, value, attr, data):
        return value
