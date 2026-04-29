"""Fields that have values encrypted on save."""
from marshmallow import fields, missing
from pumpwood_communication.encrypt import PumpwoodCryptography
from pumpwood_flaskviews.fields.aux import (
    _get_overwrite_audit, _import_function_by_string)
from pumpwood_flaskviews.auth import AuthFactory



encrypt_obj = PumpwoodCryptography()
"""Object used to encrypt fields."""


class EncryptedField(fields.Field):
    """Field used to store encrypted values in the database.

    Utilizes the `PumpwoodCryptography` class from the
    `pumpwood_communication` package for data encryption. The
    `PUMPWOOD_COMMUNICATION__CRYPTO_FERNET_KEY` environment variable
    must be set, otherwise encryption operations will raise an error.
    """

    def _serialize(self, value, attr, obj):
        """Convert the internal model value for output.

        Args:
            value (Any):
                The value to serialize.
            attr (str):
                The attribute name.
            obj (object):
                The object being serialized.

        Returns:
            Any:
                The unmodified value (as it's already encrypted or null).
        """
        return value

    def deserialize(self, value, attr, data):
        """Standardize the incoming value and encrypt it if necessary.

        Args:
            value (Any):
                The value to deserialize.
            attr (str):
                The attribute name.
            data (dict):
                The full payload being deserialized.

        Returns:
            Any:
                The encrypted value, or the existing value if it is already
                encrypted and fixed.
        """
        overwrited_data = _get_overwrite_audit(
            field=self, data=data, current_user=None,
            raise_not_superuser=False)

        if overwrited_data is not missing:
            encrypted_value = encrypt_obj.encrypt(value=overwrited_data)
            return encrypted_value

        # Access the existing object from the schema context
        existing_obj = getattr(self.parent, "instance", None)
        if existing_obj is not None:
            # If the object was already setted, get the value from the field
            # this is used when updating an object, this will not make
            # recursive encription when and object is updated with encripted
            # field already encrypted.
            existing_value = getattr(existing_obj, attr, None)
            return existing_value
        else:
            if value is not missing:
                # If the object was not setted, encrypt the value on the first
                # save
                return encrypt_obj.encrypt(value=value)
            else:
                return value
