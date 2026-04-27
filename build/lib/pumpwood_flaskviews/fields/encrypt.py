"""Fields that have values encrypted on save."""
from marshmallow import fields, missing
from pumpwood_communication.encrypt import PumpwoodCryptography
from pumpwood_flaskviews.fields.aux import (
    _get_overwrite_audit, _import_function_by_string)
from pumpwood_flaskviews.auth import AuthFactory



encrypt_obj = PumpwoodCryptography()
"""Object used to encrypt fields."""


class EncryptedField(fields.Field):
    """Field to store encrypted values on database.

    It will user pumpwood_communication PumpwoodCryptography to encrypt
    information. It is necessary to set
    `PUMPWOOD_COMUNICATION__CRYPTO_FERNET_KEY` enviroment variable or an
    error will be raised on serialization.
    """

    def _serialize(self, value, attr, obj):
        """Convert object to dictionary (JSON)."""
        return value

    def deserialize(self, value, attr, data):
        """Convert dictionary to object."""
        print("value:", value)
        print("attr:", attr)
        print("data:", data)
        overwrited_data = _get_overwrite_audit(
            field=self, data=data, current_user=None,
            raise_not_superuser=False)
        print("overwrited_data:", overwrited_data)
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
            # If the object was not setted, encrypt the value on the first
            # save
            encrypted_value = encrypt_obj.encrypt(value=value)
            return encrypted_value
