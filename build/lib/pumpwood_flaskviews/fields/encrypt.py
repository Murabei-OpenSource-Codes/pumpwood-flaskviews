"""Fields that have values encrypted on save."""
from marshmallow import fields
from pumpwood_communication.encrypt import PumpwoodCryptography


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

    def _deserialize(self, value, attr, data):
        """Convert dictionary to object."""
        encrypted_value = encrypt_obj.encrypt(value=value)
        return encrypted_value
