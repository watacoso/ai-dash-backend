"""
EncryptedJSON — SQLAlchemy TypeDecorator that transparently encrypts/decrypts
JSON values using Fernet symmetric encryption.

The encryption key is read from app.config.settings.encryption_key at runtime.
"""
import json

from cryptography.fernet import Fernet
from sqlalchemy import Text
from sqlalchemy.types import TypeDecorator


class EncryptedJSON(TypeDecorator):
    """Stores a Python dict as an encrypted text column."""

    impl = Text
    cache_ok = True

    def _fernet(self) -> Fernet:
        from app.config import settings
        return Fernet(settings.encryption_key.encode())

    def process_bind_param(self, value, dialect):
        """Python → DB: encrypt."""
        if value is None:
            return None
        plaintext = json.dumps(value).encode()
        return self._fernet().encrypt(plaintext).decode()

    def process_result_value(self, value, dialect):
        """DB → Python: decrypt."""
        if value is None:
            return None
        plaintext = self._fernet().decrypt(value.encode())
        return json.loads(plaintext)
