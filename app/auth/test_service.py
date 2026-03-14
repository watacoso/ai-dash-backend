import pytest
from freezegun import freeze_time
from jose import jwt

from app.auth.service import create_token, decode_token, hash_password, verify_password
from app.config import settings


class TestHashPassword:
    def test_should_return_bcrypt_hash_when_given_plain_password(self):
        # Arrange
        plain = "mysecret"
        # Act
        result = hash_password(plain)
        # Assert
        assert result.startswith("$2b$")
        assert result != plain

    def test_should_return_different_hashes_for_same_input(self):
        # Arrange / Act
        hash1 = hash_password("mysecret")
        hash2 = hash_password("mysecret")
        # Assert
        assert hash1 != hash2


class TestVerifyPassword:
    def test_should_return_true_when_password_matches_hash(self):
        # Arrange
        plain = "mysecret"
        hashed = hash_password(plain)
        # Act
        result = verify_password(plain, hashed)
        # Assert
        assert result is True

    def test_should_return_false_when_password_does_not_match_hash(self):
        # Arrange
        hashed = hash_password("mysecret")
        # Act
        result = verify_password("wrong", hashed)
        # Assert
        assert result is False


class TestCreateToken:
    def test_should_return_jwt_with_correct_claims(self):
        # Arrange
        user_id = "abc-123"
        role = "admin"
        # Act
        token = create_token(user_id, role)
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
        # Assert
        assert payload["sub"] == user_id
        assert payload["role"] == role

    def test_should_include_expiry_claim(self):
        # Arrange / Act
        token = create_token("abc-123", "analyst")
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
        # Assert
        assert "exp" in payload


class TestDecodeToken:
    def test_should_return_claims_for_valid_token(self):
        # Arrange
        token = create_token("abc-123", "admin")
        # Act
        result = decode_token(token)
        # Assert
        assert result["sub"] == "abc-123"
        assert result["role"] == "admin"

    def test_should_raise_for_expired_token(self):
        # Arrange
        from datetime import datetime, timedelta, timezone
        from jose import jwt as _jwt
        expired_token = _jwt.encode(
            {"sub": "abc-123", "role": "admin", "exp": datetime.now(timezone.utc) - timedelta(seconds=1)},
            settings.jwt_secret,
            algorithm=settings.jwt_algorithm,
        )
        # Act / Assert
        with pytest.raises(Exception, match="expired"):
            decode_token(expired_token)

    def test_should_raise_for_tampered_token(self):
        # Arrange
        token = create_token("abc-123", "admin")
        tampered = token[:-4] + "xxxx"
        # Act / Assert
        with pytest.raises(Exception):
            decode_token(tampered)
