from datetime import datetime, timedelta, timezone

from jose import ExpiredSignatureError, JWTError, jwt

from app.config import settings

_INVITE_TTL_HOURS = 48


def create_invite_token(role: str) -> str:
    payload = {
        "purpose": "invite",
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(hours=_INVITE_TTL_HOURS),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_invite_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    except ExpiredSignatureError:
        raise Exception("Invite token expired")
    except JWTError:
        raise Exception("Invalid invite token")
    if payload.get("purpose") != "invite":
        raise Exception("Token is not an invite token")
    return payload
