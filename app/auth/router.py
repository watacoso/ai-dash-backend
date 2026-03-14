from typing import Callable

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.models import User
from app.auth.service import create_token, decode_token, verify_password
from app.config import settings
from app.database import get_session
from app.redis import add_to_blocklist, is_blocklisted

router = APIRouter(prefix="/auth", tags=["auth"])
bearer = HTTPBearer(auto_error=False)

_COOKIE_NAME = "access_token"

_INVALID_CREDENTIALS = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Invalid credentials",
)


class LoginRequest(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(bearer),
    session: AsyncSession = Depends(get_session),
) -> User:
    # Prefer cookie; fall back to Bearer header
    token = request.cookies.get(_COOKIE_NAME)
    if not token:
        if not credentials:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
        token = credentials.credentials

    if await is_blocklisted(token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token revoked")
    try:
        payload = decode_token(token)
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")
    if "role" not in payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token claims")
    user = await session.get(User, payload["sub"])
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")
    return user


def require_role(role: str) -> Callable:
    """Return a FastAPI dependency that enforces a minimum role."""
    async def dependency(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role.value != role:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Insufficient permissions")
        return current_user
    return dependency


@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest, response: Response, session: AsyncSession = Depends(get_session)):
    result = await session.execute(select(User).where(User.email == body.email))
    user = result.scalar_one_or_none()
    if not user or not user.is_active or not verify_password(body.password, user.hashed_password):
        raise _INVALID_CREDENTIALS
    token = create_token(str(user.id), user.role.value)
    response.set_cookie(
        key=_COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="strict",
        max_age=settings.jwt_expiry_seconds,
        path="/",
    )
    return TokenResponse(access_token=token)


@router.post("/logout", status_code=status.HTTP_200_OK)
async def logout(request: Request, response: Response,
                 credentials: HTTPAuthorizationCredentials = Depends(bearer)):
    # Blocklist whichever token we can find
    token = request.cookies.get(_COOKIE_NAME)
    if not token:
        if not credentials:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
        token = credentials.credentials
    await add_to_blocklist(token)
    response.set_cookie(
        key=_COOKIE_NAME,
        value="",
        httponly=True,
        samesite="strict",
        max_age=0,
        path="/",
    )
    return {"detail": "Logged out"}


@router.get("/me")
async def me(current_user: User = Depends(get_current_user)):
    return {"id": str(current_user.id), "email": current_user.email, "role": current_user.role}
