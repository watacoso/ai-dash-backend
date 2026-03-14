import uuid

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.models import Role, User
from app.auth.router import get_current_user, require_role
from app.admin.service import create_invite_token, decode_invite_token
from app.database import get_session
from app.redis import is_invite_used, mark_invite_used

router = APIRouter(prefix="/admin", tags=["admin"])


# ── Shared response model ──────────────────────────────────────────────────────

class UserSummary(BaseModel):
    id: str
    email: str
    name: str
    role: str
    is_active: bool


# ── GET /admin/users ───────────────────────────────────────────────────────────

@router.get("/users", response_model=list[UserSummary])
async def list_users(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_role("admin")),
):
    result = await session.execute(select(User))
    users = result.scalars().all()
    return [
        UserSummary(id=str(u.id), email=u.email, name=u.name,
                    role=u.role.value, is_active=u.is_active)
        for u in users
    ]


# ── POST /admin/invite ─────────────────────────────────────────────────────────

class InviteRequest(BaseModel):
    role: str


class InviteResponse(BaseModel):
    invite_url: str


@router.post("/invite", response_model=InviteResponse, status_code=status.HTTP_201_CREATED)
async def generate_invite(
    body: InviteRequest,
    request: Request,
    _: User = Depends(require_role("admin")),
):
    if body.role not in (r.value for r in Role):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="Invalid role")
    token = create_invite_token(body.role)
    base = str(request.base_url).rstrip("/")
    return InviteResponse(invite_url=f"{base}/accept-invite?token={token}")


# ── POST /admin/users/accept-invite ───────────────────────────────────────────

class AcceptInviteRequest(BaseModel):
    token: str
    email: str
    name: str
    password: str


@router.post("/users/accept-invite", response_model=UserSummary,
             status_code=status.HTTP_201_CREATED)
async def accept_invite(
    body: AcceptInviteRequest,
    session: AsyncSession = Depends(get_session),
):
    # Validate token
    try:
        payload = decode_invite_token(body.token)
    except Exception:
        raise HTTPException(status_code=status.HTTP_410_GONE, detail="Invite expired or invalid")

    # Check single-use
    if await is_invite_used(body.token):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Invite already used")

    # Check email not taken
    existing = await session.execute(select(User).where(User.email == body.email))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already registered")

    from app.auth.service import hash_password
    user = User(
        email=body.email,
        name=body.name,
        role=Role(payload["role"]),
        hashed_password=hash_password(body.password),
        is_active=True,
    )
    session.add(user)
    await mark_invite_used(body.token)
    await session.commit()
    return UserSummary(id=str(user.id), email=user.email, name=user.name,
                       role=user.role.value, is_active=user.is_active)


# ── PATCH /admin/users/{id}/role ──────────────────────────────────────────────

class PatchRoleRequest(BaseModel):
    role: str


@router.patch("/users/{user_id}/role", response_model=UserSummary)
async def patch_role(
    user_id: uuid.UUID,
    body: PatchRoleRequest,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_role("admin")),
):
    if body.role not in (r.value for r in Role):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="Invalid role")
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    user.role = Role(body.role)
    await session.commit()
    return UserSummary(id=str(user.id), email=user.email, name=user.name,
                       role=user.role.value, is_active=user.is_active)


# ── PATCH /admin/users/{id}/active ────────────────────────────────────────────

class PatchActiveRequest(BaseModel):
    is_active: bool


@router.patch("/users/{user_id}/active", response_model=UserSummary)
async def patch_active(
    user_id: uuid.UUID,
    body: PatchActiveRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    _: User = Depends(require_role("admin")),
):
    if current_user.id == user_id and not body.is_active:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail="Cannot deactivate your own account")
    user = await session.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    user.is_active = body.is_active
    await session.commit()
    return UserSummary(id=str(user.id), email=user.email, name=user.name,
                       role=user.role.value, is_active=user.is_active)
