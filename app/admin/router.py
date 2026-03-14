from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.models import User
from app.auth.router import require_role
from app.database import get_session

router = APIRouter(prefix="/admin", tags=["admin"])


class UserSummary(BaseModel):
    id: str
    email: str
    name: str
    role: str
    is_active: bool


@router.get("/users", response_model=list[UserSummary])
async def list_users(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_role("admin")),
):
    result = await session.execute(select(User))
    users = result.scalars().all()
    return [
        UserSummary(
            id=str(u.id),
            email=u.email,
            name=u.name,
            role=u.role.value,
            is_active=u.is_active,
        )
        for u in users
    ]
