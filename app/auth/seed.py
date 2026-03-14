"""
Seed script for local development and E2E testing.
Run: python -m app.auth.seed
Creates default analyst and admin users if they do not already exist.
"""
import asyncio
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.auth.models import User, Role
from app.auth.service import hash_password


async def _ensure_user(session, email: str, name: str, role: Role, password: str) -> None:
    result = await session.execute(select(User).where(User.email == email))
    if result.scalar_one_or_none():
        print(f"Seed user already exists — skipping: {email}")
        return
    user = User(
        email=email,
        name=name,
        role=role,
        hashed_password=hash_password(password),
        is_active=True,
    )
    session.add(user)
    print(f"Seed user created: {email} / {password}")


async def seed() -> None:
    async with AsyncSessionLocal() as session:
        await _ensure_user(session, "analyst@example.com", "Test Analyst", Role.analyst, "password123")
        await _ensure_user(session, "admin@example.com", "Test Admin", Role.admin, "adminpass123")
        await session.commit()


if __name__ == "__main__":
    asyncio.run(seed())
