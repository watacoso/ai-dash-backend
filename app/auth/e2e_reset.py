"""
Reset DB to a clean E2E baseline.
- Restores analyst@example.com and admin@example.com to their default state.
- Removes any users created during E2E tests (invite-created users).

Run: python -m app.auth.e2e_reset
"""
import asyncio
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.auth.models import Base, User, Role
from app.auth.service import hash_password
from app.database import engine

SEED_USERS = [
    ("analyst@example.com", "Test Analyst", Role.analyst, "password123"),
    ("admin@example.com", "Test Admin", Role.admin, "adminpass123"),
]
SEED_EMAILS = {email for email, _, _, _ in SEED_USERS}


async def e2e_reset() -> None:
    # Ensure tables exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        # Remove non-seed users (created by invite during E2E)
        result = await session.execute(select(User))
        for user in result.scalars().all():
            if user.email not in SEED_EMAILS:
                await session.delete(user)
                print(f"Removed: {user.email}")

        # Restore seed users to default state
        for email, name, role, password in SEED_USERS:
            result = await session.execute(select(User).where(User.email == email))
            user = result.scalar_one_or_none()
            if user:
                user.is_active = True
                user.role = role
                user.hashed_password = hash_password(password)
                print(f"Reset: {email}")
            else:
                session.add(User(
                    email=email, name=name, role=role,
                    hashed_password=hash_password(password), is_active=True,
                ))
                print(f"Created: {email}")

        await session.commit()
    print("E2E DB reset complete.")


if __name__ == "__main__":
    asyncio.run(e2e_reset())
