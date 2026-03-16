"""
Reset DB and Redis to a clean E2E baseline.
- Restores analyst@example.com and admin@example.com to their default state.
- Removes any users created during E2E tests (invite-created users).
- Flushes Redis used_invite keys.

Run: python -m app.auth.e2e_reset
"""
import asyncio
from sqlalchemy import select, delete
from app.database import AsyncSessionLocal
from app.auth.models import Base, User, Role
from app.auth.service import hash_password
from app.charts.models import Chart
from app.connections.models import Connection
from app.datasets.models import Dataset
from app.database import engine
from app.redis import get_redis

SEED_USERS = [
    ("analyst@example.com", "Test Analyst", Role.analyst, "password123"),
    ("admin@example.com", "Test Admin", Role.admin, "adminpass123"),
]
SEED_EMAILS = {email for email, _, _, _ in SEED_USERS}


async def e2e_reset() -> None:
    # Ensure tables exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Flush Redis invite keys (and blocklist) so invite tests are repeatable
    r = get_redis()
    invite_keys = await r.keys("used_invite:*")
    blocklist_keys = await r.keys("blocklist:*")
    keys_to_del = invite_keys + blocklist_keys
    if keys_to_del:
        await r.delete(*keys_to_del)
        print(f"Redis: cleared {len(keys_to_del)} key(s)")

    async with AsyncSessionLocal() as session:
        # Remove charts first (FK refs datasets)
        result = await session.execute(delete(Chart))
        print(f"Charts: cleared {result.rowcount} row(s)")

        # Remove datasets (FK refs connections)
        result = await session.execute(delete(Dataset))
        print(f"Datasets: cleared {result.rowcount} row(s)")

        # Remove all connections (none should persist between E2E runs)
        result = await session.execute(delete(Connection))
        print(f"Connections: cleared {result.rowcount} row(s)")

        # Remove non-seed users
        result = await session.execute(select(User))
        for user in result.scalars().all():
            if user.email not in SEED_EMAILS:
                await session.delete(user)
                print(f"Removed: {user.email}")

        # Restore seed users
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
