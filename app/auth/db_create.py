"""
Create all database tables (idempotent).
Run: python -m app.auth.db_create
"""
import asyncio
from app.auth.models import Base
from app.connections.models import Connection  # registers with Base.metadata  # noqa: F401
from app.database import engine


async def db_create() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Tables created (or already exist).")


if __name__ == "__main__":
    asyncio.run(db_create())
