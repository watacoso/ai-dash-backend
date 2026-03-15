"""
Seed live connections from ../ai-dash-frontend/.env.e2e.

Creates (or replaces) two connections owned by admin@example.com:
  - LIVE SNOWFLAKE  (type=snowflake)
  - LIVE CLAUDE     (type=claude)

Run: python -m app.connections.seed_live
     make seed-live-connections

Note: app.auth.e2e_reset clears all connections — run this script after it
if you need live connections restored.
"""
import asyncio
import os
import pathlib
from sqlalchemy import select, delete
from app.database import AsyncSessionLocal
from app.auth.models import User
from app.connections.models import Connection, ConnectionType


def _parse_env_file(path: pathlib.Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        eq = line.index("=") if "=" in line else -1
        if eq == -1:
            continue
        key = line[:eq].strip()
        value = line[eq + 1:].strip()
        # Strip surrounding single or double quotes
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        env[key] = value
    return env


async def seed_live_connections() -> None:
    env_file = pathlib.Path(__file__).resolve().parents[2] / ".." / "ai-dash-frontend" / ".env.e2e"
    env_file = env_file.resolve()

    if not env_file.exists():
        print(f"ERROR: {env_file} not found — copy .env.e2e.example and fill in credentials.")
        return

    env = _parse_env_file(env_file)

    required_sf = ["E2E_SF_ACCOUNT", "E2E_SF_USERNAME", "E2E_SF_PRIVATE_KEY"]
    required_cl = ["E2E_CL_API_KEY", "E2E_CL_MODEL"]
    missing = [k for k in required_sf + required_cl if not env.get(k)]
    if missing:
        print(f"ERROR: missing keys in .env.e2e: {', '.join(missing)}")
        return

    sf_creds = {
        "account": env["E2E_SF_ACCOUNT"],
        "username": env["E2E_SF_USERNAME"],
        "private_key": env["E2E_SF_PRIVATE_KEY"],
        "warehouse": env.get("E2E_SF_WAREHOUSE", ""),
        "database": env.get("E2E_SF_DATABASE", ""),
        "passphrase": env.get("E2E_SF_PASSPHRASE", "") or None,
    }
    cl_creds = {
        "api_key": env["E2E_CL_API_KEY"],
        "model": env["E2E_CL_MODEL"],
    }

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(User).where(User.email == "admin@example.com"))
        admin = result.scalar_one_or_none()
        if not admin:
            print("ERROR: admin@example.com not found — run `make seed` first.")
            return

        # Remove existing live connections by name to ensure clean state
        for name in ("LIVE SNOWFLAKE", "LIVE CLAUDE"):
            await session.execute(
                delete(Connection).where(Connection.name == name, Connection.owner_id == admin.id)
            )

        session.add(Connection(
            name="LIVE SNOWFLAKE",
            type=ConnectionType.snowflake,
            owner_id=admin.id,
            credentials=sf_creds,
            is_active=True,
        ))
        session.add(Connection(
            name="LIVE CLAUDE",
            type=ConnectionType.claude,
            owner_id=admin.id,
            credentials=cl_creds,
            is_active=True,
        ))
        await session.commit()

    print("Created: LIVE SNOWFLAKE (snowflake)")
    print("Created: LIVE CLAUDE (claude)")


if __name__ == "__main__":
    asyncio.run(seed_live_connections())
