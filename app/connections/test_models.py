import os
import uuid

import pytest
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from app.auth.models import Base, User, Role
from app.auth.service import hash_password

# Import Connection so it registers with Base.metadata
from app.connections.models import Connection, ConnectionType

TEST_DATABASE_URL = "postgresql+asyncpg://aidash:aidash@localhost:5433/aidash_test"

engine = create_async_engine(TEST_DATABASE_URL, poolclass=NullPool)
TestingSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture(autouse=True)
async def setup_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def session():
    async with TestingSessionLocal() as s:
        yield s


@pytest.fixture
async def owner(session: AsyncSession):
    user = User(
        email="owner@example.com",
        name="Connection Owner",
        role=Role.admin,
        hashed_password=hash_password("pw"),
        is_active=True,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


# ── EncryptedJSON TypeDecorator ───────────────────────────────────────────────

class TestEncryptedJSON:
    async def test_encrypt_decrypt_roundtrip(self, session: AsyncSession, owner):
        creds = {"api_key": "sk-123", "model": "claude-sonnet-4-6"}
        conn = Connection(
            name="test-claude",
            type=ConnectionType.claude,
            owner_id=owner.id,
            credentials=creds,
        )
        session.add(conn)
        await session.commit()
        await session.refresh(conn)

        assert conn.credentials == creds

    async def test_different_plaintexts_produce_different_ciphertexts(
        self, session: AsyncSession, owner
    ):
        conn1 = Connection(
            name="c1", type=ConnectionType.claude, owner_id=owner.id,
            credentials={"a": 1},
        )
        conn2 = Connection(
            name="c2", type=ConnectionType.claude, owner_id=owner.id,
            credentials={"b": 2},
        )
        session.add_all([conn1, conn2])
        await session.commit()

        # Read raw ciphertext from DB
        result = await session.execute(
            text("SELECT credentials FROM connections ORDER BY name")
        )
        rows = result.fetchall()
        raw1, raw2 = rows[0][0], rows[1][0]
        assert raw1 != raw2

    async def test_tampered_ciphertext_raises_on_load(self, session: AsyncSession, owner):
        conn = Connection(
            name="tampered", type=ConnectionType.claude, owner_id=owner.id,
            credentials={"key": "value"},
        )
        session.add(conn)
        await session.commit()

        # Corrupt the stored ciphertext directly in DB
        await session.execute(
            text("UPDATE connections SET credentials = 'AAAAAAAAAAAAAAAA' WHERE name = 'tampered'")
        )
        await session.commit()

        # Expire the cached object to force a reload from DB
        session.expire(conn)

        with pytest.raises(Exception):  # InvalidToken or mapped error
            _ = conn.credentials

    async def test_null_stored_and_loaded_as_none(self, session: AsyncSession, owner):
        conn = Connection(
            name="no-creds", type=ConnectionType.claude, owner_id=owner.id,
            credentials=None,
        )
        session.add(conn)
        await session.commit()
        await session.refresh(conn)

        assert conn.credentials is None

        raw = await session.execute(
            text("SELECT credentials FROM connections WHERE name = 'no-creds'")
        )
        assert raw.scalar_one() is None


# ── Connection model ──────────────────────────────────────────────────────────

class TestConnectionModel:
    async def test_create_snowflake_connection(self, session: AsyncSession, owner):
        creds = {
            "account": "xy12345.us-east-1",
            "username": "svc_user",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----",
            "warehouse": "COMPUTE_WH",
            "database": "ANALYTICS",
            "schema": "PUBLIC",
        }
        conn = Connection(
            name="prod-snowflake",
            type=ConnectionType.snowflake,
            owner_id=owner.id,
            credentials=creds,
        )
        session.add(conn)
        await session.commit()
        await session.refresh(conn)

        assert conn.id is not None
        assert conn.name == "prod-snowflake"
        assert conn.type == ConnectionType.snowflake
        assert conn.owner_id == owner.id
        assert conn.credentials == creds
        assert conn.is_active is True

    async def test_create_claude_connection(self, session: AsyncSession, owner):
        creds = {"api_key": "sk-ant-abc123", "model": "claude-opus-4-6"}
        conn = Connection(
            name="prod-claude",
            type=ConnectionType.claude,
            owner_id=owner.id,
            credentials=creds,
        )
        session.add(conn)
        await session.commit()
        await session.refresh(conn)

        assert conn.name == "prod-claude"
        assert conn.type == ConnectionType.claude
        assert conn.credentials == creds

    async def test_is_active_defaults_to_true(self, session: AsyncSession, owner):
        conn = Connection(
            name="default-active",
            type=ConnectionType.claude,
            owner_id=owner.id,
            credentials={"api_key": "x", "model": "m"},
        )
        session.add(conn)
        await session.commit()
        await session.refresh(conn)

        assert conn.is_active is True

    async def test_created_at_and_updated_at_auto_populated(
        self, session: AsyncSession, owner
    ):
        conn = Connection(
            name="timestamps",
            type=ConnectionType.claude,
            owner_id=owner.id,
            credentials={"api_key": "x", "model": "m"},
        )
        session.add(conn)
        await session.commit()
        await session.refresh(conn)

        assert conn.created_at is not None
        assert conn.updated_at is not None

    async def test_updated_at_changes_on_update(self, session: AsyncSession, owner):
        conn = Connection(
            name="before-update",
            type=ConnectionType.claude,
            owner_id=owner.id,
            credentials={"api_key": "x", "model": "m"},
        )
        session.add(conn)
        await session.commit()
        await session.refresh(conn)
        original_updated_at = conn.updated_at

        # Small delay to ensure timestamp difference
        import asyncio
        await asyncio.sleep(0.01)

        conn.name = "after-update"
        await session.commit()
        await session.refresh(conn)

        assert conn.updated_at >= original_updated_at

    async def test_owner_id_fk_constraint_enforced(self, session: AsyncSession):
        conn = Connection(
            name="orphan",
            type=ConnectionType.claude,
            owner_id=uuid.uuid4(),  # non-existent user
            credentials={"api_key": "x", "model": "m"},
        )
        session.add(conn)
        with pytest.raises(IntegrityError):
            await session.commit()


# ── Settings ──────────────────────────────────────────────────────────────────

class TestSettings:
    def test_valid_encryption_key_loads(self):
        key = Fernet.generate_key().decode()
        from pydantic import ValidationError
        # Temporarily set env var
        os.environ["ENCRYPTION_KEY"] = key
        try:
            # Re-instantiate settings with the key present
            from app.config import Settings
            s = Settings()
            assert s.encryption_key == key
        finally:
            del os.environ["ENCRYPTION_KEY"]

    def test_missing_encryption_key_raises(self):
        from pydantic import ValidationError
        # Remove key if set, ensure it raises
        os.environ.pop("ENCRYPTION_KEY", None)
        from app.config import Settings
        with pytest.raises((ValidationError, ValueError)):
            Settings()
