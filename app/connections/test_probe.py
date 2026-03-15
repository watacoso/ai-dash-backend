import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from app.auth.models import Base, Role, User
from app.auth.service import hash_password, create_token
from app.connections.models import Connection, ConnectionType
from app.main import app
from app.database import get_session

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
async def admin_user(session: AsyncSession):
    user = User(
        email="admin@example.com",
        name="Test Admin",
        role=Role.admin,
        hashed_password=hash_password("pw"),
        is_active=True,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


@pytest.fixture
async def analyst_user(session: AsyncSession):
    user = User(
        email="analyst@example.com",
        name="Test Analyst",
        role=Role.analyst,
        hashed_password=hash_password("pw"),
        is_active=True,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


@pytest.fixture
def admin_token(admin_user):
    return create_token(str(admin_user.id), admin_user.role.value)


@pytest.fixture
def analyst_token(analyst_user):
    return create_token(str(analyst_user.id), analyst_user.role.value)


@pytest.fixture
def fake_blocklist():
    store: set[str] = set()

    async def fake_add(token: str) -> None:
        store.add(token)

    async def fake_is(token: str) -> bool:
        return token in store

    return fake_add, fake_is


@pytest.fixture
async def client(session: AsyncSession, fake_blocklist, monkeypatch):
    _, is_fn = fake_blocklist
    monkeypatch.setattr("app.auth.router.is_blocklisted", is_fn)
    app.dependency_overrides[get_session] = lambda: session
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
async def snowflake_conn(session: AsyncSession, admin_user):
    conn = Connection(
        name="test-sf",
        type=ConnectionType.snowflake,
        owner_id=admin_user.id,
        credentials={
            "account": "xy12345.us-east-1",
            "username": "svc_user",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----",
            "warehouse": "COMPUTE_WH",
            "database": "ANALYTICS",
            "schema": "PUBLIC",
        },
    )
    session.add(conn)
    await session.commit()
    await session.refresh(conn)
    return conn


@pytest.fixture
async def claude_conn(session: AsyncSession, admin_user):
    conn = Connection(
        name="test-claude",
        type=ConnectionType.claude,
        owner_id=admin_user.id,
        credentials={"api_key": "sk-ant-abc123", "model": "claude-sonnet-4-6"},
    )
    session.add(conn)
    await session.commit()
    await session.refresh(conn)
    return conn


# ── Snowflake probe ────────────────────────────────────────────────────────────

class TestSnowflakeProbe:
    async def test_successful_connection_returns_ok(
        self, client, admin_token, snowflake_conn
    ):
        mock_cursor = MagicMock()
        mock_cursor.execute.return_value = None
        mock_sf_conn = MagicMock()
        mock_sf_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_sf_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_sf_conn.close.return_value = None

        with patch("app.connections.probe._load_private_key_bytes", return_value=b"fake-key"), \
             patch("app.connections.probe.snowflake.connector.connect", return_value=mock_sf_conn):
            res = await client.post(
                f"/connections/{snowflake_conn.id}/test",
                cookies={"access_token": admin_token},
            )

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is True
        assert "latency_ms" in data
        assert isinstance(data["latency_ms"], int)

    async def test_auth_failure_returns_ok_false(
        self, client, admin_token, snowflake_conn
    ):
        import snowflake.connector
        with patch(
            "app.connections.probe.snowflake.connector.connect",
            side_effect=snowflake.connector.errors.DatabaseError("Incorrect username or password"),
        ):
            res = await client.post(
                f"/connections/{snowflake_conn.id}/test",
                cookies={"access_token": admin_token},
            )

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is False
        assert "error" in data

    async def test_timeout_returns_ok_false(
        self, client, admin_token, snowflake_conn
    ):
        import asyncio

        async def slow_connect(*args, **kwargs):
            await asyncio.sleep(20)

        with patch("app.connections.router.run_snowflake_probe", side_effect=asyncio.TimeoutError):
            res = await client.post(
                f"/connections/{snowflake_conn.id}/test",
                cookies={"access_token": admin_token},
            )

        assert res.status_code == 200
        assert res.json() == {"ok": False, "error": "timeout"}


# ── Claude probe ───────────────────────────────────────────────────────────────

class TestClaudeProbe:
    async def test_successful_connection_returns_ok(
        self, client, admin_token, claude_conn
    ):
        mock_response = MagicMock()
        mock_client = MagicMock()
        mock_client.messages.create.return_value = mock_response

        with patch("app.connections.probe.anthropic.Anthropic", return_value=mock_client):
            res = await client.post(
                f"/connections/{claude_conn.id}/test",
                cookies={"access_token": admin_token},
            )

        assert res.status_code == 200
        assert res.json()["ok"] is True

    async def test_auth_failure_returns_ok_false(
        self, client, admin_token, claude_conn
    ):
        import anthropic
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = anthropic.AuthenticationError(
            message="Invalid API key",
            response=MagicMock(status_code=401, headers={}),
            body={},
        )

        with patch("app.connections.probe.anthropic.Anthropic", return_value=mock_client):
            res = await client.post(
                f"/connections/{claude_conn.id}/test",
                cookies={"access_token": admin_token},
            )

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is False
        assert "error" in data

    async def test_bad_model_returns_ok_false(
        self, client, admin_token, claude_conn
    ):
        import anthropic
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = anthropic.NotFoundError(
            message="Model not found",
            response=MagicMock(status_code=404, headers={}),
            body={},
        )

        with patch("app.connections.probe.anthropic.Anthropic", return_value=mock_client):
            res = await client.post(
                f"/connections/{claude_conn.id}/test",
                cookies={"access_token": admin_token},
            )

        assert res.status_code == 200
        data = res.json()
        assert data["ok"] is False
        assert "error" in data

    async def test_timeout_returns_ok_false(
        self, client, admin_token, claude_conn
    ):
        import asyncio

        with patch("app.connections.router.run_claude_probe", side_effect=asyncio.TimeoutError):
            res = await client.post(
                f"/connections/{claude_conn.id}/test",
                cookies={"access_token": admin_token},
            )

        assert res.status_code == 200
        assert res.json() == {"ok": False, "error": "timeout"}


# ── Access control ─────────────────────────────────────────────────────────────

class TestProbeAccessControl:
    async def test_unknown_id_returns_404(self, client, admin_token):
        res = await client.post(
            f"/connections/{uuid.uuid4()}/test",
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 404

    async def test_analyst_returns_403(self, client, analyst_token, claude_conn):
        res = await client.post(
            f"/connections/{claude_conn.id}/test",
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 403

    async def test_unauthenticated_returns_401(self, client, claude_conn):
        res = await client.post(f"/connections/{claude_conn.id}/test")
        assert res.status_code == 401
