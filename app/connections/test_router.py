import uuid

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


SNOWFLAKE_CREDS = {
    "account": "xy12345.us-east-1",
    "username": "svc_user",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----",
    "warehouse": "COMPUTE_WH",
    "database": "ANALYTICS",
    "schema": "PUBLIC",
}

CLAUDE_CREDS = {
    "api_key": "sk-ant-abc123",
    "model": "claude-sonnet-4-6",
}


# ── POST /connections ──────────────────────────────────────────────────────────

class TestCreateConnection:
    async def test_admin_creates_snowflake_connection(self, client, admin_token):
        res = await client.post(
            "/connections",
            json={"name": "prod-sf", "type": "snowflake", "credentials": SNOWFLAKE_CREDS},
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 201
        data = res.json()
        assert data["name"] == "prod-sf"
        assert data["type"] == "snowflake"
        assert data["is_active"] is True
        assert "id" in data
        assert "credentials" not in data

    async def test_admin_creates_claude_connection(self, client, admin_token):
        res = await client.post(
            "/connections",
            json={"name": "prod-claude", "type": "claude", "credentials": CLAUDE_CREDS},
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 201
        data = res.json()
        assert data["name"] == "prod-claude"
        assert data["type"] == "claude"

    async def test_missing_snowflake_field_returns_422(self, client, admin_token):
        bad_creds = {k: v for k, v in SNOWFLAKE_CREDS.items() if k != "warehouse"}
        res = await client.post(
            "/connections",
            json={"name": "bad-sf", "type": "snowflake", "credentials": bad_creds},
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 422

    async def test_missing_claude_field_returns_422(self, client, admin_token):
        bad_creds = {"api_key": "sk-ant-abc123"}  # missing model
        res = await client.post(
            "/connections",
            json={"name": "bad-claude", "type": "claude", "credentials": bad_creds},
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 422

    async def test_analyst_cannot_create(self, client, analyst_token):
        res = await client.post(
            "/connections",
            json={"name": "x", "type": "claude", "credentials": CLAUDE_CREDS},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 403

    async def test_unauthenticated_returns_401(self, client):
        res = await client.post(
            "/connections",
            json={"name": "x", "type": "claude", "credentials": CLAUDE_CREDS},
        )
        assert res.status_code == 401


# ── GET /connections ───────────────────────────────────────────────────────────

class TestListConnections:
    async def _create(self, client, token, name, type_, creds):
        await client.post(
            "/connections",
            json={"name": name, "type": type_, "credentials": creds},
            cookies={"access_token": token},
        )

    async def test_admin_lists_connections(self, client, admin_token):
        await self._create(client, admin_token, "sf1", "snowflake", SNOWFLAKE_CREDS)
        await self._create(client, admin_token, "cl1", "claude", CLAUDE_CREDS)

        res = await client.get("/connections", cookies={"access_token": admin_token})
        assert res.status_code == 200
        data = res.json()
        assert len(data) == 2
        assert all("credentials" not in row for row in data)

    async def test_analyst_can_list(self, client, admin_token, analyst_token):
        await self._create(client, admin_token, "sf1", "snowflake", SNOWFLAKE_CREDS)

        res = await client.get("/connections", cookies={"access_token": analyst_token})
        assert res.status_code == 200
        assert len(res.json()) == 1

    async def test_soft_deleted_excluded(self, client, admin_token):
        await self._create(client, admin_token, "active", "claude", CLAUDE_CREDS)
        await self._create(client, admin_token, "inactive", "claude", CLAUDE_CREDS)

        # Soft-delete 'inactive'
        list_res = await client.get("/connections", cookies={"access_token": admin_token})
        inactive_id = next(r["id"] for r in list_res.json() if r["name"] == "inactive")
        await client.delete(f"/connections/{inactive_id}", cookies={"access_token": admin_token})

        res = await client.get("/connections", cookies={"access_token": admin_token})
        names = [r["name"] for r in res.json()]
        assert "active" in names
        assert "inactive" not in names

    async def test_unauthenticated_returns_401(self, client):
        res = await client.get("/connections")
        assert res.status_code == 401


# ── GET /connections/{id} ──────────────────────────────────────────────────────

class TestGetConnection:
    async def test_admin_fetches_connection(self, client, admin_token):
        create_res = await client.post(
            "/connections",
            json={"name": "sf1", "type": "snowflake", "credentials": SNOWFLAKE_CREDS},
            cookies={"access_token": admin_token},
        )
        conn_id = create_res.json()["id"]

        res = await client.get(f"/connections/{conn_id}", cookies={"access_token": admin_token})
        assert res.status_code == 200
        assert res.json()["name"] == "sf1"
        assert "credentials" not in res.json()

    async def test_unknown_id_returns_404(self, client, admin_token):
        res = await client.get(
            f"/connections/{uuid.uuid4()}", cookies={"access_token": admin_token}
        )
        assert res.status_code == 404

    async def test_analyst_cannot_get_single(self, client, admin_token, analyst_token):
        create_res = await client.post(
            "/connections",
            json={"name": "sf1", "type": "snowflake", "credentials": SNOWFLAKE_CREDS},
            cookies={"access_token": admin_token},
        )
        conn_id = create_res.json()["id"]

        res = await client.get(
            f"/connections/{conn_id}", cookies={"access_token": analyst_token}
        )
        assert res.status_code == 403


# ── PATCH /connections/{id} ────────────────────────────────────────────────────

class TestUpdateConnection:
    async def _create_claude(self, client, token, name="cl1"):
        res = await client.post(
            "/connections",
            json={"name": name, "type": "claude", "credentials": CLAUDE_CREDS},
            cookies={"access_token": token},
        )
        return res.json()["id"]

    async def test_admin_updates_name(self, client, admin_token):
        conn_id = await self._create_claude(client, admin_token)
        res = await client.patch(
            f"/connections/{conn_id}",
            json={"name": "renamed"},
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 200
        assert res.json()["name"] == "renamed"

    async def test_admin_updates_credentials(self, client, admin_token, session):
        conn_id = await self._create_claude(client, admin_token)
        new_creds = {"api_key": "sk-ant-newkey", "model": "claude-opus-4-6"}
        res = await client.patch(
            f"/connections/{conn_id}",
            json={"credentials": new_creds},
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 200

        # Verify the stored creds decrypt correctly
        conn = await session.get(Connection, uuid.UUID(conn_id))
        await session.refresh(conn)
        assert conn.credentials == new_creds

    async def test_admin_deactivates_connection(self, client, admin_token):
        conn_id = await self._create_claude(client, admin_token)
        res = await client.patch(
            f"/connections/{conn_id}",
            json={"is_active": False},
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 200
        assert res.json()["is_active"] is False

    async def test_unknown_id_returns_404(self, client, admin_token):
        res = await client.patch(
            f"/connections/{uuid.uuid4()}",
            json={"name": "x"},
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 404

    async def test_analyst_cannot_update(self, client, admin_token, analyst_token):
        conn_id = await self._create_claude(client, admin_token)
        res = await client.patch(
            f"/connections/{conn_id}",
            json={"name": "hack"},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 403


# ── DELETE /connections/{id} ───────────────────────────────────────────────────

class TestDeleteConnection:
    async def test_admin_deletes_connection(self, client, admin_token):
        create_res = await client.post(
            "/connections",
            json={"name": "to-delete", "type": "claude", "credentials": CLAUDE_CREDS},
            cookies={"access_token": admin_token},
        )
        conn_id = create_res.json()["id"]

        res = await client.delete(
            f"/connections/{conn_id}", cookies={"access_token": admin_token}
        )
        assert res.status_code in (200, 204)

    async def test_deleted_connection_absent_from_list(self, client, admin_token):
        create_res = await client.post(
            "/connections",
            json={"name": "bye", "type": "claude", "credentials": CLAUDE_CREDS},
            cookies={"access_token": admin_token},
        )
        conn_id = create_res.json()["id"]
        await client.delete(f"/connections/{conn_id}", cookies={"access_token": admin_token})

        list_res = await client.get("/connections", cookies={"access_token": admin_token})
        ids = [r["id"] for r in list_res.json()]
        assert conn_id not in ids

    async def test_unknown_id_returns_404(self, client, admin_token):
        res = await client.delete(
            f"/connections/{uuid.uuid4()}", cookies={"access_token": admin_token}
        )
        assert res.status_code == 404

    async def test_analyst_cannot_delete(self, client, admin_token, analyst_token):
        create_res = await client.post(
            "/connections",
            json={"name": "protected", "type": "claude", "credentials": CLAUDE_CREDS},
            cookies={"access_token": admin_token},
        )
        conn_id = create_res.json()["id"]

        res = await client.delete(
            f"/connections/{conn_id}", cookies={"access_token": analyst_token}
        )
        assert res.status_code == 403


# ── Integration: round-trips ───────────────────────────────────────────────────

class TestIntegration:
    async def test_create_then_fetch_roundtrip(self, client, admin_token):
        create_res = await client.post(
            "/connections",
            json={"name": "roundtrip", "type": "snowflake", "credentials": SNOWFLAKE_CREDS},
            cookies={"access_token": admin_token},
        )
        conn_id = create_res.json()["id"]

        get_res = await client.get(
            f"/connections/{conn_id}", cookies={"access_token": admin_token}
        )
        assert get_res.json()["name"] == "roundtrip"
        assert get_res.json()["type"] == "snowflake"

    async def test_create_then_list_shows_connection(self, client, admin_token):
        await client.post(
            "/connections",
            json={"name": "listed", "type": "claude", "credentials": CLAUDE_CREDS},
            cookies={"access_token": admin_token},
        )
        list_res = await client.get("/connections", cookies={"access_token": admin_token})
        names = [r["name"] for r in list_res.json()]
        assert "listed" in names

    async def test_update_credentials_decrypts_correctly(self, client, admin_token, session):
        create_res = await client.post(
            "/connections",
            json={"name": "creds-test", "type": "claude", "credentials": CLAUDE_CREDS},
            cookies={"access_token": admin_token},
        )
        conn_id = create_res.json()["id"]
        new_creds = {"api_key": "sk-ant-updated", "model": "claude-haiku-4-5"}

        await client.patch(
            f"/connections/{conn_id}",
            json={"credentials": new_creds},
            cookies={"access_token": admin_token},
        )

        conn = await session.get(Connection, uuid.UUID(conn_id))
        await session.refresh(conn)
        assert conn.credentials == new_creds

    async def test_delete_then_list_excludes(self, client, admin_token):
        create_res = await client.post(
            "/connections",
            json={"name": "gone", "type": "claude", "credentials": CLAUDE_CREDS},
            cookies={"access_token": admin_token},
        )
        conn_id = create_res.json()["id"]
        await client.delete(f"/connections/{conn_id}", cookies={"access_token": admin_token})

        list_res = await client.get("/connections", cookies={"access_token": admin_token})
        assert all(r["id"] != conn_id for r in list_res.json())
