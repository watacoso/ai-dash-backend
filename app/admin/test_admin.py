import hashlib
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from app.auth.models import Base, Role, User
from app.auth.service import hash_password
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
        hashed_password=hash_password("admin-password"),
        is_active=True,
    )
    session.add(user)
    await session.commit()
    return user


@pytest.fixture
async def analyst_user(session: AsyncSession):
    user = User(
        email="analyst@example.com",
        name="Test Analyst",
        role=Role.analyst,
        hashed_password=hash_password("analyst-password"),
        is_active=True,
    )
    session.add(user)
    await session.commit()
    return user


@pytest.fixture
def fake_blocklist():
    store: set[str] = set()

    async def fake_add(token: str) -> None:
        store.add(token)

    async def fake_is(token: str) -> bool:
        return token in store

    return fake_add, fake_is


@pytest.fixture
def fake_invite_store():
    """Replaces Redis used_invites set."""
    used: set[str] = set()

    async def fake_mark_used(token: str) -> None:
        used.add(hashlib.sha256(token.encode()).hexdigest())

    async def fake_is_used(token: str) -> bool:
        return hashlib.sha256(token.encode()).hexdigest() in used

    return fake_mark_used, fake_is_used


@pytest.fixture
async def client(session: AsyncSession, fake_blocklist, fake_invite_store, monkeypatch):
    add_fn, is_fn = fake_blocklist
    mark_used, is_used = fake_invite_store
    monkeypatch.setattr("app.auth.router.add_to_blocklist", add_fn)
    monkeypatch.setattr("app.auth.router.is_blocklisted", is_fn)
    monkeypatch.setattr("app.admin.router.mark_invite_used", mark_used)
    monkeypatch.setattr("app.admin.router.is_invite_used", is_used)
    app.dependency_overrides[get_session] = lambda: session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


async def _login(client, email: str, password: str) -> str:
    r = await client.post("/auth/login", json={"email": email, "password": password})
    return r.json()["access_token"]


# ── Invite token unit tests ────────────────────────────────────────────────────

class TestInviteToken:
    def test_create_invite_token_contains_role_and_purpose(self):
        from app.admin.service import create_invite_token, decode_invite_token
        token = create_invite_token("analyst")
        payload = decode_invite_token(token)
        assert payload["role"] == "analyst"
        assert payload["purpose"] == "invite"

    def test_decode_invite_token_rejects_expired_token(self):
        from app.admin.service import decode_invite_token
        from datetime import datetime, timedelta, timezone
        from jose import jwt
        from app.config import settings
        expired = jwt.encode(
            {"purpose": "invite", "role": "analyst",
             "exp": datetime.now(timezone.utc) - timedelta(seconds=1)},
            settings.jwt_secret,
            algorithm=settings.jwt_algorithm,
        )
        with pytest.raises(Exception):
            decode_invite_token(expired)

    def test_decode_invite_token_rejects_wrong_purpose(self):
        from app.admin.service import decode_invite_token
        from app.auth.service import create_token
        # regular access token has purpose=None (no purpose field)
        access_token = create_token("some-id", "analyst")
        with pytest.raises(Exception):
            decode_invite_token(access_token)


# ── POST /admin/invite ─────────────────────────────────────────────────────────

class TestPostInvite:
    async def test_admin_can_generate_invite(self, client, admin_user):
        token = await _login(client, "admin@example.com", "admin-password")
        response = await client.post(
            "/admin/invite",
            json={"role": "analyst"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 201
        assert "invite_url" in response.json()

    async def test_analyst_cannot_generate_invite(self, client, analyst_user):
        token = await _login(client, "analyst@example.com", "analyst-password")
        response = await client.post(
            "/admin/invite",
            json={"role": "analyst"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 403


# ── POST /admin/users/accept-invite ───────────────────────────────────────────

class TestAcceptInvite:
    async def test_valid_invite_creates_user(self, client, admin_user):
        admin_token = await _login(client, "admin@example.com", "admin-password")
        # Generate invite
        invite_resp = await client.post(
            "/admin/invite",
            json={"role": "analyst"},
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        invite_url = invite_resp.json()["invite_url"]
        invite_token = invite_url.split("token=")[1]
        # Accept invite
        response = await client.post(
            "/admin/users/accept-invite",
            json={"token": invite_token, "email": "new@example.com",
                  "name": "New User", "password": "newpassword123"},
        )
        assert response.status_code == 201
        # New user can log in
        login_resp = await client.post(
            "/auth/login",
            json={"email": "new@example.com", "password": "newpassword123"},
        )
        assert login_resp.status_code == 200

    async def test_reused_invite_returns_409(self, client, admin_user):
        admin_token = await _login(client, "admin@example.com", "admin-password")
        invite_resp = await client.post(
            "/admin/invite",
            json={"role": "analyst"},
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        invite_token = invite_resp.json()["invite_url"].split("token=")[1]
        # Use it once
        await client.post(
            "/admin/users/accept-invite",
            json={"token": invite_token, "email": "first@example.com",
                  "name": "First", "password": "pass1234"},
        )
        # Use it again
        response = await client.post(
            "/admin/users/accept-invite",
            json={"token": invite_token, "email": "second@example.com",
                  "name": "Second", "password": "pass1234"},
        )
        assert response.status_code == 409

    async def test_expired_invite_returns_410(self, client):
        from datetime import datetime, timedelta, timezone
        from jose import jwt
        from app.config import settings
        expired = jwt.encode(
            {"purpose": "invite", "role": "analyst",
             "exp": datetime.now(timezone.utc) - timedelta(seconds=1)},
            settings.jwt_secret,
            algorithm=settings.jwt_algorithm,
        )
        response = await client.post(
            "/admin/users/accept-invite",
            json={"token": expired, "email": "late@example.com",
                  "name": "Late", "password": "pass1234"},
        )
        assert response.status_code == 410


# ── PATCH /admin/users/{id}/role ──────────────────────────────────────────────

class TestPatchRole:
    async def test_admin_can_change_user_role(self, client, admin_user, analyst_user):
        token = await _login(client, "admin@example.com", "admin-password")
        response = await client.patch(
            f"/admin/users/{analyst_user.id}/role",
            json={"role": "admin"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 200
        assert response.json()["role"] == "admin"

    async def test_analyst_cannot_change_role(self, client, admin_user, analyst_user):
        token = await _login(client, "analyst@example.com", "analyst-password")
        response = await client.patch(
            f"/admin/users/{admin_user.id}/role",
            json={"role": "analyst"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 403


# ── PATCH /admin/users/{id}/active ────────────────────────────────────────────

class TestPatchActive:
    async def test_admin_can_deactivate_user(self, client, admin_user, analyst_user):
        token = await _login(client, "admin@example.com", "admin-password")
        response = await client.patch(
            f"/admin/users/{analyst_user.id}/active",
            json={"is_active": False},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 200
        assert response.json()["is_active"] is False

    async def test_admin_cannot_deactivate_themselves(self, client, admin_user):
        token = await _login(client, "admin@example.com", "admin-password")
        response = await client.patch(
            f"/admin/users/{admin_user.id}/active",
            json={"is_active": False},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 400

    async def test_analyst_cannot_deactivate_user(self, client, admin_user, analyst_user):
        token = await _login(client, "analyst@example.com", "analyst-password")
        response = await client.patch(
            f"/admin/users/{admin_user.id}/active",
            json={"is_active": False},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 403
