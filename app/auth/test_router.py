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
async def active_user(session: AsyncSession):
    user = User(
        email="analyst@example.com",
        name="Test Analyst",
        role=Role.analyst,
        hashed_password=hash_password("correct-password"),
        is_active=True,
    )
    session.add(user)
    await session.commit()
    return user


@pytest.fixture
async def inactive_user(session: AsyncSession):
    user = User(
        email="inactive@example.com",
        name="Inactive User",
        role=Role.analyst,
        hashed_password=hash_password("correct-password"),
        is_active=False,
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
async def client(session: AsyncSession, fake_blocklist, monkeypatch):
    add_fn, is_fn = fake_blocklist
    monkeypatch.setattr("app.auth.router.add_to_blocklist", add_fn)
    monkeypatch.setattr("app.auth.router.is_blocklisted", is_fn)
    app.dependency_overrides[get_session] = lambda: session
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


class TestLogin:
    async def test_should_return_token_when_credentials_are_valid(self, client, active_user):
        # Arrange / Act
        response = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "correct-password",
        })
        # Assert
        assert response.status_code == 200
        assert "access_token" in response.json()

    async def test_should_return_401_when_password_is_wrong(self, client, active_user):
        # Arrange / Act
        response = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "wrong-password",
        })
        # Assert
        assert response.status_code == 401
        assert "access_token" not in response.json()

    async def test_should_return_401_when_email_is_unknown(self, client):
        # Arrange / Act
        response = await client.post("/auth/login", json={
            "email": "nobody@example.com",
            "password": "any-password",
        })
        # Assert
        assert response.status_code == 401

    async def test_should_return_same_message_for_wrong_password_and_unknown_email(self, client, active_user):
        # Arrange / Act
        wrong_password = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "wrong",
        })
        unknown_email = await client.post("/auth/login", json={
            "email": "nobody@example.com",
            "password": "any",
        })
        # Assert — same error message prevents user enumeration
        assert wrong_password.json()["detail"] == unknown_email.json()["detail"]

    async def test_should_return_401_when_user_is_inactive(self, client, inactive_user):
        # Arrange / Act
        response = await client.post("/auth/login", json={
            "email": "inactive@example.com",
            "password": "correct-password",
        })
        # Assert
        assert response.status_code == 401

    async def test_should_return_422_when_fields_are_missing(self, client):
        # Arrange / Act
        response = await client.post("/auth/login", json={})
        # Assert
        assert response.status_code == 422


class TestLogout:
    async def test_should_return_200_and_invalidate_token(self, client, active_user):
        # Arrange — log in first to get a token
        login = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "correct-password",
        })
        token = login.json()["access_token"]
        # Act — logout
        response = await client.post(
            "/auth/logout",
            headers={"Authorization": f"Bearer {token}"},
        )
        # Assert — logout succeeded
        assert response.status_code == 200
        # Assert — token is now rejected
        protected = await client.get(
            "/auth/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert protected.status_code == 401


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


class TestRequireRole:
    async def test_admin_token_passes_admin_guard(self, client, admin_user):
        # Arrange — log in as admin
        login = await client.post("/auth/login", json={
            "email": "admin@example.com",
            "password": "admin-password",
        })
        token = login.json()["access_token"]
        # Act
        response = await client.get(
            "/admin/users",
            headers={"Authorization": f"Bearer {token}"},
        )
        # Assert — admin is allowed through
        assert response.status_code == 200

    async def test_analyst_token_blocked_by_admin_guard(self, client, active_user):
        # Arrange — log in as analyst
        login = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "correct-password",
        })
        token = login.json()["access_token"]
        # Act
        response = await client.get(
            "/admin/users",
            headers={"Authorization": f"Bearer {token}"},
        )
        # Assert — analyst is blocked
        assert response.status_code == 403

    async def test_analyst_token_passes_analyst_guard(self, client, active_user):
        # Arrange — log in as analyst
        login = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "correct-password",
        })
        token = login.json()["access_token"]
        # Act — /auth/me is guarded only by authentication, not role,
        # so we verify the analyst token itself is valid and the role
        # value is returned correctly (no role guard blocks it)
        response = await client.get(
            "/auth/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        # Assert — analyst token works for analyst-accessible endpoints
        assert response.status_code == 200
        assert response.json()["role"] == "analyst"


class TestAdminRoutes:
    async def test_admin_can_list_users(self, client, admin_user, active_user):
        # Arrange
        login = await client.post("/auth/login", json={
            "email": "admin@example.com",
            "password": "admin-password",
        })
        token = login.json()["access_token"]
        # Act
        response = await client.get(
            "/admin/users",
            headers={"Authorization": f"Bearer {token}"},
        )
        # Assert
        assert response.status_code == 200
        users = response.json()
        emails = [u["email"] for u in users]
        assert "admin@example.com" in emails
        assert "analyst@example.com" in emails

    async def test_analyst_cannot_list_users(self, client, active_user):
        # Arrange
        login = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "correct-password",
        })
        token = login.json()["access_token"]
        # Act
        response = await client.get(
            "/admin/users",
            headers={"Authorization": f"Bearer {token}"},
        )
        # Assert
        assert response.status_code == 403

    async def test_missing_role_claim_returns_401(self, client):
        # Arrange — JWT with no role claim
        from jose import jwt
        from app.config import settings
        from datetime import datetime, timedelta, timezone
        token = jwt.encode(
            {"sub": "abc-123", "exp": datetime.now(timezone.utc) + timedelta(hours=1)},
            settings.jwt_secret,
            algorithm=settings.jwt_algorithm,
        )
        # Act
        response = await client.get(
            "/admin/users",
            headers={"Authorization": f"Bearer {token}"},
        )
        # Assert — missing role claim treated as unauthenticated
        assert response.status_code == 401


class TestJWTMiddleware:
    async def test_should_allow_request_with_valid_token(self, client, active_user):
        # Arrange
        login = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "correct-password",
        })
        token = login.json()["access_token"]
        # Act
        response = await client.get(
            "/auth/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        # Assert
        assert response.status_code == 200

    async def test_should_reject_request_with_no_token(self, client):
        # Arrange / Act
        response = await client.get("/auth/me")
        # Assert
        assert response.status_code == 401

    async def test_should_reject_request_with_expired_token(self, client):
        # Arrange
        from datetime import datetime, timedelta
        from jose import jwt
        from app.config import settings
        expired_token = jwt.encode(
            {"sub": "abc-123", "role": "analyst", "exp": datetime.utcnow() - timedelta(seconds=1)},
            settings.jwt_secret,
            algorithm=settings.jwt_algorithm,
        )
        # Act
        response = await client.get(
            "/auth/me",
            headers={"Authorization": f"Bearer {expired_token}"},
        )
        # Assert
        assert response.status_code == 401

    async def test_should_reject_request_with_tampered_token(self, client, active_user):
        # Arrange — generate token directly to avoid cookie being set on the client
        from app.auth.service import create_token
        token = create_token(str(active_user.id), active_user.role.value)
        tampered = token[:-4] + "xxxx"
        # Act
        response = await client.get(
            "/auth/me",
            headers={"Authorization": f"Bearer {tampered}"},
        )
        # Assert
        assert response.status_code == 401


class TestCookieAuth:
    async def test_login_sets_httponly_cookie(self, client, active_user):
        response = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "correct-password",
        })
        assert response.status_code == 200
        cookie = response.headers.get("set-cookie", "")
        assert "access_token" in cookie
        assert "HttpOnly" in cookie
        assert "samesite=strict" in cookie.lower()

    async def test_login_still_returns_token_in_body(self, client, active_user):
        # Backwards compat: body still contains access_token
        response = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "correct-password",
        })
        assert response.status_code == 200
        assert "access_token" in response.json()

    async def test_logout_clears_cookie(self, client, active_user):
        # Login first
        login = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "correct-password",
        })
        token = login.json()["access_token"]
        # Logout via bearer
        response = await client.post(
            "/auth/logout",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 200
        cookie = response.headers.get("set-cookie", "")
        assert "access_token" in cookie
        # Cookie should be expired/cleared (Max-Age=0 or expires in the past)
        assert "max-age=0" in cookie.lower() or "expires=" in cookie.lower()

    async def test_me_works_with_cookie(self, client, active_user):
        # Login to get cookie
        login = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "correct-password",
        })
        token = login.json()["access_token"]
        # Call /auth/me using cookie (no Authorization header)
        response = await client.get(
            "/auth/me",
            cookies={"access_token": token},
        )
        assert response.status_code == 200
        assert response.json()["email"] == "analyst@example.com"

    async def test_me_falls_back_to_bearer(self, client, active_user):
        # Bearer still works (no cookie)
        login = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "correct-password",
        })
        token = login.json()["access_token"]
        response = await client.get(
            "/auth/me",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 200

    async def test_me_returns_401_with_neither_cookie_nor_bearer(self, client):
        response = await client.get("/auth/me")
        assert response.status_code == 401
