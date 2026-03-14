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
        # Arrange
        login = await client.post("/auth/login", json={
            "email": "analyst@example.com",
            "password": "correct-password",
        })
        token = login.json()["access_token"]
        tampered = token[:-4] + "xxxx"
        # Act
        response = await client.get(
            "/auth/me",
            headers={"Authorization": f"Bearer {tampered}"},
        )
        # Assert
        assert response.status_code == 401
