"""Shared fixtures for app/charts tests."""
import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from app.auth.models import Base, Role, User
from app.auth.service import hash_password, create_token
from app.connections.models import Connection, ConnectionType
from app.datasets.models import Dataset
from app.charts.models import Chart
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
def admin_token(admin_user):
    return create_token(str(admin_user.id), admin_user.role.value)


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
async def sf_connection(session: AsyncSession, admin_user):
    conn = Connection(
        name="test-sf",
        type=ConnectionType.snowflake,
        owner_id=admin_user.id,
        credentials={"account": "xy", "username": "u", "private_key": "k",
                     "warehouse": "WH", "database": "DB", "passphrase": None},
        is_active=True,
    )
    session.add(conn)
    await session.commit()
    await session.refresh(conn)
    return conn


@pytest.fixture
async def cl_connection(session: AsyncSession, admin_user):
    conn = Connection(
        name="test-claude",
        type=ConnectionType.claude,
        owner_id=admin_user.id,
        credentials={"api_key": "sk-ant-abc123", "model": "claude-sonnet-4-6"},
        is_active=True,
    )
    session.add(conn)
    await session.commit()
    await session.refresh(conn)
    return conn


@pytest.fixture
async def dataset(session: AsyncSession, admin_user, sf_connection):
    ds = Dataset(
        name="test-ds",
        sql="SELECT 1",
        snowflake_connection_id=sf_connection.id,
        models_used=[],
        created_by=admin_user.id,
    )
    session.add(ds)
    await session.commit()
    await session.refresh(ds)
    return ds


@pytest.fixture
async def chart(session: AsyncSession, admin_user, dataset):
    c = Chart(
        name="test-chart",
        datasource_id=dataset.id,
        versions=[],
        created_by=admin_user.id,
    )
    session.add(c)
    await session.commit()
    await session.refresh(c)
    return c


def auth_headers(token):
    return {"Authorization": f"Bearer {token}"}
