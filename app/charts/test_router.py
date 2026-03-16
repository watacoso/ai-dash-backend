import uuid

import pytest
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from app.auth.models import Base, Role, User
from app.auth.service import hash_password, create_token
from app.connections.models import Connection, ConnectionType
from app.datasets.models import Dataset
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


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


def _payload(dataset_id):
    return {"name": "My Chart", "datasource_id": str(dataset_id)}


class TestChartCRUD:
    async def test_create_chart_returns_201(self, client, admin_token, dataset):
        r = await client.post("/charts", json=_payload(dataset.id), headers=_auth(admin_token))
        assert r.status_code == 201
        body = r.json()
        assert body["name"] == "My Chart"
        assert body["datasource_id"] == str(dataset.id)
        assert body["versions"] == []

    async def test_create_chart_missing_name_returns_422(self, client, admin_token, dataset):
        r = await client.post("/charts", json={"datasource_id": str(dataset.id)}, headers=_auth(admin_token))
        assert r.status_code == 422

    async def test_create_chart_missing_datasource_returns_422(self, client, admin_token):
        r = await client.post("/charts", json={"name": "X"}, headers=_auth(admin_token))
        assert r.status_code == 422

    async def test_list_charts_returns_created_chart(self, client, admin_token, dataset):
        await client.post("/charts", json=_payload(dataset.id), headers=_auth(admin_token))
        r = await client.get("/charts", headers=_auth(admin_token))
        assert r.status_code == 200
        assert any(c["name"] == "My Chart" for c in r.json())

    async def test_get_chart_returns_200(self, client, admin_token, dataset):
        created = (await client.post("/charts", json=_payload(dataset.id), headers=_auth(admin_token))).json()
        r = await client.get(f"/charts/{created['id']}", headers=_auth(admin_token))
        assert r.status_code == 200
        assert r.json()["id"] == created["id"]

    async def test_get_chart_unknown_id_returns_404(self, client, admin_token):
        r = await client.get(f"/charts/{uuid.uuid4()}", headers=_auth(admin_token))
        assert r.status_code == 404

    async def test_patch_chart_name(self, client, admin_token, dataset):
        created = (await client.post("/charts", json=_payload(dataset.id), headers=_auth(admin_token))).json()
        r = await client.patch(f"/charts/{created['id']}", json={"name": "Renamed"}, headers=_auth(admin_token))
        assert r.status_code == 200
        assert r.json()["name"] == "Renamed"

    async def test_patch_chart_append_version(self, client, admin_token, dataset):
        created = (await client.post("/charts", json=_payload(dataset.id), headers=_auth(admin_token))).json()
        r = await client.patch(
            f"/charts/{created['id']}",
            json={"d3_code": "d3.select('svg')"},
            headers=_auth(admin_token),
        )
        assert r.status_code == 200
        versions = r.json()["versions"]
        assert len(versions) == 1
        assert versions[0]["d3_code"] == "d3.select('svg')"
        assert versions[0]["version"] == 0
        assert versions[0]["accepted"] is False
        assert "created_at" in versions[0]

    async def test_patch_chart_accept_version(self, client, admin_token, dataset):
        created = (await client.post("/charts", json=_payload(dataset.id), headers=_auth(admin_token))).json()
        # append two versions
        chart_id = created["id"]
        await client.patch(f"/charts/{chart_id}", json={"d3_code": "v0"}, headers=_auth(admin_token))
        await client.patch(f"/charts/{chart_id}", json={"d3_code": "v1"}, headers=_auth(admin_token))
        # accept version 1
        r = await client.patch(f"/charts/{chart_id}", json={"accepted_version": 1}, headers=_auth(admin_token))
        assert r.status_code == 200
        versions = r.json()["versions"]
        assert versions[0]["accepted"] is False
        assert versions[1]["accepted"] is True

    async def test_patch_chart_unknown_id_returns_404(self, client, admin_token):
        r = await client.patch(f"/charts/{uuid.uuid4()}", json={"name": "X"}, headers=_auth(admin_token))
        assert r.status_code == 404

    async def test_delete_chart_returns_204(self, client, admin_token, dataset):
        created = (await client.post("/charts", json=_payload(dataset.id), headers=_auth(admin_token))).json()
        r = await client.delete(f"/charts/{created['id']}", headers=_auth(admin_token))
        assert r.status_code == 204

    async def test_delete_chart_unknown_id_returns_404(self, client, admin_token):
        r = await client.delete(f"/charts/{uuid.uuid4()}", headers=_auth(admin_token))
        assert r.status_code == 404

    async def test_requires_auth(self, client):
        r = await client.get("/charts")
        assert r.status_code == 401
