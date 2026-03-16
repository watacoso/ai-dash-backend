import uuid
from unittest.mock import MagicMock

import pytest
import snowflake.connector.errors
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
async def sf_connection(session: AsyncSession, admin_user):
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
            "passphrase": None,
        },
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


# ── Schema ─────────────────────────────────────────────────────────────────────

class TestDatasetSchema:
    async def test_should_create_datasets_table_with_expected_columns(self, session):
        from sqlalchemy import inspect, text
        async with engine.connect() as conn:
            cols = await conn.run_sync(
                lambda sync_conn: [
                    c["name"]
                    for c in inspect(sync_conn).get_columns("datasets")
                ]
            )
        expected = {
            "id", "name", "description", "sql",
            "snowflake_connection_id", "claude_connection_id",
            "models_used", "created_by", "created_at", "updated_at",
        }
        assert expected.issubset(set(cols))


# ── GET /datasets ──────────────────────────────────────────────────────────────

class TestListDatasets:
    async def test_should_return_empty_list_when_no_datasets_exist(
        self, client, analyst_token
    ):
        res = await client.get("/datasets", cookies={"access_token": analyst_token})
        assert res.status_code == 200
        assert res.json() == []

    async def test_should_return_all_datasets_for_authenticated_user(
        self, client, analyst_token, sf_connection
    ):
        payload = {
            "name": "ds1",
            "sql": "SELECT 1",
            "snowflake_connection_id": str(sf_connection.id),
        }
        await client.post("/datasets", json=payload, cookies={"access_token": analyst_token})
        await client.post(
            "/datasets",
            json={**payload, "name": "ds2"},
            cookies={"access_token": analyst_token},
        )

        res = await client.get("/datasets", cookies={"access_token": analyst_token})
        assert res.status_code == 200
        assert len(res.json()) == 2

    async def test_should_return_401_when_unauthenticated(self, client):
        res = await client.get("/datasets")
        assert res.status_code == 401


# ── POST /datasets ─────────────────────────────────────────────────────────────

class TestCreateDataset:
    async def test_should_create_dataset_and_return_201_with_id(
        self, client, analyst_token, sf_connection
    ):
        res = await client.post(
            "/datasets",
            json={
                "name": "my dataset",
                "sql": "SELECT id FROM orders",
                "snowflake_connection_id": str(sf_connection.id),
            },
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 201
        data = res.json()
        assert "id" in data
        assert data["name"] == "my dataset"

    async def test_should_persist_name_sql_connection_and_created_by(
        self, client, analyst_user, analyst_token, sf_connection, session
    ):
        from app.datasets.models import Dataset
        res = await client.post(
            "/datasets",
            json={
                "name": "persist test",
                "sql": "SELECT 2",
                "snowflake_connection_id": str(sf_connection.id),
            },
            cookies={"access_token": analyst_token},
        )
        ds_id = uuid.UUID(res.json()["id"])
        ds = await session.get(Dataset, ds_id)
        await session.refresh(ds)
        assert ds.name == "persist test"
        assert ds.sql == "SELECT 2"
        assert ds.snowflake_connection_id == sf_connection.id
        assert ds.created_by == analyst_user.id

    async def test_should_default_description_to_empty_and_models_used_to_empty_list(
        self, client, analyst_token, sf_connection
    ):
        res = await client.post(
            "/datasets",
            json={"name": "defaults", "sql": "SELECT 3", "snowflake_connection_id": str(sf_connection.id)},
            cookies={"access_token": analyst_token},
        )
        data = res.json()
        assert data["description"] == ""
        assert data["models_used"] == []

    async def test_should_accept_optional_claude_connection_id(
        self, client, analyst_token, sf_connection, cl_connection
    ):
        res = await client.post(
            "/datasets",
            json={
                "name": "with claude",
                "sql": "SELECT 4",
                "snowflake_connection_id": str(sf_connection.id),
                "claude_connection_id": str(cl_connection.id),
            },
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 201
        assert res.json()["claude_connection_id"] == str(cl_connection.id)

    async def test_should_return_422_when_name_missing(
        self, client, analyst_token, sf_connection
    ):
        res = await client.post(
            "/datasets",
            json={"sql": "SELECT 1", "snowflake_connection_id": str(sf_connection.id)},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 422

    async def test_should_return_422_when_sql_missing(
        self, client, analyst_token, sf_connection
    ):
        res = await client.post(
            "/datasets",
            json={"name": "x", "snowflake_connection_id": str(sf_connection.id)},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 422

    async def test_should_return_422_when_snowflake_connection_id_missing(
        self, client, analyst_token
    ):
        res = await client.post(
            "/datasets",
            json={"name": "x", "sql": "SELECT 1"},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 422

    async def test_should_return_404_when_snowflake_connection_id_does_not_exist(
        self, client, analyst_token
    ):
        res = await client.post(
            "/datasets",
            json={"name": "x", "sql": "SELECT 1", "snowflake_connection_id": str(uuid.uuid4())},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 404

    async def test_should_return_401_when_unauthenticated(self, client, sf_connection):
        res = await client.post(
            "/datasets",
            json={"name": "x", "sql": "SELECT 1", "snowflake_connection_id": str(sf_connection.id)},
        )
        assert res.status_code == 401


# ── GET /datasets/{id} ─────────────────────────────────────────────────────────

class TestGetDataset:
    async def _create(self, client, token, sf_id, name="ds"):
        res = await client.post(
            "/datasets",
            json={"name": name, "sql": "SELECT 1", "snowflake_connection_id": str(sf_id)},
            cookies={"access_token": token},
        )
        return res.json()["id"]

    async def test_should_return_dataset_by_id(
        self, client, analyst_token, sf_connection
    ):
        ds_id = await self._create(client, analyst_token, sf_connection.id, "fetched")
        res = await client.get(f"/datasets/{ds_id}", cookies={"access_token": analyst_token})
        assert res.status_code == 200
        assert res.json()["name"] == "fetched"

    async def test_should_return_404_when_dataset_does_not_exist(
        self, client, analyst_token
    ):
        res = await client.get(
            f"/datasets/{uuid.uuid4()}", cookies={"access_token": analyst_token}
        )
        assert res.status_code == 404

    async def test_should_return_401_when_unauthenticated(self, client, analyst_token, sf_connection):
        ds_id = await self._create(client, analyst_token, sf_connection.id)
        res = await client.get(f"/datasets/{ds_id}")
        assert res.status_code == 401


# ── PATCH /datasets/{id} ───────────────────────────────────────────────────────

class TestUpdateDataset:
    async def _create(self, client, token, sf_id):
        res = await client.post(
            "/datasets",
            json={"name": "original", "sql": "SELECT 1", "snowflake_connection_id": str(sf_id)},
            cookies={"access_token": token},
        )
        return res.json()["id"]

    async def test_should_update_name(self, client, analyst_token, sf_connection):
        ds_id = await self._create(client, analyst_token, sf_connection.id)
        res = await client.patch(
            f"/datasets/{ds_id}",
            json={"name": "renamed"},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 200
        assert res.json()["name"] == "renamed"

    async def test_should_update_description(self, client, analyst_token, sf_connection):
        ds_id = await self._create(client, analyst_token, sf_connection.id)
        res = await client.patch(
            f"/datasets/{ds_id}",
            json={"description": "new desc"},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 200
        assert res.json()["description"] == "new desc"

    async def test_should_update_sql(self, client, analyst_token, sf_connection):
        ds_id = await self._create(client, analyst_token, sf_connection.id)
        res = await client.patch(
            f"/datasets/{ds_id}",
            json={"sql": "SELECT 99"},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 200
        assert res.json()["sql"] == "SELECT 99"

    async def test_should_ignore_models_used_in_patch(
        self, client, analyst_token, sf_connection, session
    ):
        from app.datasets.models import Dataset
        ds_id = await self._create(client, analyst_token, sf_connection.id)
        res = await client.patch(
            f"/datasets/{ds_id}",
            json={"models_used": ["gpt-4"]},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 200
        ds = await session.get(Dataset, uuid.UUID(ds_id))
        await session.refresh(ds)
        assert ds.models_used == []

    async def test_should_return_404_when_dataset_does_not_exist(
        self, client, analyst_token
    ):
        res = await client.patch(
            f"/datasets/{uuid.uuid4()}",
            json={"name": "x"},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 404

    async def test_should_return_401_when_unauthenticated(
        self, client, analyst_token, sf_connection
    ):
        ds_id = await self._create(client, analyst_token, sf_connection.id)
        res = await client.patch(f"/datasets/{ds_id}", json={"name": "x"})
        assert res.status_code == 401


# ── DELETE /datasets/{id} ──────────────────────────────────────────────────────

class TestDeleteDataset:
    async def _create(self, client, token, sf_id):
        res = await client.post(
            "/datasets",
            json={"name": "to-delete", "sql": "SELECT 1", "snowflake_connection_id": str(sf_id)},
            cookies={"access_token": token},
        )
        return res.json()["id"]

    async def test_should_delete_dataset_and_return_204(
        self, client, analyst_token, sf_connection, session
    ):
        from app.datasets.models import Dataset
        ds_id = await self._create(client, analyst_token, sf_connection.id)
        res = await client.delete(
            f"/datasets/{ds_id}", cookies={"access_token": analyst_token}
        )
        assert res.status_code == 204
        ds = await session.get(Dataset, uuid.UUID(ds_id))
        assert ds is None

    async def test_should_return_404_when_dataset_does_not_exist(
        self, client, analyst_token
    ):
        res = await client.delete(
            f"/datasets/{uuid.uuid4()}", cookies={"access_token": analyst_token}
        )
        assert res.status_code == 404

    async def test_should_return_401_when_unauthenticated(
        self, client, analyst_token, sf_connection
    ):
        ds_id = await self._create(client, analyst_token, sf_connection.id)
        res = await client.delete(f"/datasets/{ds_id}")
        assert res.status_code == 401


# ── POST /datasets/run + POST /datasets/{id}/run ───────────────────────────────

def _mock_execute_sample(mocker, columns=None, rows=None):
    """Patch SnowflakeQueryService.execute_sample to return a canned result."""
    columns = columns or ["id", "name"]
    rows = rows or [[1, "foo"], [2, "bar"]]
    mock_svc = MagicMock()
    mock_svc.execute_sample.return_value = {"columns": columns, "rows": rows}
    mocker.patch(
        "app.datasets.router.SnowflakeQueryService",
        return_value=mock_svc,
    )
    return mock_svc


class TestRunDataset:
    async def _create_dataset(self, client, token, sf_id, sql="SELECT id FROM orders"):
        res = await client.post(
            "/datasets",
            json={"name": "run-ds", "sql": sql, "snowflake_connection_id": str(sf_id)},
            cookies={"access_token": token},
        )
        return res.json()["id"]

    async def test_should_execute_sql_and_return_result_shape(
        self, client, analyst_token, sf_connection, mocker
    ):
        _mock_execute_sample(mocker, columns=["id"], rows=[[1], [2]])
        res = await client.post(
            "/datasets/run",
            json={"sql": "SELECT id FROM t", "snowflake_connection_id": str(sf_connection.id)},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 200
        data = res.json()
        assert data["columns"] == ["id"]
        assert data["rows"] == [[1], [2]]
        assert data["row_count"] == 2
        assert isinstance(data["duration_ms"], int) and data["duration_ms"] >= 0
        assert "executed_at" in data

    async def test_should_use_snowflake_connection_id_from_body(
        self, client, analyst_token, sf_connection, mocker
    ):
        mock_svc = _mock_execute_sample(mocker)
        await client.post(
            "/datasets/run",
            json={"sql": "SELECT 1", "snowflake_connection_id": str(sf_connection.id)},
            cookies={"access_token": analyst_token},
        )
        mock_svc.execute_sample.assert_called_once_with("SELECT 1")

    async def test_should_return_422_with_error_on_database_error(
        self, client, analyst_token, sf_connection, mocker
    ):
        mock_svc = MagicMock()
        mock_svc.execute_sample.side_effect = snowflake.connector.errors.DatabaseError("bad sql")
        mocker.patch("app.datasets.router.SnowflakeQueryService", return_value=mock_svc)
        res = await client.post(
            "/datasets/run",
            json={"sql": "SELECT bad", "snowflake_connection_id": str(sf_connection.id)},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 422
        assert "error" in res.json()

    async def test_should_return_422_with_error_on_programming_error(
        self, client, analyst_token, sf_connection, mocker
    ):
        mock_svc = MagicMock()
        mock_svc.execute_sample.side_effect = snowflake.connector.errors.ProgrammingError("syntax")
        mocker.patch("app.datasets.router.SnowflakeQueryService", return_value=mock_svc)
        res = await client.post(
            "/datasets/run",
            json={"sql": "SELEKT bad", "snowflake_connection_id": str(sf_connection.id)},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 422
        assert "error" in res.json()

    async def test_should_return_404_when_adhoc_connection_not_found(
        self, client, analyst_token
    ):
        res = await client.post(
            "/datasets/run",
            json={"sql": "SELECT 1", "snowflake_connection_id": str(uuid.uuid4())},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 404

    async def test_should_return_401_when_adhoc_unauthenticated(self, client, sf_connection):
        res = await client.post(
            "/datasets/run",
            json={"sql": "SELECT 1", "snowflake_connection_id": str(sf_connection.id)},
        )
        assert res.status_code == 401

    async def test_should_run_saved_dataset_using_stored_connection(
        self, client, analyst_token, sf_connection, mocker
    ):
        mock_svc = _mock_execute_sample(mocker)
        ds_id = await self._create_dataset(
            client, analyst_token, sf_connection.id, sql="SELECT id FROM orders"
        )
        await client.post(
            f"/datasets/{ds_id}/run",
            cookies={"access_token": analyst_token},
        )
        mock_svc.execute_sample.assert_called_once_with("SELECT id FROM orders")

    async def test_should_return_result_shape_for_saved_dataset(
        self, client, analyst_token, sf_connection, mocker
    ):
        _mock_execute_sample(mocker, columns=["x"], rows=[[42]])
        ds_id = await self._create_dataset(client, analyst_token, sf_connection.id)
        res = await client.post(
            f"/datasets/{ds_id}/run",
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 200
        data = res.json()
        assert data["columns"] == ["x"]
        assert data["row_count"] == 1
        assert isinstance(data["duration_ms"], int)
        assert "executed_at" in data

    async def test_should_return_422_on_snowflake_error_for_saved_dataset(
        self, client, analyst_token, sf_connection, mocker
    ):
        mock_svc = MagicMock()
        mock_svc.execute_sample.side_effect = snowflake.connector.errors.DatabaseError("err")
        mocker.patch("app.datasets.router.SnowflakeQueryService", return_value=mock_svc)
        ds_id = await self._create_dataset(client, analyst_token, sf_connection.id)
        res = await client.post(
            f"/datasets/{ds_id}/run",
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 422
        assert "error" in res.json()

    async def test_should_return_404_when_saved_dataset_not_found(
        self, client, analyst_token
    ):
        res = await client.post(
            f"/datasets/{uuid.uuid4()}/run",
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 404

    async def test_should_return_401_when_saved_run_unauthenticated(
        self, client, analyst_token, sf_connection
    ):
        ds_id = await self._create_dataset(client, analyst_token, sf_connection.id)
        res = await client.post(f"/datasets/{ds_id}/run")
        assert res.status_code == 401

    async def test_should_not_confuse_run_literal_with_dataset_id(
        self, client, analyst_token, sf_connection, mocker
    ):
        # POST /datasets/run with a valid payload must NOT be routed to
        # POST /datasets/{dataset_id} (which would 422 on UUID parse of "run").
        # A 200 here confirms the literal route is registered first.
        _mock_execute_sample(mocker)
        res = await client.post(
            "/datasets/run",
            json={"sql": "SELECT 1", "snowflake_connection_id": str(sf_connection.id)},
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 200
