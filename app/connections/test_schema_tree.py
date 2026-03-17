"""
Tests for GET /connections/{id}/schema-tree (TKT-0045).
"""
import uuid

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

SNOWFLAKE_CREDS = {
    "account": "xy12345",
    "username": "svc",
    "private_key": "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
    "warehouse": "WH",
    "database": "DB",
    "schema": "PUBLIC",
}


@pytest.fixture(autouse=True)
async def setup_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear the in-process schema tree cache between tests."""
    import app.connections.router as router_mod
    router_mod._schema_tree_cache.clear()
    yield
    router_mod._schema_tree_cache.clear()


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
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
async def sf_connection(session: AsyncSession, admin_user: User):
    conn = Connection(
        name="test-sf",
        type=ConnectionType.snowflake,
        owner_id=admin_user.id,
        credentials=SNOWFLAKE_CREDS,
        is_active=True,
    )
    session.add(conn)
    await session.commit()
    await session.refresh(conn)
    return conn


@pytest.fixture
async def claude_connection(session: AsyncSession, admin_user: User):
    conn = Connection(
        name="test-claude",
        type=ConnectionType.claude,
        owner_id=admin_user.id,
        credentials={"api_key": "sk-test", "model": "claude-sonnet-4-6"},
        is_active=True,
    )
    session.add(conn)
    await session.commit()
    await session.refresh(conn)
    return conn


def _mock_sf_tree(mocker, databases, schemas_by_db, tables_by_db_schema):
    """
    Patch SnowflakeSchemaService methods to return controlled data.
    databases: list[str]
    schemas_by_db: dict[str, list[str]]
    tables_by_db_schema: dict[tuple[str,str], list[str]]
    """
    mocker.patch("app.connections.router.SnowflakeSchemaService.list_databases",
                 return_value=databases)

    def _list_schemas(self, db):
        return schemas_by_db.get(db, [])

    def _list_tables(self, db, schema):
        return tables_by_db_schema.get((db, schema), [])

    mocker.patch("app.connections.router.SnowflakeSchemaService.list_schemas",
                 new=_list_schemas)
    mocker.patch("app.connections.router.SnowflakeSchemaService.list_tables",
                 new=_list_tables)
    mocker.patch("app.connections.router.SnowflakeSchemaService._connect")
    mocker.patch("app.explore.schema_service._load_private_key_bytes", return_value=b"fake")


class TestSchemaTree:
    async def test_returns_full_tree(self, client, admin_token, sf_connection, mocker):
        # Arrange
        _mock_sf_tree(
            mocker,
            databases=["DB1", "DB2"],
            schemas_by_db={"DB1": ["PUBLIC", "RAW"], "DB2": ["PROD"]},
            tables_by_db_schema={
                ("DB1", "PUBLIC"): ["ORDERS", "USERS"],
                ("DB1", "RAW"): ["EVENTS"],
                ("DB2", "PROD"): ["SALES"],
            },
        )
        # Act
        res = await client.get(
            f"/connections/{sf_connection.id}/schema-tree",
            cookies={"access_token": admin_token},
        )
        # Assert
        assert res.status_code == 200
        data = res.json()
        dbs = {d["name"]: d for d in data["databases"]}
        assert set(dbs) == {"DB1", "DB2"}
        db1_schemas = {s["name"]: s for s in dbs["DB1"]["schemas"]}
        assert db1_schemas["PUBLIC"]["tables"] == ["ORDERS", "USERS"]
        assert db1_schemas["RAW"]["tables"] == ["EVENTS"]
        assert dbs["DB2"]["schemas"][0]["tables"] == ["SALES"]

    async def test_returns_404_for_unknown_connection(self, client, admin_token):
        res = await client.get(
            f"/connections/{uuid.uuid4()}/schema-tree",
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 404

    async def test_returns_404_for_inactive_connection(
        self, client, admin_token, sf_connection, session
    ):
        sf_connection.is_active = False
        await session.commit()
        res = await client.get(
            f"/connections/{sf_connection.id}/schema-tree",
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 404

    async def test_returns_404_for_non_snowflake_connection(
        self, client, admin_token, claude_connection
    ):
        res = await client.get(
            f"/connections/{claude_connection.id}/schema-tree",
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 404

    async def test_returns_401_when_unauthenticated(self, client, sf_connection):
        res = await client.get(f"/connections/{sf_connection.id}/schema-tree")
        assert res.status_code == 401

    async def test_allows_analyst_role(self, client, analyst_token, sf_connection, mocker):
        _mock_sf_tree(mocker, databases=["DB1"],
                      schemas_by_db={"DB1": ["PUBLIC"]},
                      tables_by_db_schema={("DB1", "PUBLIC"): ["T1"]})
        res = await client.get(
            f"/connections/{sf_connection.id}/schema-tree",
            cookies={"access_token": analyst_token},
        )
        assert res.status_code == 200

    async def test_cache_hit_does_not_re_fetch(self, client, admin_token, sf_connection, mocker):
        # Arrange
        list_dbs = mocker.patch(
            "app.connections.router.SnowflakeSchemaService.list_databases",
            return_value=["DB1"],
        )
        mocker.patch("app.connections.router.SnowflakeSchemaService.list_schemas",
                     return_value=["PUBLIC"])
        mocker.patch("app.connections.router.SnowflakeSchemaService.list_tables",
                     return_value=["T1"])
        mocker.patch("app.connections.router.SnowflakeSchemaService._connect")
        mocker.patch("app.explore.schema_service._load_private_key_bytes", return_value=b"fake")
        # Act — two requests within TTL
        res1 = await client.get(
            f"/connections/{sf_connection.id}/schema-tree",
            cookies={"access_token": admin_token},
        )
        res2 = await client.get(
            f"/connections/{sf_connection.id}/schema-tree",
            cookies={"access_token": admin_token},
        )
        # Assert — Snowflake only called once
        assert res1.status_code == 200
        assert res2.status_code == 200
        assert list_dbs.call_count == 1

    async def test_cache_miss_after_ttl_refetches(self, client, admin_token, sf_connection, mocker):
        # Arrange
        list_dbs = mocker.patch(
            "app.connections.router.SnowflakeSchemaService.list_databases",
            return_value=["DB1"],
        )
        mocker.patch("app.connections.router.SnowflakeSchemaService.list_schemas",
                     return_value=["PUBLIC"])
        mocker.patch("app.connections.router.SnowflakeSchemaService.list_tables",
                     return_value=["T1"])
        mocker.patch("app.connections.router.SnowflakeSchemaService._connect")
        mocker.patch("app.explore.schema_service._load_private_key_bytes", return_value=b"fake")

        # First request — populates cache
        await client.get(
            f"/connections/{sf_connection.id}/schema-tree",
            cookies={"access_token": admin_token},
        )
        assert list_dbs.call_count == 1

        # Expire the cache entry
        import app.connections.router as router_mod
        cache_key = str(sf_connection.id)
        _, tree = router_mod._schema_tree_cache[cache_key]
        router_mod._schema_tree_cache[cache_key] = (0.0, tree)  # expired

        # Second request — should re-fetch
        await client.get(
            f"/connections/{sf_connection.id}/schema-tree",
            cookies={"access_token": admin_token},
        )
        assert list_dbs.call_count == 2

    async def test_returns_502_on_snowflake_error(self, client, admin_token, sf_connection, mocker):
        mocker.patch("app.explore.schema_service._load_private_key_bytes", return_value=b"fake")
        mocker.patch(
            "app.explore.schema_service.snowflake.connector.connect",
            side_effect=snowflake.connector.errors.DatabaseError("auth failed"),
        )
        res = await client.get(
            f"/connections/{sf_connection.id}/schema-tree",
            cookies={"access_token": admin_token},
        )
        assert res.status_code == 502
