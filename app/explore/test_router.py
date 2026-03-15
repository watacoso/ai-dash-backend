"""
Unit/integration tests for GET /explore/schema and POST /explore/chat.
Uses real test DB for connection fixtures; mocks external clients.
"""
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


def _mock_sf(mocker, rows: list[tuple]):
    """Patch snowflake key loader and connector to return rows from cursor.fetchall."""
    mocker.patch("app.explore.schema_service._load_private_key_bytes", return_value=b"fake-key")
    cursor = mocker.MagicMock()
    cursor.__enter__ = mocker.MagicMock(return_value=cursor)
    cursor.__exit__ = mocker.MagicMock(return_value=False)
    cursor.fetchall.return_value = rows
    conn = mocker.MagicMock()
    conn.cursor.return_value = cursor
    mocker.patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn)
    return conn


class TestSchemaEndpoint:
    async def test_should_return_databases(self, client, admin_token, sf_connection, mocker):
        # Arrange
        _mock_sf(mocker, [(None, "DB1"), (None, "DB2")])
        # Act
        res = await client.get(
            f"/explore/schema?connection_id={sf_connection.id}&level=databases",
            cookies={"access_token": admin_token},
        )
        # Assert
        assert res.status_code == 200
        assert res.json() == {"items": ["DB1", "DB2"]}

    async def test_should_return_schemas_for_database(self, client, admin_token, sf_connection, mocker):
        # Arrange
        _mock_sf(mocker, [(None, "PUBLIC"), (None, "RAW")])
        # Act
        res = await client.get(
            f"/explore/schema?connection_id={sf_connection.id}&level=schemas&database=MYDB",
            cookies={"access_token": admin_token},
        )
        # Assert
        assert res.status_code == 200
        assert res.json() == {"items": ["PUBLIC", "RAW"]}

    async def test_should_return_tables_for_schema(self, client, admin_token, sf_connection, mocker):
        # Arrange
        _mock_sf(mocker, [(None, "ORDERS")])
        # Act
        res = await client.get(
            f"/explore/schema?connection_id={sf_connection.id}&level=tables&database=MYDB&schema=PUBLIC",
            cookies={"access_token": admin_token},
        )
        # Assert
        assert res.status_code == 200
        assert res.json() == {"items": ["ORDERS"]}

    async def test_should_return_columns_for_table(self, client, admin_token, sf_connection, mocker):
        # Arrange
        _mock_sf(mocker, [("id",), ("amount",)])
        # Act
        res = await client.get(
            f"/explore/schema?connection_id={sf_connection.id}&level=columns"
            f"&database=MYDB&schema=PUBLIC&table=ORDERS",
            cookies={"access_token": admin_token},
        )
        # Assert
        assert res.status_code == 200
        assert res.json() == {"items": ["id", "amount"]}

    async def test_should_return_422_when_database_missing_for_schemas(
        self, client, admin_token, sf_connection
    ):
        # Act
        res = await client.get(
            f"/explore/schema?connection_id={sf_connection.id}&level=schemas",
            cookies={"access_token": admin_token},
        )
        # Assert
        assert res.status_code == 422

    async def test_should_return_422_when_database_missing_for_tables(
        self, client, admin_token, sf_connection
    ):
        # Act
        res = await client.get(
            f"/explore/schema?connection_id={sf_connection.id}&level=tables&schema=PUBLIC",
            cookies={"access_token": admin_token},
        )
        # Assert
        assert res.status_code == 422

    async def test_should_return_422_when_table_missing_for_columns(
        self, client, admin_token, sf_connection
    ):
        # Act
        res = await client.get(
            f"/explore/schema?connection_id={sf_connection.id}&level=columns"
            f"&database=MYDB&schema=PUBLIC",
            cookies={"access_token": admin_token},
        )
        # Assert
        assert res.status_code == 422

    async def test_should_return_404_for_unknown_connection(self, client, admin_token):
        # Act
        res = await client.get(
            f"/explore/schema?connection_id={uuid.uuid4()}&level=databases",
            cookies={"access_token": admin_token},
        )
        # Assert
        assert res.status_code == 404

    async def test_should_return_404_for_inactive_connection(
        self, client, admin_token, sf_connection, session
    ):
        # Arrange
        sf_connection.is_active = False
        await session.commit()
        # Act
        res = await client.get(
            f"/explore/schema?connection_id={sf_connection.id}&level=databases",
            cookies={"access_token": admin_token},
        )
        # Assert
        assert res.status_code == 404

    async def test_should_return_502_on_snowflake_error(
        self, client, admin_token, sf_connection, mocker
    ):
        # Arrange
        mocker.patch("app.explore.schema_service._load_private_key_bytes", return_value=b"fake-key")
        mocker.patch(
            "app.explore.schema_service.snowflake.connector.connect",
            side_effect=snowflake.connector.errors.DatabaseError("auth failed"),
        )
        # Act
        res = await client.get(
            f"/explore/schema?connection_id={sf_connection.id}&level=databases",
            cookies={"access_token": admin_token},
        )
        # Assert
        assert res.status_code == 502
        assert "detail" in res.json()

    async def test_should_return_401_when_unauthenticated(self, client, sf_connection):
        # Act
        res = await client.get(
            f"/explore/schema?connection_id={sf_connection.id}&level=databases"
        )
        # Assert
        assert res.status_code == 401

    async def test_should_allow_analyst_role(self, client, analyst_token, sf_connection, mocker):
        # Arrange
        _mock_sf(mocker, [(None, "DB1")])
        # Act
        res = await client.get(
            f"/explore/schema?connection_id={sf_connection.id}&level=databases",
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 200


CLAUDE_CREDS = {
    "api_key": "sk-ant-test",
    "model": "claude-sonnet-4-6",
}


def _make_text_response(text: str) -> MagicMock:
    """Anthropic response with stop_reason=end_turn and a text block."""
    block = MagicMock()
    block.type = "text"
    block.text = text
    resp = MagicMock()
    resp.stop_reason = "end_turn"
    resp.content = [block]
    return resp


def _make_tool_use_response(tool_use_id: str, tool_name: str, tool_input: dict) -> MagicMock:
    """Anthropic response with stop_reason=tool_use and a tool_use block."""
    block = MagicMock()
    block.type = "tool_use"
    block.id = tool_use_id
    block.name = tool_name
    block.input = tool_input
    resp = MagicMock()
    resp.stop_reason = "tool_use"
    resp.content = [block]
    return resp


@pytest.fixture
async def claude_connection(session: AsyncSession, admin_user: User):
    conn = Connection(
        name="test-claude",
        type=ConnectionType.claude,
        owner_id=admin_user.id,
        credentials=CLAUDE_CREDS,
        is_active=True,
    )
    session.add(conn)
    await session.commit()
    await session.refresh(conn)
    return conn


class TestChatEndpoint:
    async def test_should_return_assistant_message_when_no_tool_call(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange
        mocker.patch(
            "app.explore.router.anthropic.Anthropic",
            return_value=MagicMock(
                messages=MagicMock(
                    create=MagicMock(return_value=_make_text_response("Here are your results."))
                )
            ),
        )
        payload = {
            "snowflake_connection_id": str(sf_connection.id),
            "claude_connection_id": str(claude_connection.id),
            "messages": [{"role": "user", "content": "Hello"}],
        }
        # Act
        res = await client.post(
            "/explore/chat",
            json=payload,
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 200
        assert res.json() == {"role": "assistant", "content": "Here are your results."}

    async def test_should_return_assistant_message_after_one_tool_call(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange
        tool_resp = _make_tool_use_response("tu_1", "get_schema", {"level": "databases"})
        text_resp = _make_text_response("You have DB1 and DB2.")
        mock_create = MagicMock(side_effect=[tool_resp, text_resp])
        mocker.patch(
            "app.explore.router.anthropic.Anthropic",
            return_value=MagicMock(messages=MagicMock(create=mock_create)),
        )
        _mock_sf(mocker, [(None, "DB1"), (None, "DB2")])
        payload = {
            "snowflake_connection_id": str(sf_connection.id),
            "claude_connection_id": str(claude_connection.id),
            "messages": [{"role": "user", "content": "What databases do I have?"}],
        }
        # Act
        res = await client.post(
            "/explore/chat",
            json=payload,
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 200
        assert res.json() == {"role": "assistant", "content": "You have DB1 and DB2."}
        assert mock_create.call_count == 2

    async def test_should_return_404_when_snowflake_connection_missing(
        self, client, analyst_token, claude_connection
    ):
        # Arrange
        payload = {
            "snowflake_connection_id": str(uuid.uuid4()),
            "claude_connection_id": str(claude_connection.id),
            "messages": [{"role": "user", "content": "Hi"}],
        }
        # Act
        res = await client.post(
            "/explore/chat", json=payload, cookies={"access_token": analyst_token}
        )
        # Assert
        assert res.status_code == 404

    async def test_should_return_404_when_claude_connection_missing(
        self, client, analyst_token, sf_connection
    ):
        # Arrange
        payload = {
            "snowflake_connection_id": str(sf_connection.id),
            "claude_connection_id": str(uuid.uuid4()),
            "messages": [{"role": "user", "content": "Hi"}],
        }
        # Act
        res = await client.post(
            "/explore/chat", json=payload, cookies={"access_token": analyst_token}
        )
        # Assert
        assert res.status_code == 404

    async def test_should_return_404_when_snowflake_connection_inactive(
        self, client, analyst_token, sf_connection, claude_connection, session
    ):
        # Arrange
        sf_connection.is_active = False
        await session.commit()
        payload = {
            "snowflake_connection_id": str(sf_connection.id),
            "claude_connection_id": str(claude_connection.id),
            "messages": [{"role": "user", "content": "Hi"}],
        }
        # Act
        res = await client.post(
            "/explore/chat", json=payload, cookies={"access_token": analyst_token}
        )
        # Assert
        assert res.status_code == 404

    async def test_should_return_404_when_claude_connection_inactive(
        self, client, analyst_token, sf_connection, claude_connection, session
    ):
        # Arrange
        claude_connection.is_active = False
        await session.commit()
        payload = {
            "snowflake_connection_id": str(sf_connection.id),
            "claude_connection_id": str(claude_connection.id),
            "messages": [{"role": "user", "content": "Hi"}],
        }
        # Act
        res = await client.post(
            "/explore/chat", json=payload, cookies={"access_token": analyst_token}
        )
        # Assert
        assert res.status_code == 404

    async def test_should_propagate_tool_error_gracefully(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange
        tool_resp = _make_tool_use_response("tu_1", "get_schema", {"level": "databases"})
        text_resp = _make_text_response("I could not retrieve the schema.")
        mock_create = MagicMock(side_effect=[tool_resp, text_resp])
        mocker.patch(
            "app.explore.router.anthropic.Anthropic",
            return_value=MagicMock(messages=MagicMock(create=mock_create)),
        )
        mocker.patch("app.explore.schema_service._load_private_key_bytes", return_value=b"fake-key")
        mocker.patch(
            "app.explore.schema_service.snowflake.connector.connect",
            side_effect=snowflake.connector.errors.DatabaseError("connection refused"),
        )
        payload = {
            "snowflake_connection_id": str(sf_connection.id),
            "claude_connection_id": str(claude_connection.id),
            "messages": [{"role": "user", "content": "What databases?"}],
        }
        # Act
        res = await client.post(
            "/explore/chat", json=payload, cookies={"access_token": analyst_token}
        )
        # Assert
        assert res.status_code == 200
        assert res.json()["role"] == "assistant"
        assert mock_create.call_count == 2

    async def test_should_return_error_message_when_max_iterations_exceeded(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange — Claude always returns tool_use
        tool_resp = _make_tool_use_response("tu_1", "get_schema", {"level": "databases"})
        mock_create = MagicMock(return_value=tool_resp)
        mocker.patch(
            "app.explore.router.anthropic.Anthropic",
            return_value=MagicMock(messages=MagicMock(create=mock_create)),
        )
        _mock_sf(mocker, [(None, "DB1")])
        payload = {
            "snowflake_connection_id": str(sf_connection.id),
            "claude_connection_id": str(claude_connection.id),
            "messages": [{"role": "user", "content": "What databases?"}],
        }
        # Act
        res = await client.post(
            "/explore/chat", json=payload, cookies={"access_token": analyst_token}
        )
        # Assert
        assert res.status_code == 200
        data = res.json()
        assert data["role"] == "assistant"
        assert "limit" in data["content"].lower() or "iterations" in data["content"].lower()
        assert mock_create.call_count == 5

    async def test_should_return_401_when_unauthenticated(
        self, client, sf_connection, claude_connection
    ):
        # Arrange
        payload = {
            "snowflake_connection_id": str(sf_connection.id),
            "claude_connection_id": str(claude_connection.id),
            "messages": [{"role": "user", "content": "Hi"}],
        }
        # Act
        res = await client.post("/explore/chat", json=payload)
        # Assert
        assert res.status_code == 401

    async def test_should_allow_analyst_role(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange
        mocker.patch(
            "app.explore.router.anthropic.Anthropic",
            return_value=MagicMock(
                messages=MagicMock(
                    create=MagicMock(return_value=_make_text_response("OK"))
                )
            ),
        )
        payload = {
            "snowflake_connection_id": str(sf_connection.id),
            "claude_connection_id": str(claude_connection.id),
            "messages": [{"role": "user", "content": "Hi"}],
        }
        # Act
        res = await client.post(
            "/explore/chat", json=payload, cookies={"access_token": analyst_token}
        )
        # Assert
        assert res.status_code == 200
