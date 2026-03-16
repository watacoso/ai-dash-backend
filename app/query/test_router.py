"""
Integration tests for POST /query/chat.
Uses real test DB for connection fixtures; mocks Anthropic client and Snowflake connector.
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
}

CLAUDE_CREDS = {
    "api_key": "sk-ant-test",
    "model": "claude-sonnet-4-6",
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
        credentials=CLAUDE_CREDS,
        is_active=True,
    )
    session.add(conn)
    await session.commit()
    await session.refresh(conn)
    return conn


def _make_text_response(text: str) -> MagicMock:
    block = MagicMock()
    block.type = "text"
    block.text = text
    resp = MagicMock()
    resp.stop_reason = "end_turn"
    resp.content = [block]
    return resp


def _make_tool_use_response(tool_use_id: str, tool_name: str, tool_input: dict) -> MagicMock:
    block = MagicMock()
    block.type = "tool_use"
    block.id = tool_use_id
    block.name = tool_name
    block.input = tool_input
    resp = MagicMock()
    resp.stop_reason = "tool_use"
    resp.content = [block]
    return resp


def _mock_sf(mocker, description: list[tuple], rows: list[tuple]):
    """Patch Snowflake key loader and connector for query service."""
    mocker.patch("app.query.query_service._load_private_key_bytes", return_value=b"fake-key")
    cursor = mocker.MagicMock()
    cursor.__enter__ = mocker.MagicMock(return_value=cursor)
    cursor.__exit__ = mocker.MagicMock(return_value=False)
    cursor.description = description
    cursor.fetchall.return_value = rows
    conn = mocker.MagicMock()
    conn.cursor.return_value = cursor
    mocker.patch("app.query.query_service.snowflake.connector.connect", return_value=conn)
    return conn


class TestQueryChatEndpoint:
    def _payload(self, sf_id, cl_id, text="generate a query for my orders"):
        return {
            "snowflake_connection_id": str(sf_id),
            "claude_connection_id": str(cl_id),
            "messages": [{"role": "user", "content": text}],
        }

    async def test_should_return_assistant_message_and_null_query_when_no_sql(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange
        mocker.patch(
            "app.query.router.anthropic.Anthropic",
            return_value=MagicMock(
                messages=MagicMock(create=MagicMock(return_value=_make_text_response("I need more info.")))
            ),
        )
        # Act
        res = await client.post(
            "/query/chat",
            json=self._payload(sf_connection.id, claude_connection.id),
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 200
        data = res.json()
        assert data["role"] == "assistant"
        assert data["content"] == "I need more info."
        assert data["query"] is None

    async def test_should_extract_sql_from_tagged_code_block(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange
        response_text = "Here is your query:\n```sql\nSELECT id, amount FROM orders\n```"
        mocker.patch(
            "app.query.router.anthropic.Anthropic",
            return_value=MagicMock(
                messages=MagicMock(create=MagicMock(return_value=_make_text_response(response_text)))
            ),
        )
        # Act
        res = await client.post(
            "/query/chat",
            json=self._payload(sf_connection.id, claude_connection.id),
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 200
        assert res.json()["query"] == "SELECT id, amount FROM orders"

    async def test_should_extract_sql_from_untagged_code_block(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange — single untagged block, no sql tag
        response_text = "Try this:\n```\nSELECT * FROM users\n```"
        mocker.patch(
            "app.query.router.anthropic.Anthropic",
            return_value=MagicMock(
                messages=MagicMock(create=MagicMock(return_value=_make_text_response(response_text)))
            ),
        )
        # Act
        res = await client.post(
            "/query/chat",
            json=self._payload(sf_connection.id, claude_connection.id),
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 200
        assert res.json()["query"] == "SELECT * FROM users"

    async def test_should_return_null_query_when_multiple_untagged_blocks(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange — two untagged blocks, ambiguous which is SQL → null
        response_text = "Option A:\n```\nSELECT a FROM t\n```\nOption B:\n```\nSELECT b FROM t\n```"
        mocker.patch(
            "app.query.router.anthropic.Anthropic",
            return_value=MagicMock(
                messages=MagicMock(create=MagicMock(return_value=_make_text_response(response_text)))
            ),
        )
        # Act
        res = await client.post(
            "/query/chat",
            json=self._payload(sf_connection.id, claude_connection.id),
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 200
        assert res.json()["query"] is None

    async def test_should_return_assistant_message_after_tool_call(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange
        tool_resp = _make_tool_use_response(
            "tu_1", "execute_query_sample", {"sql": "SELECT id FROM orders"}
        )
        text_resp = _make_text_response("Here is the result:\n```sql\nSELECT id FROM orders\n```")
        mock_create = MagicMock(side_effect=[tool_resp, text_resp])
        mocker.patch(
            "app.query.router.anthropic.Anthropic",
            return_value=MagicMock(messages=MagicMock(create=mock_create)),
        )
        _mock_sf(mocker, [("id", None, None, None, None, None, None)], [("1",), ("2",)])
        # Act
        res = await client.post(
            "/query/chat",
            json=self._payload(sf_connection.id, claude_connection.id),
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 200
        data = res.json()
        assert data["role"] == "assistant"
        assert data["query"] == "SELECT id FROM orders"
        assert mock_create.call_count == 2

    async def test_should_pass_sql_from_tool_input_to_execute_sample(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange
        tool_resp = _make_tool_use_response(
            "tu_1", "execute_query_sample", {"sql": "SELECT amount FROM orders"}
        )
        text_resp = _make_text_response("Done.")
        mocker.patch(
            "app.query.router.anthropic.Anthropic",
            return_value=MagicMock(messages=MagicMock(create=MagicMock(side_effect=[tool_resp, text_resp]))),
        )
        sf_conn_mock = _mock_sf(mocker, [("amount", None, None, None, None, None, None)], [("100",)])
        # Act
        await client.post(
            "/query/chat",
            json=self._payload(sf_connection.id, claude_connection.id),
            cookies={"access_token": analyst_token},
        )
        # Assert — the SQL from tool input was executed (wrapped in subquery)
        executed_sql: str = sf_conn_mock.cursor.return_value.execute.call_args[0][0]
        assert "SELECT amount FROM orders" in executed_sql

    async def test_should_return_401_when_unauthenticated(
        self, client, sf_connection, claude_connection
    ):
        # Act
        res = await client.post(
            "/query/chat", json=self._payload(sf_connection.id, claude_connection.id)
        )
        # Assert
        assert res.status_code == 401

    async def test_should_return_404_when_snowflake_connection_missing(
        self, client, analyst_token, claude_connection
    ):
        # Act
        res = await client.post(
            "/query/chat",
            json=self._payload(uuid.uuid4(), claude_connection.id),
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 404

    async def test_should_return_404_when_claude_connection_missing(
        self, client, analyst_token, sf_connection
    ):
        # Act
        res = await client.post(
            "/query/chat",
            json=self._payload(sf_connection.id, uuid.uuid4()),
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 404

    async def test_should_return_404_when_snowflake_connection_inactive(
        self, client, analyst_token, sf_connection, claude_connection, session
    ):
        # Arrange
        sf_connection.is_active = False
        await session.commit()
        # Act
        res = await client.post(
            "/query/chat",
            json=self._payload(sf_connection.id, claude_connection.id),
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 404

    async def test_should_propagate_tool_error_gracefully(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange — Snowflake raises on execute; Claude gets error string and replies
        tool_resp = _make_tool_use_response(
            "tu_1", "execute_query_sample", {"sql": "SELECT * FROM bad_table"}
        )
        text_resp = _make_text_response("I could not execute the query.")
        mock_create = MagicMock(side_effect=[tool_resp, text_resp])
        mocker.patch(
            "app.query.router.anthropic.Anthropic",
            return_value=MagicMock(messages=MagicMock(create=mock_create)),
        )
        mocker.patch("app.query.query_service._load_private_key_bytes", return_value=b"fake-key")
        mocker.patch(
            "app.query.query_service.snowflake.connector.connect",
            side_effect=snowflake.connector.errors.ProgrammingError("table not found"),
        )
        # Act
        res = await client.post(
            "/query/chat",
            json=self._payload(sf_connection.id, claude_connection.id),
            cookies={"access_token": analyst_token},
        )
        # Assert — endpoint returns 200, error was surfaced to Claude not the user
        assert res.status_code == 200
        assert res.json()["role"] == "assistant"
        assert mock_create.call_count == 2

    async def test_should_return_error_message_when_max_iterations_exceeded(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange — Claude always returns tool_use, never end_turn
        tool_resp = _make_tool_use_response(
            "tu_1", "execute_query_sample", {"sql": "SELECT 1"}
        )
        mock_create = MagicMock(return_value=tool_resp)
        mocker.patch(
            "app.query.router.anthropic.Anthropic",
            return_value=MagicMock(messages=MagicMock(create=mock_create)),
        )
        _mock_sf(mocker, [("1", None, None, None, None, None, None)], [("1",)])
        # Act
        res = await client.post(
            "/query/chat",
            json=self._payload(sf_connection.id, claude_connection.id),
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 200
        data = res.json()
        assert data["role"] == "assistant"
        assert "limit" in data["content"].lower() or "iterations" in data["content"].lower()
        assert mock_create.call_count == 5

    async def test_should_include_empty_logs_when_no_tool_calls(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange
        mocker.patch(
            "app.query.router.anthropic.Anthropic",
            return_value=MagicMock(
                messages=MagicMock(create=MagicMock(return_value=_make_text_response("Done.")))
            ),
        )
        # Act
        res = await client.post(
            "/query/chat",
            json=self._payload(sf_connection.id, claude_connection.id),
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 200
        assert res.json()["logs"] == []

    async def test_should_add_info_logs_for_tool_call_and_result(
        self, client, analyst_token, sf_connection, claude_connection, mocker
    ):
        # Arrange
        tool_resp = _make_tool_use_response(
            "tu_1", "execute_query_sample", {"sql": "SELECT id FROM orders"}
        )
        text_resp = _make_text_response("Here is the data.")
        mocker.patch(
            "app.query.router.anthropic.Anthropic",
            return_value=MagicMock(
                messages=MagicMock(create=MagicMock(side_effect=[tool_resp, text_resp]))
            ),
        )
        _mock_sf(mocker, [("id", None, None, None, None, None, None)], [("1",)])
        # Act
        res = await client.post(
            "/query/chat",
            json=self._payload(sf_connection.id, claude_connection.id),
            cookies={"access_token": analyst_token},
        )
        # Assert
        assert res.status_code == 200
        logs = res.json()["logs"]
        info_logs = [l for l in logs if l["level"] == "INFO"]
        assert len(info_logs) >= 2
        assert any("execute_query_sample" in l["message"] for l in info_logs)
        # First log is the call, second is the result
        assert "execute_query_sample" in logs[0]["message"]
