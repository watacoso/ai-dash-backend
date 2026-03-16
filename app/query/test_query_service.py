"""
Unit tests for SnowflakeQueryService.execute_sample.
snowflake.connector.connect and _load_private_key_bytes are mocked at the module level.
"""
import pytest
import snowflake.connector.errors
from unittest.mock import MagicMock, patch

from app.query.query_service import SnowflakeQueryService

CREDS = {
    "account": "xy12345",
    "username": "svc",
    "private_key": "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
    "warehouse": "WH",
    "database": "DB",
}


def _mock_conn(description: list[tuple], rows: list[tuple]) -> MagicMock:
    """Return a mock connector connection whose cursor yields the given description and rows."""
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.description = description
    cursor.fetchall.return_value = rows
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


class TestSnowflakeQueryService:
    @pytest.fixture(autouse=True)
    def mock_key_loader(self):
        with patch("app.query.query_service._load_private_key_bytes", return_value=b"fake-key"):
            yield

    def test_should_wrap_plain_sql_with_default_limit(self):
        # Arrange
        conn = _mock_conn([("id", None, None, None, None, None, None)], [])
        with patch("app.query.query_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeQueryService(CREDS)
            # Act
            svc.execute_sample("SELECT id FROM orders")
        # Assert — subquery wrapping with default LIMIT 200
        executed_sql: str = conn.cursor.return_value.execute.call_args[0][0]
        assert "SELECT * FROM (SELECT id FROM orders) AS _sample LIMIT 200" == executed_sql

    def test_should_wrap_sql_with_custom_limit(self):
        # Arrange
        conn = _mock_conn([("id", None, None, None, None, None, None)], [])
        with patch("app.query.query_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeQueryService(CREDS)
            # Act
            svc.execute_sample("SELECT id FROM orders", limit=50)
        # Assert
        executed_sql: str = conn.cursor.return_value.execute.call_args[0][0]
        assert "SELECT * FROM (SELECT id FROM orders) AS _sample LIMIT 50" == executed_sql

    def test_should_wrap_sql_that_already_contains_limit(self):
        # Arrange — SQL already has LIMIT; must still be wrapped so outer LIMIT applies cleanly
        conn = _mock_conn([("id", None, None, None, None, None, None)], [])
        with patch("app.query.query_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeQueryService(CREDS)
            # Act
            svc.execute_sample("SELECT id FROM orders LIMIT 10")
        # Assert — wrapped regardless
        executed_sql: str = conn.cursor.return_value.execute.call_args[0][0]
        assert "SELECT * FROM (SELECT id FROM orders LIMIT 10) AS _sample LIMIT 200" == executed_sql

    def test_should_wrap_sql_that_already_contains_order_by(self):
        # Arrange — SQL with ORDER BY; wrapping prevents ORDER BY from interfering with outer LIMIT
        conn = _mock_conn([("id", None, None, None, None, None, None)], [])
        with patch("app.query.query_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeQueryService(CREDS)
            # Act
            svc.execute_sample("SELECT id FROM orders ORDER BY id")
        # Assert
        executed_sql: str = conn.cursor.return_value.execute.call_args[0][0]
        assert "SELECT * FROM (SELECT id FROM orders ORDER BY id) AS _sample LIMIT 200" == executed_sql

    def test_should_return_columns_and_rows_on_success(self):
        # Arrange
        description = [("id", None, None, None, None, None, None), ("name", None, None, None, None, None, None)]
        rows = [("1", "Alice"), ("2", "Bob")]
        conn = _mock_conn(description, rows)
        with patch("app.query.query_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeQueryService(CREDS)
            # Act
            result = svc.execute_sample("SELECT id, name FROM users")
        # Assert
        assert result == {"columns": ["id", "name"], "rows": [["1", "Alice"], ["2", "Bob"]]}

    def test_should_return_empty_rows_when_query_has_no_results(self):
        # Arrange
        description = [("id", None, None, None, None, None, None)]
        conn = _mock_conn(description, [])
        with patch("app.query.query_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeQueryService(CREDS)
            # Act
            result = svc.execute_sample("SELECT id FROM orders WHERE 1=0")
        # Assert
        assert result == {"columns": ["id"], "rows": []}

    def test_should_propagate_database_error(self):
        # Arrange
        with patch(
            "app.query.query_service.snowflake.connector.connect",
            side_effect=snowflake.connector.errors.DatabaseError("permission denied"),
        ):
            svc = SnowflakeQueryService(CREDS)
            # Act / Assert
            with pytest.raises(snowflake.connector.errors.DatabaseError):
                svc.execute_sample("SELECT id FROM orders")

    def test_should_propagate_programming_error(self):
        # Arrange — ProgrammingError is raised by Snowflake for SQL syntax errors
        conn = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.execute.side_effect = snowflake.connector.errors.ProgrammingError("syntax error")
        conn.cursor.return_value = cursor
        with patch("app.query.query_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeQueryService(CREDS)
            # Act / Assert
            with pytest.raises(snowflake.connector.errors.ProgrammingError):
                svc.execute_sample("SELECT * FORM orders")
