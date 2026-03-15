"""
Unit tests for SnowflakeSchemaService.
snowflake.connector.connect is mocked at the module level.
"""
import pytest
import snowflake.connector.errors
from unittest.mock import MagicMock, patch

from app.explore.schema_service import SnowflakeSchemaService

CREDS = {
    "account": "xy12345",
    "username": "svc",
    "private_key": "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
    "warehouse": "WH",
    "database": "DB",
    "schema": "PUBLIC",
}


def _mock_conn(rows: list[tuple]) -> MagicMock:
    """Return a mock connector connection whose cursor yields the given rows."""
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchall.return_value = rows
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


class TestSnowflakeSchemaService:
    def test_should_return_database_names(self):
        # Arrange
        conn = _mock_conn([("DB1",), ("DB2",)])
        with patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeSchemaService(CREDS)
            # Act
            result = svc.list_databases()
        # Assert
        assert result == ["DB1", "DB2"]

    def test_should_return_schema_names_for_database(self):
        # Arrange
        conn = _mock_conn([("PUBLIC",), ("RAW",)])
        with patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeSchemaService(CREDS)
            # Act
            result = svc.list_schemas("MYDB")
        # Assert
        assert result == ["PUBLIC", "RAW"]

    def test_should_return_table_names_for_schema(self):
        # Arrange
        conn = _mock_conn([("ORDERS",), ("USERS",)])
        with patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeSchemaService(CREDS)
            # Act
            result = svc.list_tables("MYDB", "PUBLIC")
        # Assert
        assert result == ["ORDERS", "USERS"]

    def test_should_return_column_names_for_table(self):
        # Arrange
        conn = _mock_conn([("id",), ("amount",), ("created_at",)])
        with patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeSchemaService(CREDS)
            # Act
            result = svc.list_columns("MYDB", "PUBLIC", "ORDERS")
        # Assert
        assert result == ["id", "amount", "created_at"]

    def test_should_propagate_database_error(self):
        # Arrange
        with patch(
            "app.explore.schema_service.snowflake.connector.connect",
            side_effect=snowflake.connector.errors.DatabaseError("bad creds"),
        ):
            svc = SnowflakeSchemaService(CREDS)
            # Act / Assert
            with pytest.raises(snowflake.connector.errors.DatabaseError):
                svc.list_databases()

    def test_should_close_connection_after_successful_call(self):
        # Arrange
        conn = _mock_conn([("DB1",)])
        with patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeSchemaService(CREDS)
            # Act
            svc.list_databases()
        # Assert
        conn.close.assert_called_once()

    def test_should_close_connection_even_when_error_raised(self):
        # Arrange
        conn = MagicMock()
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.execute.side_effect = snowflake.connector.errors.DatabaseError("fail")
        conn.cursor.return_value = cursor
        with patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeSchemaService(CREDS)
            # Act / Assert
            with pytest.raises(snowflake.connector.errors.DatabaseError):
                svc.list_databases()
        conn.close.assert_called_once()
