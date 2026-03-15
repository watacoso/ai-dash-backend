"""
Unit tests for SnowflakeSchemaService.
snowflake.connector.connect and _load_private_key_bytes are mocked at the module level.
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
    @pytest.fixture(autouse=True)
    def mock_key_loader(self):
        with patch("app.explore.schema_service._load_private_key_bytes", return_value=b"fake-key"):
            yield

    def test_should_return_database_names(self):
        # Arrange — SHOW DATABASES: col 0 = created_on, col 1 = name
        conn = _mock_conn([(None, "DB1"), (None, "DB2")])
        with patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeSchemaService(CREDS)
            # Act
            result = svc.list_databases()
        # Assert
        assert result == ["DB1", "DB2"]

    def test_should_return_schema_names_for_database(self):
        # Arrange — SHOW SCHEMAS: col 0 = created_on, col 1 = name
        conn = _mock_conn([(None, "PUBLIC"), (None, "RAW")])
        with patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeSchemaService(CREDS)
            # Act
            result = svc.list_schemas("MYDB")
        # Assert
        assert result == ["PUBLIC", "RAW"]

    def test_should_return_table_names_for_schema(self):
        # Arrange — SHOW TABLES: col 0 = created_on, col 1 = name
        conn = _mock_conn([(None, "ORDERS"), (None, "USERS")])
        with patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeSchemaService(CREDS)
            # Act
            result = svc.list_tables("MYDB", "PUBLIC")
        # Assert
        assert result == ["ORDERS", "USERS"]

    def test_should_return_column_names_for_table(self):
        # Arrange — INFORMATION_SCHEMA.COLUMNS SELECT: col 0 = COLUMN_NAME
        conn = _mock_conn([("id",), ("amount",), ("created_at",)])
        with patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeSchemaService(CREDS)
            # Act
            result = svc.list_columns("MYDB", "PUBLIC", "ORDERS")
        # Assert
        assert result == ["id", "amount", "created_at"]

    def test_should_pass_correct_params_to_column_query(self):
        # Arrange
        conn = _mock_conn([("id",)])
        with patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeSchemaService(CREDS)
            # Act
            svc.list_columns("MYDB", "PUBLIC", "ORDERS")
        # Assert — must use INFORMATION_SCHEMA SELECT with correct identifiers
        executed_sql: str = conn.cursor.return_value.execute.call_args[0][0]
        assert "INFORMATION_SCHEMA" in executed_sql
        assert "MYDB" in executed_sql
        assert "PUBLIC" in executed_sql
        assert "ORDERS" in executed_sql

    def test_should_return_empty_list_when_table_has_no_columns(self):
        # Arrange
        conn = _mock_conn([])
        with patch("app.explore.schema_service.snowflake.connector.connect", return_value=conn):
            svc = SnowflakeSchemaService(CREDS)
            # Act
            result = svc.list_columns("MYDB", "PUBLIC", "EMPTY_TABLE")
        # Assert
        assert result == []

    def test_should_propagate_database_error_from_list_columns(self):
        # Arrange
        with patch(
            "app.explore.schema_service.snowflake.connector.connect",
            side_effect=snowflake.connector.errors.DatabaseError("permission denied"),
        ):
            svc = SnowflakeSchemaService(CREDS)
            # Act / Assert
            with pytest.raises(snowflake.connector.errors.DatabaseError):
                svc.list_columns("MYDB", "PUBLIC", "ORDERS")

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
        conn = _mock_conn([(None, "DB1")])
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
