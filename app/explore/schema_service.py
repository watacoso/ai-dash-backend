"""
Snowflake schema introspection service.
All methods are synchronous (snowflake connector is blocking).
The router runs them in a thread-pool executor.
"""
import snowflake.connector
import snowflake.connector.errors
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    Encoding, NoEncryption, PrivateFormat, load_pem_private_key,
)


def _load_private_key_bytes(pem: str, passphrase: str | None) -> bytes:
    # Normalize literal \n sequences that survive JSON round-trips via some clients
    pem = pem.replace("\\n", "\n").strip()
    password = passphrase.encode() if passphrase else None
    key = load_pem_private_key(pem.encode(), password=password, backend=default_backend())
    return key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )


class SnowflakeSchemaService:
    def __init__(self, credentials: dict) -> None:
        self._creds = credentials

    def _connect(self):
        private_key_bytes = _load_private_key_bytes(
            self._creds["private_key"],
            self._creds.get("passphrase"),
        )
        return snowflake.connector.connect(
            account=self._creds["account"],
            user=self._creds["username"],
            private_key=private_key_bytes,
            warehouse=self._creds.get("warehouse"),
            database=self._creds.get("database"),
        )

    def _query(self, sql: str, col: int = 1) -> list[str]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return [str(row[col]) for row in cur.fetchall()]
        finally:
            conn.close()

    def list_databases(self) -> list[str]:
        # SHOW DATABASES: col 0 = created_on, col 1 = name
        return self._query("SHOW DATABASES", col=1)

    def list_schemas(self, database: str) -> list[str]:
        # SHOW SCHEMAS: col 0 = created_on, col 1 = name
        return self._query(f"SHOW SCHEMAS IN DATABASE {database}", col=1)

    def list_tables(self, database: str, schema: str) -> list[str]:
        # SHOW TABLES: col 0 = created_on, col 1 = name
        return self._query(f"SHOW TABLES IN SCHEMA {database}.{schema}", col=1)

    def list_columns(self, database: str, schema: str, table: str) -> list[str]:
        # INFORMATION_SCHEMA.COLUMNS SELECT: col 0 = COLUMN_NAME, ordered by position
        sql = (
            f"SELECT COLUMN_NAME FROM {database}.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' "
            f"ORDER BY ORDINAL_POSITION"
        )
        return self._query(sql, col=0)
