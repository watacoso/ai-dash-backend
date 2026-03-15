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

    def _query(self, sql: str) -> list[str]:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    def list_databases(self) -> list[str]:
        return self._query("SHOW DATABASES")

    def list_schemas(self, database: str) -> list[str]:
        return self._query(f"SHOW SCHEMAS IN DATABASE {database}")

    def list_tables(self, database: str, schema: str) -> list[str]:
        return self._query(f"SHOW TABLES IN SCHEMA {database}.{schema}")

    def list_columns(self, database: str, schema: str, table: str) -> list[str]:
        return self._query(f"SHOW COLUMNS IN TABLE {database}.{schema}.{table}")
