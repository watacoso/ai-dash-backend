"""
Snowflake query execution service.
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
    pem = pem.replace("\\n", "\n").strip()
    password = passphrase.encode() if passphrase else None
    key = load_pem_private_key(pem.encode(), password=password, backend=default_backend())
    return key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )


class SnowflakeQueryService:
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

    def execute_sample(self, sql: str, limit: int = 200) -> dict:
        """
        Execute a SQL query wrapped in a subquery with a LIMIT clause.
        Returns {"columns": [...], "rows": [[...], ...]}.
        Raises snowflake.connector.errors on failure — the caller is responsible
        for catching and formatting the error for Claude.
        """
        wrapped = f"SELECT * FROM ({sql}) AS _sample LIMIT {limit}"
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(wrapped)
                columns = [col[0] for col in cur.description]
                rows = [list(row) for row in cur.fetchall()]
                return {"columns": columns, "rows": rows}
        finally:
            conn.close()
