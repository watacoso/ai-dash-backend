"""
Snowflake schema introspection service.
All methods are synchronous (snowflake connector is blocking).
The router runs them in a thread-pool executor.
"""
import snowflake.connector
import snowflake.connector.errors


class SnowflakeSchemaService:
    def __init__(self, credentials: dict) -> None:
        self._creds = credentials

    def _connect(self):
        return snowflake.connector.connect(
            account=self._creds["account"],
            user=self._creds["username"],
            private_key=self._creds["private_key"].encode(),
            warehouse=self._creds.get("warehouse"),
            database=self._creds.get("database"),
            schema=self._creds.get("schema"),
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
