"""
Connectivity probes for Snowflake and Claude connections.
Both connectors are synchronous; we run them in a thread-pool executor
and wrap with asyncio.wait_for to enforce the timeout.
"""
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import anthropic
import snowflake.connector
import snowflake.connector.errors

_TIMEOUT_SECONDS = 10
_executor = ThreadPoolExecutor(max_workers=4)


def _snowflake_sync(credentials: dict) -> dict[str, Any]:
    """Blocking Snowflake probe — runs in thread pool."""
    start = time.monotonic()
    conn = snowflake.connector.connect(
        account=credentials["account"],
        user=credentials["username"],
        private_key=credentials["private_key"].encode(),
        warehouse=credentials.get("warehouse"),
        database=credentials.get("database"),
        schema=credentials.get("schema"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT CURRENT_TIMESTAMP()")
    finally:
        conn.close()
    latency_ms = int((time.monotonic() - start) * 1000)
    return {"ok": True, "latency_ms": latency_ms}


def _claude_sync(credentials: dict) -> dict[str, Any]:
    """Blocking Claude probe — runs in thread pool."""
    client = anthropic.Anthropic(api_key=credentials["api_key"])
    client.messages.create(
        model=credentials["model"],
        max_tokens=1,
        messages=[{"role": "user", "content": "ping"}],
    )
    return {"ok": True}


async def run_snowflake_probe(credentials: dict) -> dict[str, Any]:
    loop = asyncio.get_event_loop()
    return await asyncio.wait_for(
        loop.run_in_executor(_executor, _snowflake_sync, credentials),
        timeout=_TIMEOUT_SECONDS,
    )


async def run_claude_probe(credentials: dict) -> dict[str, Any]:
    loop = asyncio.get_event_loop()
    return await asyncio.wait_for(
        loop.run_in_executor(_executor, _claude_sync, credentials),
        timeout=_TIMEOUT_SECONDS,
    )
