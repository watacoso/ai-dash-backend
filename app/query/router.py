"""
POST /query/chat — Claude tool-use loop for SQL generation.
Exposes execute_query_sample as the only tool (not get_schema).
"""
import asyncio
import re
import uuid
from concurrent.futures import ThreadPoolExecutor

import anthropic
import snowflake.connector.errors
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.models import User
from app.auth.router import get_current_user
from app.connections.models import Connection, ConnectionType
from app.database import get_session
from app.explore.router import LogEntry
from app.query.query_service import SnowflakeQueryService

router = APIRouter(prefix="/query", tags=["query"])

_executor = ThreadPoolExecutor(max_workers=4)
_MAX_TOOL_ITERATIONS = 5

_EXECUTE_QUERY_SAMPLE_TOOL = {
    "name": "execute_query_sample",
    "description": (
        "Execute a SQL query against Snowflake on a sampled subset of rows (default 200). "
        "Use this to test and validate a query before presenting it to the user. "
        "Returns columns and rows on success; returns an error string on failure so you can fix it."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {"type": "string", "description": "The SQL query to execute."},
            "limit": {"type": "integer", "description": "Max rows to sample (default 200)."},
        },
        "required": ["sql"],
    },
}


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    snowflake_connection_id: uuid.UUID
    claude_connection_id: uuid.UUID
    messages: list[ChatMessage]


class ChatResponse(BaseModel):
    role: str
    content: str
    query: str | None = None
    logs: list[LogEntry] = []


def _extract_sql(text: str) -> str | None:
    """
    Extract SQL from a fenced code block in the assistant response.
    Priority:
      1. Last ```sql ... ``` or ```SQL ... ``` block
      2. If exactly one untagged ``` ... ``` block exists, use it
      3. Otherwise return None (ambiguous)
    """
    # Look for sql-tagged blocks
    tagged = re.findall(r"```(?:sql|SQL)\s*\n(.*?)```", text, re.DOTALL)
    if tagged:
        return tagged[-1].strip()

    # Fall back to untagged blocks only if there is exactly one
    untagged = re.findall(r"```\s*\n(.*?)```", text, re.DOTALL)
    if len(untagged) == 1:
        return untagged[0].strip()

    return None


def _run_query_tool(svc: SnowflakeQueryService, tool_input: dict) -> tuple[str, bool]:
    """Run execute_query_sample. Returns (result_string, is_error)."""
    sql = tool_input.get("sql", "")
    limit = tool_input.get("limit", 200)
    try:
        result = svc.execute_sample(sql, limit=limit)
        # Format as a compact JSON-like string for Claude
        import json
        return (json.dumps(result), False)
    except (
        snowflake.connector.errors.DatabaseError,
        snowflake.connector.errors.ProgrammingError,
    ) as exc:
        return (f"Error: {exc}", True)


async def _get_active_sf_connection(connection_id: uuid.UUID, session: AsyncSession) -> Connection:
    conn = await session.get(Connection, connection_id)
    if not conn or not conn.is_active or conn.type != ConnectionType.snowflake:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    return conn


async def _get_active_claude_connection(connection_id: uuid.UUID, session: AsyncSession) -> Connection:
    conn = await session.get(Connection, connection_id)
    if not conn or not conn.is_active or conn.type != ConnectionType.claude:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    return conn


@router.post("/chat", response_model=ChatResponse)
async def post_chat(
    body: ChatRequest,
    db_session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
) -> ChatResponse:
    sf_conn = await _get_active_sf_connection(body.snowflake_connection_id, db_session)
    cl_conn = await _get_active_claude_connection(body.claude_connection_id, db_session)

    svc = SnowflakeQueryService(sf_conn.credentials or {})
    cl_creds = cl_conn.credentials or {}
    client = anthropic.Anthropic(api_key=cl_creds["api_key"])
    model = cl_creds["model"]

    messages = [{"role": m.role, "content": m.content} for m in body.messages]
    logs: list[LogEntry] = []

    loop = asyncio.get_event_loop()
    for _ in range(_MAX_TOOL_ITERATIONS):
        response = client.messages.create(
            model=model,
            max_tokens=4096,
            tools=[_EXECUTE_QUERY_SAMPLE_TOOL],
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            text = next(b.text for b in response.content if b.type == "text")
            return ChatResponse(
                role="assistant",
                content=text,
                query=_extract_sql(text),
                logs=logs,
            )

        messages.append({"role": "assistant", "content": response.content})
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                params = ", ".join(f"{k}={v}" for k, v in block.input.items())
                logs.append(LogEntry(level="INFO", message=f"Tool call: {block.name}({params})"))
                result, is_error = await loop.run_in_executor(_executor, _run_query_tool, svc, block.input)
                if is_error:
                    logs.append(LogEntry(level="ERROR", message=result.removeprefix("Error: ")))
                else:
                    logs.append(LogEntry(level="INFO", message=f"Tool result: {result[:120]}"))
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })
        messages.append({"role": "user", "content": tool_results})

    return ChatResponse(
        role="assistant",
        content="I reached the tool-use iterations limit and could not complete the request.",
        logs=logs,
    )
