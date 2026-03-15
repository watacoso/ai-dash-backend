"""
GET /explore/schema — Snowflake schema introspection endpoint.
POST /explore/chat  — Claude tool-use loop with get_schema tool.
"""
import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Annotated

import anthropic
import snowflake.connector.errors
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.models import User
from app.auth.router import get_current_user
from app.connections.models import Connection, ConnectionType
from app.database import get_session
from app.explore.schema_service import SnowflakeSchemaService

router = APIRouter(prefix="/explore", tags=["explore"])

_executor = ThreadPoolExecutor(max_workers=4)


class SchemaLevel(str, Enum):
    databases = "databases"
    schemas = "schemas"
    tables = "tables"
    columns = "columns"


class SchemaResponse(BaseModel):
    items: list[str]


_MAX_TOOL_ITERATIONS = 5

_GET_SCHEMA_TOOL = {
    "name": "get_schema",
    "description": "Introspect a Snowflake account. Use level='databases' first, then narrow down.",
    "input_schema": {
        "type": "object",
        "properties": {
            "level": {
                "type": "string",
                "enum": ["databases", "schemas", "tables", "columns"],
                "description": "Introspection level.",
            },
            "database": {"type": "string", "description": "Required for levels schemas/tables/columns."},
            "schema": {"type": "string", "description": "Required for levels tables/columns."},
            "table": {"type": "string", "description": "Required for level columns."},
        },
        "required": ["level"],
    },
}


async def _get_active_sf_connection(
    connection_id: uuid.UUID,
    session: AsyncSession,
) -> Connection:
    conn = await session.get(Connection, connection_id)
    if not conn or not conn.is_active or conn.type != ConnectionType.snowflake:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    return conn


async def _get_active_claude_connection(
    connection_id: uuid.UUID,
    session: AsyncSession,
) -> Connection:
    conn = await session.get(Connection, connection_id)
    if not conn or not conn.is_active or conn.type != ConnectionType.claude:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    return conn


@router.get("/schema", response_model=SchemaResponse)
async def get_schema(
    connection_id: uuid.UUID,
    level: SchemaLevel,
    database: Annotated[str | None, Query()] = None,
    schema: Annotated[str | None, Query()] = None,
    table: Annotated[str | None, Query()] = None,
    db_session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
) -> SchemaResponse:
    # Validate required filter params per level
    if level == SchemaLevel.schemas and not database:
        raise HTTPException(status_code=422, detail="`database` is required for level=schemas")
    if level == SchemaLevel.tables and (not database or not schema):
        raise HTTPException(status_code=422, detail="`database` and `schema` are required for level=tables")
    if level == SchemaLevel.columns and (not database or not schema or not table):
        raise HTTPException(
            status_code=422, detail="`database`, `schema`, and `table` are required for level=columns"
        )

    conn = await _get_active_sf_connection(connection_id, db_session)
    svc = SnowflakeSchemaService(conn.credentials or {})

    loop = asyncio.get_event_loop()
    try:
        if level == SchemaLevel.databases:
            items = await loop.run_in_executor(_executor, svc.list_databases)
        elif level == SchemaLevel.schemas:
            items = await loop.run_in_executor(_executor, svc.list_schemas, database)
        elif level == SchemaLevel.tables:
            items = await loop.run_in_executor(_executor, svc.list_tables, database, schema)
        else:
            items = await loop.run_in_executor(_executor, svc.list_columns, database, schema, table)
    except snowflake.connector.errors.DatabaseError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc))

    return SchemaResponse(items=items)


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    snowflake_connection_id: uuid.UUID
    claude_connection_id: uuid.UUID
    messages: list[ChatMessage]


class LogEntry(BaseModel):
    level: str
    message: str


class ChatResponse(BaseModel):
    role: str
    content: str
    logs: list[LogEntry] = []


def _run_tool(svc: SnowflakeSchemaService, tool_input: dict) -> tuple[str, bool]:
    """Run a schema tool. Returns (result_string, is_error)."""
    level = tool_input.get("level")
    database = tool_input.get("database")
    schema = tool_input.get("schema")
    table = tool_input.get("table")
    try:
        if level == "databases":
            items = svc.list_databases()
        elif level == "schemas":
            items = svc.list_schemas(database)
        elif level == "tables":
            items = svc.list_tables(database, schema)
        else:
            items = svc.list_columns(database, schema, table)
        return (", ".join(items) if items else "(no results)", False)
    except snowflake.connector.errors.DatabaseError as exc:
        return (f"Error: {exc}", True)


@router.post("/chat", response_model=ChatResponse)
async def post_chat(
    body: ChatRequest,
    db_session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
) -> ChatResponse:
    sf_conn = await _get_active_sf_connection(body.snowflake_connection_id, db_session)
    cl_conn = await _get_active_claude_connection(body.claude_connection_id, db_session)

    svc = SnowflakeSchemaService(sf_conn.credentials or {})
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
            tools=[_GET_SCHEMA_TOOL],
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            text = next(b.text for b in response.content if b.type == "text")
            return ChatResponse(role="assistant", content=text, logs=logs)

        # Process tool_use blocks
        messages.append({"role": "assistant", "content": response.content})
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                params = ", ".join(f"{k}={v}" for k, v in block.input.items())
                logs.append(LogEntry(level="INFO", message=f"Tool call: {block.name}({params})"))
                result, is_error = await loop.run_in_executor(_executor, _run_tool, svc, block.input)
                if is_error:
                    logs.append(LogEntry(level="ERROR", message=result.removeprefix("Error: ")))
                else:
                    logs.append(LogEntry(level="INFO", message=f"Tool result: {result}"))
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
