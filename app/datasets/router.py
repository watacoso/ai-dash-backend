import asyncio
import re
import time
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import anthropic
import snowflake.connector.errors
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.models import User
from app.auth.router import get_current_user
from app.connections.models import Connection
from app.database import get_session
from app.datasets.models import Dataset
from app.query.query_service import SnowflakeQueryService

_executor = ThreadPoolExecutor(max_workers=4)

router = APIRouter(prefix="/datasets", tags=["datasets"])


# ── Pydantic I/O schemas ───────────────────────────────────────────────────────

class DatasetCreate(BaseModel):
    name: str
    sql: str
    snowflake_connection_id: uuid.UUID
    description: str = ""
    claude_connection_id: uuid.UUID | None = None


class DatasetUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    sql: str | None = None
    snowflake_connection_id: uuid.UUID | None = None
    claude_connection_id: uuid.UUID | None = None
    # models_used intentionally excluded — managed by AI chat (TKT-0032)


class DatasetResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    description: str
    sql: str
    snowflake_connection_id: uuid.UUID
    claude_connection_id: uuid.UUID | None
    models_used: list[str]
    created_by: uuid.UUID
    created_at: datetime
    updated_at: datetime


async def _get_or_404(session: AsyncSession, dataset_id: uuid.UUID) -> Dataset:
    ds = await session.get(Dataset, dataset_id)
    if not ds:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")
    return ds


async def _require_connection(session: AsyncSession, connection_id: uuid.UUID) -> None:
    conn = await session.get(Connection, connection_id)
    if not conn:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")


# ── GET /datasets ──────────────────────────────────────────────────────────────

@router.get("", response_model=list[DatasetResponse])
async def list_datasets(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    result = await session.execute(select(Dataset))
    return result.scalars().all()


# ── POST /datasets ─────────────────────────────────────────────────────────────

@router.post("", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_dataset(
    body: DatasetCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    await _require_connection(session, body.snowflake_connection_id)
    if body.claude_connection_id:
        await _require_connection(session, body.claude_connection_id)

    ds = Dataset(
        name=body.name,
        description=body.description,
        sql=body.sql,
        snowflake_connection_id=body.snowflake_connection_id,
        claude_connection_id=body.claude_connection_id,
        models_used=[],
        created_by=current_user.id,
    )
    session.add(ds)
    await session.commit()
    await session.refresh(ds)
    return ds


# ── GET /datasets/{id} ─────────────────────────────────────────────────────────

@router.get("/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(
    dataset_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    return await _get_or_404(session, dataset_id)


# ── PATCH /datasets/{id} ───────────────────────────────────────────────────────

@router.patch("/{dataset_id}", response_model=DatasetResponse)
async def update_dataset(
    dataset_id: uuid.UUID,
    body: DatasetUpdate,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    ds = await _get_or_404(session, dataset_id)

    if body.name is not None:
        ds.name = body.name
    if body.description is not None:
        ds.description = body.description
    if body.sql is not None:
        ds.sql = body.sql
    if body.snowflake_connection_id is not None:
        await _require_connection(session, body.snowflake_connection_id)
        ds.snowflake_connection_id = body.snowflake_connection_id
    if body.claude_connection_id is not None:
        await _require_connection(session, body.claude_connection_id)
        ds.claude_connection_id = body.claude_connection_id

    await session.commit()
    await session.refresh(ds)
    return ds


# ── DELETE /datasets/{id} ──────────────────────────────────────────────────────

@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset(
    dataset_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    ds = await _get_or_404(session, dataset_id)
    await session.delete(ds)
    await session.commit()


# ── Run helpers ────────────────────────────────────────────────────────────────

class RunPayload(BaseModel):
    sql: str
    snowflake_connection_id: uuid.UUID


class RunResponse(BaseModel):
    columns: list[str]
    rows: list[list]
    row_count: int
    duration_ms: int
    executed_at: str


class RunErrorResponse(BaseModel):
    error: str


async def _execute_run(
    session: AsyncSession, connection_id: uuid.UUID, sql: str
) -> RunResponse:
    import asyncio
    conn = await session.get(Connection, connection_id)
    if not conn:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    svc = SnowflakeQueryService(conn.credentials or {})
    executed_at = datetime.now(timezone.utc).isoformat()
    start = time.monotonic()
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(_executor, svc.execute_sample, sql)
    except (
        snowflake.connector.errors.DatabaseError,
        snowflake.connector.errors.ProgrammingError,
    ) as exc:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"error": str(exc)},
        )
    duration_ms = int((time.monotonic() - start) * 1000)
    return RunResponse(
        columns=result["columns"],
        rows=result["rows"],
        row_count=len(result["rows"]),
        duration_ms=duration_ms,
        executed_at=executed_at,
    )


# ── POST /datasets/run (ad-hoc) ────────────────────────────────────────────────
# Must be registered before /{dataset_id}/run to avoid "run" matching as a UUID.

@router.post("/run")
async def run_adhoc(
    body: RunPayload,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    return await _execute_run(session, body.snowflake_connection_id, body.sql)


# ── POST /datasets/{id}/run (saved dataset) ────────────────────────────────────

@router.post("/{dataset_id}/run")
async def run_saved(
    dataset_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    ds = await _get_or_404(session, dataset_id)
    return await _execute_run(session, ds.snowflake_connection_id, ds.sql)


# ── Chat helpers ──────────────────────────────────────────────────────────────

_MAX_TOOL_ITERATIONS = 5

_EXECUTE_QUERY_SAMPLE_TOOL = {
    "name": "execute_query_sample",
    "description": (
        "Execute a SQL query against Snowflake on a sampled subset of rows (default 200). "
        "Use this to test and validate a query before presenting it to the user."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {"type": "string"},
            "limit": {"type": "integer"},
        },
        "required": ["sql"],
    },
}


def _extract_sql(text: str) -> str | None:
    tagged = re.findall(r"```(?:sql|SQL)\s*\n(.*?)```", text, re.DOTALL)
    if tagged:
        return tagged[-1].strip()
    untagged = re.findall(r"```\s*\n(.*?)```", text, re.DOTALL)
    if len(untagged) == 1:
        return untagged[0].strip()
    return None


def _extract_name(text: str) -> str | None:
    m = re.search(r"\*\*Name:\*\*\s*(.+)", text)
    return m.group(1).strip() if m else None


def _extract_description(text: str) -> str | None:
    m = re.search(r"\*\*Description:\*\*\s*(.+)", text)
    return m.group(1).strip() if m else None


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    snowflake_connection_id: uuid.UUID
    claude_connection_id: uuid.UUID
    messages: list[ChatMessage]
    sql: str = ""
    name: str = ""
    description: str = ""


class ChatResponse(BaseModel):
    role: str
    content: str
    sql_update: str | None = None
    name_update: str | None = None
    description_update: str | None = None


async def _run_dataset_chat(body: ChatRequest, session: AsyncSession) -> ChatResponse:
    sf_conn = await session.get(Connection, body.snowflake_connection_id)
    if not sf_conn or not sf_conn.is_active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    cl_conn = await session.get(Connection, body.claude_connection_id)
    if not cl_conn or not cl_conn.is_active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")

    svc = SnowflakeQueryService(sf_conn.credentials or {})
    cl_creds = cl_conn.credentials or {}
    client = anthropic.Anthropic(api_key=cl_creds["api_key"])
    model = cl_creds["model"]

    messages = [{"role": m.role, "content": m.content} for m in body.messages]
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
                sql_update=_extract_sql(text),
                name_update=_extract_name(text),
                description_update=_extract_description(text),
            )
        messages.append({"role": "assistant", "content": response.content})
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                import json as _json
                try:
                    result = await loop.run_in_executor(
                        _executor, svc.execute_sample, block.input.get("sql", ""),
                    )
                    content = _json.dumps(result)
                except Exception as exc:
                    content = f"Error: {exc}"
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": content,
                })
        messages.append({"role": "user", "content": tool_results})

    return ChatResponse(
        role="assistant",
        content="I reached the tool-use iterations limit and could not complete the request.",
    )


# ── POST /datasets/chat (ad-hoc, no dataset id) ───────────────────────────────
# Must be registered before /{dataset_id}/chat to avoid "chat" matching as UUID.

@router.post("/chat", response_model=ChatResponse)
async def chat_adhoc(
    body: ChatRequest,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
) -> ChatResponse:
    return await _run_dataset_chat(body, session)


# ── POST /datasets/{id}/chat (saved dataset — updates models_used) ────────────

@router.post("/{dataset_id}/chat", response_model=ChatResponse)
async def chat_saved(
    dataset_id: uuid.UUID,
    body: ChatRequest,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
) -> ChatResponse:
    ds = await _get_or_404(session, dataset_id)
    cl_conn = await session.get(Connection, body.claude_connection_id)
    if not cl_conn or not cl_conn.is_active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")

    result = await _run_dataset_chat(body, session)

    # Append model name to models_used (server-side, deduped)
    model = (cl_conn.credentials or {}).get("model", "")
    if model and model not in ds.models_used:
        ds.models_used = [*ds.models_used, model]
        await session.commit()

    return result
