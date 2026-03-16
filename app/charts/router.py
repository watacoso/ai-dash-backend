import asyncio
import json as _json
import re
import uuid
from datetime import datetime, timezone
from typing import Any

import anthropic
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.models import User
from app.auth.router import get_current_user
from app.connections.models import Connection
from app.database import get_session
from app.charts.models import Chart
from app.charts.d3_validator import validate_d3, _VALIDATE_D3_TOOL
from app.charts.d3_renderer import render_chart, _RENDER_CHART_TOOL
from app.datasets.models import Dataset

_MAX_TOOL_ITERATIONS = 8

router = APIRouter(prefix="/charts", tags=["charts"])


# ── Pydantic schemas ───────────────────────────────────────────────────────────

class ChartCreate(BaseModel):
    name: str
    datasource_id: uuid.UUID


class ChartUpdate(BaseModel):
    name: str | None = None
    d3_code: str | None = None       # appends a new version entry
    accepted_version: int | None = None  # sets accepted=True on that index


class ChartResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    datasource_id: uuid.UUID
    versions: list[Any]
    created_by: uuid.UUID
    created_at: datetime
    updated_at: datetime


# ── Helpers ────────────────────────────────────────────────────────────────────

async def _get_or_404(session: AsyncSession, chart_id: uuid.UUID) -> Chart:
    chart = await session.get(Chart, chart_id)
    if not chart:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Chart not found")
    return chart


async def _require_dataset(session: AsyncSession, datasource_id: uuid.UUID) -> None:
    ds = await session.get(Dataset, datasource_id)
    if not ds:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")


# ── GET /charts ────────────────────────────────────────────────────────────────

@router.get("", response_model=list[ChartResponse])
async def list_charts(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    result = await session.execute(select(Chart))
    return result.scalars().all()


# ── POST /charts ───────────────────────────────────────────────────────────────

@router.post("", response_model=ChartResponse, status_code=status.HTTP_201_CREATED)
async def create_chart(
    body: ChartCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    await _require_dataset(session, body.datasource_id)
    chart = Chart(
        name=body.name,
        datasource_id=body.datasource_id,
        versions=[],
        created_by=current_user.id,
    )
    session.add(chart)
    await session.commit()
    await session.refresh(chart)
    return chart


# ── GET /charts/{id} ──────────────────────────────────────────────────────────

@router.get("/{chart_id}", response_model=ChartResponse)
async def get_chart(
    chart_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    return await _get_or_404(session, chart_id)


# ── PATCH /charts/{id} ────────────────────────────────────────────────────────

@router.patch("/{chart_id}", response_model=ChartResponse)
async def update_chart(
    chart_id: uuid.UUID,
    body: ChartUpdate,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    chart = await _get_or_404(session, chart_id)

    if body.name is not None:
        chart.name = body.name

    if body.d3_code is not None:
        new_version = {
            "version": len(chart.versions),
            "d3_code": body.d3_code,
            "accepted": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        chart.versions = [*chart.versions, new_version]

    if body.accepted_version is not None:
        idx = body.accepted_version
        if idx < 0 or idx >= len(chart.versions):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Version {idx} does not exist",
            )
        chart.versions = [
            {**v, "accepted": (i == idx)}
            for i, v in enumerate(chart.versions)
        ]

    await session.commit()
    await session.refresh(chart)
    return chart


# ── Chat schemas ──────────────────────────────────────────────────────────────

class ChatMessage(BaseModel):
    role: str
    content: str


class ChartChatRequest(BaseModel):
    claude_connection_id: uuid.UUID
    datasource_id: uuid.UUID
    messages: list[ChatMessage]
    d3_code: str = ""


class ChartChatResponse(BaseModel):
    role: str
    content: str
    d3_code_update: str | None = None


# ── Helpers ────────────────────────────────────────────────────────────────────

def _extract_d3(text: str) -> str | None:
    matches = re.findall(r"```(?:d3|js)\s*\n(.*?)```", text, re.DOTALL)
    if matches:
        return matches[-1].strip()
    return None


async def _run_chart_chat(body: ChartChatRequest, session: AsyncSession) -> ChartChatResponse:
    cl_conn = await session.get(Connection, body.claude_connection_id)
    if not cl_conn or not cl_conn.is_active:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")

    cl_creds = cl_conn.credentials or {}
    client = anthropic.Anthropic(api_key=cl_creds["api_key"])
    model = cl_creds["model"]

    messages = [{"role": m.role, "content": m.content} for m in body.messages]

    for _ in range(_MAX_TOOL_ITERATIONS):
        response = client.messages.create(
            model=model,
            max_tokens=4096,
            tools=[_VALIDATE_D3_TOOL, _RENDER_CHART_TOOL],
            messages=messages,
        )
        if response.stop_reason == "end_turn":
            text = next(b.text for b in response.content if b.type == "text")
            return ChartChatResponse(
                role="assistant",
                content=text,
                d3_code_update=_extract_d3(text),
            )

        messages.append({"role": "assistant", "content": response.content})
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                try:
                    if block.name == "validate_d3":
                        result = validate_d3(block.input.get("code", ""))
                        content = _json.dumps(result)
                    elif block.name == "render_chart":
                        result = await render_chart(block.input.get("d3_code", ""))
                        content = _json.dumps(result)
                    else:
                        content = _json.dumps({"error": f"Unknown tool: {block.name}"})
                except Exception as exc:
                    content = f"Error: {exc}"
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": content,
                })
        messages.append({"role": "user", "content": tool_results})

    return ChartChatResponse(
        role="assistant",
        content="I reached the tool-use iterations limit and could not complete the request.",
    )


# ── POST /charts/chat (ad-hoc) ─────────────────────────────────────────────────
# Must be registered before /{chart_id}/chat to avoid "chat" matching as UUID.

@router.post("/chat", response_model=ChartChatResponse)
async def chart_chat_adhoc(
    body: ChartChatRequest,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
) -> ChartChatResponse:
    return await _run_chart_chat(body, session)


# ── POST /charts/{id}/chat (saved chart — appends version) ────────────────────

@router.post("/{chart_id}/chat", response_model=ChartChatResponse)
async def chart_chat_saved(
    chart_id: uuid.UUID,
    body: ChartChatRequest,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
) -> ChartChatResponse:
    chart = await _get_or_404(session, chart_id)

    result = await _run_chart_chat(body, session)

    if result.d3_code_update:
        new_version = {
            "version": len(chart.versions),
            "d3_code": result.d3_code_update,
            "accepted": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        chart.versions = [*chart.versions, new_version]
        await session.commit()

    return result


# ── DELETE /charts/{id} ───────────────────────────────────────────────────────

@router.delete("/{chart_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_chart(
    chart_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    chart = await _get_or_404(session, chart_id)
    await session.delete(chart)
    await session.commit()
