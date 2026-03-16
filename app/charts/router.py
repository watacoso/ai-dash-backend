import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.models import User
from app.auth.router import get_current_user
from app.database import get_session
from app.charts.models import Chart
from app.datasets.models import Dataset

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
