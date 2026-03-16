import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.models import User
from app.auth.router import get_current_user
from app.connections.models import Connection
from app.database import get_session
from app.datasets.models import Dataset

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
