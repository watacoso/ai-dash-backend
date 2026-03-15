import asyncio
import uuid
from typing import Any

import anthropic
import snowflake.connector.errors
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.models import User
from app.auth.router import get_current_user, require_role
from app.connections.models import Connection, ConnectionType
from app.connections.probe import run_snowflake_probe, run_claude_probe
from app.database import get_session

router = APIRouter(prefix="/connections", tags=["connections"])


# ── Credential schemas (validated by type) ────────────────────────────────────

class SnowflakeCredentials(BaseModel):
    account: str
    username: str
    private_key: str
    warehouse: str
    database: str
    passphrase: str | None = None
    schema_name: str = ""  # mapped from "schema" key

    model_config = {"populate_by_name": True}

    @classmethod
    def from_dict(cls, d: dict) -> "SnowflakeCredentials":
        d = dict(d)
        if "schema" in d:
            d["schema_name"] = d.pop("schema")
        return cls(**d)


class ClaudeCredentials(BaseModel):
    api_key: str
    model: str


def validate_credentials(type_: ConnectionType, creds: dict) -> dict:
    """Validate and normalise credentials for the given type. Returns plain dict."""
    if type_ == ConnectionType.snowflake:
        # accept both 'schema' and 'schema_name'
        raw = dict(creds)
        if "schema" in raw:
            raw["schema_name"] = raw.pop("schema")
        validated = SnowflakeCredentials(**raw)
        # store back with 'schema' key for consistency
        result = validated.model_dump()
        result["schema"] = result.pop("schema_name")
        return result
    else:
        return ClaudeCredentials(**creds).model_dump()


# ── Pydantic I/O schemas ───────────────────────────────────────────────────────

class ConnectionCreate(BaseModel):
    name: str
    type: ConnectionType
    credentials: dict[str, Any]

    @model_validator(mode="after")
    def check_credentials(self) -> "ConnectionCreate":
        validate_credentials(self.type, self.credentials)
        return self


class ConnectionUpdate(BaseModel):
    name: str | None = None
    credentials: dict[str, Any] | None = None
    is_active: bool | None = None


class ConnectionResponse(BaseModel):
    id: str
    name: str
    type: ConnectionType
    owner_id: str
    is_active: bool


def _to_response(conn: Connection) -> ConnectionResponse:
    return ConnectionResponse(
        id=str(conn.id),
        name=conn.name,
        type=conn.type,
        owner_id=str(conn.owner_id),
        is_active=conn.is_active,
    )


# ── POST /connections ──────────────────────────────────────────────────────────

@router.post("", response_model=ConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(
    body: ConnectionCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(require_role("admin")),
):
    conn = Connection(
        name=body.name,
        type=body.type,
        owner_id=current_user.id,
        credentials=body.credentials,
    )
    session.add(conn)
    await session.commit()
    await session.refresh(conn)
    return _to_response(conn)


# ── GET /connections ───────────────────────────────────────────────────────────

@router.get("", response_model=list[ConnectionResponse])
async def list_connections(
    session: AsyncSession = Depends(get_session),
    _: User = Depends(get_current_user),
):
    result = await session.execute(
        select(Connection).where(Connection.is_active == True)  # noqa: E712
    )
    return [_to_response(c) for c in result.scalars().all()]


# ── GET /connections/{id} ──────────────────────────────────────────────────────

@router.get("/{connection_id}", response_model=ConnectionResponse)
async def get_connection(
    connection_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_role("admin")),
):
    conn = await session.get(Connection, connection_id)
    if not conn:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    return _to_response(conn)


# ── PATCH /connections/{id} ────────────────────────────────────────────────────

@router.patch("/{connection_id}", response_model=ConnectionResponse)
async def update_connection(
    connection_id: uuid.UUID,
    body: ConnectionUpdate,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_role("admin")),
):
    conn = await session.get(Connection, connection_id)
    if not conn:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    if body.name is not None:
        conn.name = body.name
    if body.credentials is not None:
        validate_credentials(conn.type, body.credentials)
        conn.credentials = body.credentials
    if body.is_active is not None:
        conn.is_active = body.is_active
    await session.commit()
    await session.refresh(conn)
    return _to_response(conn)


# ── DELETE /connections/{id} ───────────────────────────────────────────────────

@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_role("admin")),
):
    conn = await session.get(Connection, connection_id)
    if not conn:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    conn.is_active = False
    await session.commit()


# ── POST /connections/{id}/test ────────────────────────────────────────────────

class TestResult(BaseModel):
    ok: bool
    latency_ms: int | None = None
    error: str | None = None


@router.post("/{connection_id}/test", response_model=TestResult, response_model_exclude_none=True)
async def test_connection(
    connection_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    _: User = Depends(require_role("admin")),
):
    conn = await session.get(Connection, connection_id)
    if not conn:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")

    creds = conn.credentials or {}
    try:
        if conn.type == ConnectionType.snowflake:
            result = await run_snowflake_probe(creds)
        else:
            result = await run_claude_probe(creds)
        return TestResult(**result)
    except asyncio.TimeoutError:
        return TestResult(ok=False, error="timeout")
    except snowflake.connector.errors.DatabaseError as exc:
        return TestResult(ok=False, error=str(exc))
    except (anthropic.AuthenticationError, anthropic.NotFoundError) as exc:
        return TestResult(ok=False, error=str(exc))
    except Exception as exc:
        return TestResult(ok=False, error=str(exc))
