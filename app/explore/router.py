"""
GET /explore/schema — Snowflake schema introspection endpoint.
"""
import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import Annotated

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


async def _get_active_sf_connection(
    connection_id: uuid.UUID,
    session: AsyncSession,
) -> Connection:
    conn = await session.get(Connection, connection_id)
    if not conn or not conn.is_active or conn.type != ConnectionType.snowflake:
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
