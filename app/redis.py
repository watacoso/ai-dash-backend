import hashlib

import redis.asyncio as aioredis

from app.config import settings

_INVITE_TTL_SECONDS = 48 * 3600

_client: aioredis.Redis | None = None


def get_redis() -> aioredis.Redis:
    global _client
    if _client is None:
        _client = aioredis.from_url(settings.redis_url)
    return _client


async def add_to_blocklist(token: str) -> None:
    r = get_redis()
    await r.setex(f"blocklist:{token}", settings.jwt_expiry_seconds, "1")


async def is_blocklisted(token: str) -> bool:
    r = get_redis()
    return await r.exists(f"blocklist:{token}") == 1


async def mark_invite_used(token: str) -> None:
    r = get_redis()
    key = f"used_invite:{hashlib.sha256(token.encode()).hexdigest()}"
    await r.setex(key, _INVITE_TTL_SECONDS, "1")


async def is_invite_used(token: str) -> bool:
    r = get_redis()
    key = f"used_invite:{hashlib.sha256(token.encode()).hexdigest()}"
    return await r.exists(key) == 1
