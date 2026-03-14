import redis.asyncio as aioredis

from app.config import settings

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
