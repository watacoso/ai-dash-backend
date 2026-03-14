import os

# Set test env vars before any app modules are imported
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://aidash:aidash@localhost:5433/aidash_test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6380/0")
os.environ.setdefault("JWT_SECRET", "test-secret-do-not-use-in-prod")
