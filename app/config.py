from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str = "postgresql+asyncpg://aidash:aidash@localhost:5432/aidash"
    redis_url: str = "redis://localhost:6379/0"
    jwt_secret: str = "test-secret"
    jwt_algorithm: str = "HS256"
    jwt_expiry_seconds: int = 3600


settings = Settings()
