"""Application configuration loaded from environment variables."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """ClickHouse connection settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="CLICKHOUSE_",
        extra="ignore",
    )

    host: str = "localhost"
    port: int = 8123
    database: str = "chainlens"
    user: str = "chainlens"
    password: str = "chainlens"


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings instance."""

    return Settings()

