"""Application configuration loaded from environment variables."""

from functools import lru_cache

from pydantic import Field, PositiveFloat, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """ClickHouse and Base RPC settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        populate_by_name=True,
    )

    host: str = Field(default="localhost", validation_alias="CLICKHOUSE_HOST")
    port: int = Field(default=8123, validation_alias="CLICKHOUSE_PORT")
    database: str = Field(default="chainlens", validation_alias="CLICKHOUSE_DATABASE")
    user: str = Field(default="chainlens", validation_alias="CLICKHOUSE_USER")
    password: str = Field(default="chainlens", validation_alias="CLICKHOUSE_PASSWORD")

    base_rpc_url: str | None = Field(default=None, validation_alias="BASE_RPC_URL")
    rpc_timeout_seconds: PositiveFloat = Field(
        default=20.0, validation_alias="RPC_TIMEOUT_SECONDS"
    )
    rpc_batch_size: PositiveInt = Field(default=20, validation_alias="RPC_BATCH_SIZE")
    reorg_max_depth: PositiveInt = Field(
        default=128, validation_alias="REORG_MAX_DEPTH"
    )


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings instance."""

    return Settings()
