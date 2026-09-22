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
    cluster_shared_funder_window_seconds: PositiveInt = Field(
        default=3600,
        validation_alias="CLUSTER_SHARED_FUNDER_WINDOW_SECONDS",
    )
    cluster_max_funder_fanout: PositiveInt = Field(
        default=20,
        validation_alias="CLUSTER_MAX_FUNDER_FANOUT",
    )
    label_propagation_factor: float = Field(
        default=0.95,
        ge=0.0,
        le=1.0,
        validation_alias="LABEL_PROPAGATION_FACTOR",
    )
    drift_max_largest_cluster_relative_increase: float = Field(
        default=1.0,
        gt=0.0,
        validation_alias="DRIFT_MAX_LARGEST_CLUSTER_RELATIVE_INCREASE",
    )
    drift_cluster_count_drop_threshold: float = Field(
        default=0.5,
        gt=0.0,
        le=1.0,
        validation_alias="DRIFT_CLUSTER_COUNT_DROP_THRESHOLD",
    )
    drift_propagated_label_spike_threshold: float = Field(
        default=1.0,
        gt=0.0,
        validation_alias="DRIFT_PROPAGATED_LABEL_SPIKE_THRESHOLD",
    )
    drift_conflict_count_threshold: PositiveInt = Field(
        default=1,
        validation_alias="DRIFT_CONFLICT_COUNT_THRESHOLD",
    )
    drift_seed_coverage_drop_threshold: float = Field(
        default=0.2,
        gt=0.0,
        le=1.0,
        validation_alias="DRIFT_SEED_COVERAGE_DROP_THRESHOLD",
    )


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings instance."""

    return Settings()
