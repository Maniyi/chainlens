"""Small ClickHouse client factory."""

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from chainlens.config import Settings, get_settings


def create_client(settings: Settings | None = None) -> Client:
    """Create a ClickHouse HTTP client from application settings."""

    config = settings or get_settings()
    return clickhouse_connect.get_client(
        host=config.host,
        port=config.port,
        database=config.database,
        username=config.user,
        password=config.password,
    )

