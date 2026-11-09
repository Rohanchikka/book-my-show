"""Application settings.

pydantic-settings reads each field from an environment variable of the same name
(case-insensitive), falling back to a .env file, then to the default below.
This is the Python equivalent of Spring's application.yml + @ConfigurationProperties,
with type validation for free: a bad value fails at startup, not at 3am.
"""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Postgres. "+asyncpg" selects the async driver. A sync driver inside async
    # code would block the event loop and stall every other request.
    database_url: str = "postgresql+asyncpg://bms:bms@localhost:5432/booking"
    db_pool_size: int = 10
    db_max_overflow: int = 40

    redis_url: str = "redis://localhost:6379/0"

    # Kafka is optional locally so you can run the service with only Postgres + Redis.
    kafka_enabled: bool = False
    kafka_bootstrap_servers: str = "localhost:29092"

    # Background jobs (outbox poller, expiry sweeper). Tests turn this off and
    # call the jobs directly so they are deterministic.
    scheduler_enabled: bool = True
    outbox_poll_seconds: float = 0.5
    expiry_sweep_seconds: float = 30

    # JWT. auth-service will issue real tokens later; this service only verifies them.
    jwt_secret: str = "dev-secret-change-me"
    jwt_algorithm: str = "HS256"

    hold_ttl_seconds: int = 480  # 8 minutes
    idempotency_stale_seconds: int = 60


@lru_cache
def get_settings() -> Settings:
    return Settings()
