"""Validated process configuration.

Secrets are intentionally represented as ``SecretStr`` and must never be logged via
``model_dump()`` without explicit exclusion.
"""

from __future__ import annotations

import os
import socket
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import AnyHttpUrl, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_DEVELOPMENT_SECRET = "development-only-change-me-32-chars"  # noqa: S105
DEFAULT_DEVELOPMENT_ADMIN_KEY = "development-admin-key"


class Environment(StrEnum):
    """Supported deployment environments."""

    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


class Settings(BaseSettings):
    """Application settings loaded from environment variables or ``.env``."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="TRPC_SERVICE_",
        extra="ignore",
        case_sensitive=False,
    )

    env: Environment = Environment.DEVELOPMENT
    service_name: str = "trpc-agent-service"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    database_url: str = "sqlite+aiosqlite:///./data/platform.db"
    redis_url: str = "redis://localhost:6379/0"
    secret_key: SecretStr = SecretStr(DEFAULT_DEVELOPMENT_SECRET)
    admin_api_key: SecretStr = SecretStr(DEFAULT_DEVELOPMENT_ADMIN_KEY)
    secret_env_allowlist: tuple[str, ...] = ()
    public_base_url: AnyHttpUrl = AnyHttpUrl("http://localhost:8000")
    worker_id: str = Field(
        default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}",
        min_length=1,
        max_length=100,
    )
    dispatcher_id: str = Field(
        default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}-delivery",
        min_length=1,
        max_length=128,
    )
    projector_id: str = Field(
        default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}-projection",
        min_length=1,
        max_length=128,
    )

    otel_enabled: bool = False
    otel_endpoint: str = "http://localhost:4317"
    otel_sample_ratio: float = Field(default=1.0, ge=0.0, le=1.0)

    model_provider: Literal["mock", "openai", "openai-compatible"] = "mock"
    model_name: str = "mock-model"
    model_api_key: SecretStr = SecretStr("")
    model_base_url: AnyHttpUrl | None = None
    max_llm_calls: int = Field(default=8, ge=1, le=64)
    max_tool_calls: int = Field(default=12, ge=0, le=128)
    max_iterations: int = Field(default=20, ge=1, le=256)

    lease_seconds: int = Field(default=20, ge=10, le=900)
    heartbeat_seconds: int = Field(default=5, ge=1, le=60)
    worker_max_attempts: int = Field(default=3, ge=1, le=100)
    outbox_max_attempts: int = Field(default=8, ge=1, le=100)
    inbox_payload_limit_bytes: int = Field(default=1_048_576, ge=1_024, le=10_485_760)

    event_store_backend: Literal["local", "sql", "redis"] = "local"
    event_store_path: Path = Path("data/sdk-events")
    event_object_max_bytes: int = Field(default=4_194_304, ge=65_536, le=67_108_864)

    catalog_refresh_seconds: float = Field(default=5.0, gt=0.0, le=300.0)
    idle_backoff_initial_seconds: float = Field(default=0.05, gt=0.0, le=30.0)
    idle_backoff_max_seconds: float = Field(default=2.0, gt=0.0, le=60.0)
    telegram_transport_attempts: int = Field(default=3, ge=1, le=5)

    @model_validator(mode="after")
    def validate_production_safety(self) -> Settings:
        """Refuse known-insecure production defaults."""

        if self.heartbeat_seconds >= self.lease_seconds:
            raise ValueError("heartbeat_seconds must be shorter than lease_seconds")
        if self.idle_backoff_initial_seconds > self.idle_backoff_max_seconds:
            raise ValueError(
                "idle_backoff_initial_seconds must not exceed idle_backoff_max_seconds"
            )
        if self.env is Environment.PRODUCTION:
            secret = self.secret_key.get_secret_value()
            if (
                secret == DEFAULT_DEVELOPMENT_SECRET
                or secret.casefold().startswith("change-")
                or len(secret) < 32
            ):
                raise ValueError("production requires a unique secret key of at least 32 chars")
            admin_key = self.admin_api_key.get_secret_value()
            if admin_key == DEFAULT_DEVELOPMENT_ADMIN_KEY or len(admin_key) < 32:
                raise ValueError("production requires a unique Admin API key of at least 32 chars")
            if self.database_url.startswith("sqlite"):
                raise ValueError("production requires a shared SQL database")
            if self.public_base_url.scheme != "https":
                raise ValueError("production public_base_url must use HTTPS")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return one immutable-by-convention settings snapshot per process."""

    return Settings()
