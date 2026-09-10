"""Environment-driven service configuration with fail-closed validation."""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppEnvironment(StrEnum):
    TEST = "test"
    DEVELOPMENT = "development"
    PRODUCTION = "production"


class ExecutionBackend(StrEnum):
    INLINE = "inline"
    REDIS = "redis"


_SECRET_SCHEMES = {"env", "file", "vault"}


def validate_secret_ref(value: str | None) -> str | None:
    if value is None:
        return None
    parsed = urlsplit(value)
    if parsed.scheme not in _SECRET_SCHEMES:
        allowed = ", ".join(f"{scheme}://" for scheme in sorted(_SECRET_SCHEMES))
        raise ValueError(f"secret reference must use one of: {allowed}")
    if parsed.scheme in {"env", "vault"} and not parsed.netloc:
        raise ValueError(f"{parsed.scheme} secret reference must include a name")
    if parsed.scheme == "file" and not (parsed.netloc or parsed.path):
        raise ValueError("file secret reference must include a path")
    if parsed.query or parsed.fragment:
        raise ValueError("secret reference cannot contain query or fragment")
    return value


class ServiceSettings(BaseSettings):
    """Validated settings shared by the CLI and the FastAPI application."""

    model_config = SettingsConfigDict(
        env_prefix="TRPC_SERVICE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: AppEnvironment = AppEnvironment.TEST
    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65535)
    log_level: str = "INFO"
    database_url: str = "sqlite+aiosqlite:///./data/trpc_agent_service.db"
    artifact_root: Path = Path("./data/artifacts")
    agent_timeout_seconds: float = Field(default=120.0, gt=0, le=600)

    model_provider: str = "test"
    model_name: str = "test-model"
    model_base_url: str | None = None
    model_api_key_ref: str | None = None

    admin_api_key_ref: str | None = None
    session_hmac_key_ref: str | None = None
    otel_console_exporter: bool = False

    execution_backend: ExecutionBackend = ExecutionBackend.INLINE
    queue_redis_url_ref: str | None = None
    queue_stream: str = "trpc-service:agent-runs"
    queue_consumer_group: str = "trpc-service-workers"
    queue_result_timeout_seconds: float = Field(default=180.0, gt=0, le=900)
    queue_claim_idle_ms: int = Field(default=60_000, ge=1_000)
    queue_max_attempts: int = Field(default=3, ge=1, le=10)

    @field_validator(
        "model_api_key_ref",
        "admin_api_key_ref",
        "session_hmac_key_ref",
        "queue_redis_url_ref",
    )
    @classmethod
    def validate_secret_reference(cls, value: str | None) -> str | None:
        return validate_secret_ref(value)

    @field_validator("model_base_url")
    @classmethod
    def validate_model_base_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        parsed = urlsplit(value)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("model_base_url must be an HTTPS URL")
        return value.rstrip("/")

    @model_validator(mode="after")
    def enforce_runtime_safety(self) -> ServiceSettings:
        if not self.database_url.startswith(("sqlite+aiosqlite:///", "postgresql+asyncpg://")):
            raise ValueError("database_url must use sqlite+aiosqlite or postgresql+asyncpg")

        if self.app_env != AppEnvironment.TEST:
            if self.model_provider.casefold() in {"test", "fake", "mock"}:
                raise ValueError("non-test environments require a real model provider")
            required_refs = {
                "model_api_key_ref": self.model_api_key_ref,
                "admin_api_key_ref": self.admin_api_key_ref,
                "session_hmac_key_ref": self.session_hmac_key_ref,
            }
            missing = [name for name, value in required_refs.items() if value is None]
            if missing:
                raise ValueError(
                    "non-test environments require secret references: " + ", ".join(missing)
                )
        if self.execution_backend == ExecutionBackend.REDIS and self.queue_redis_url_ref is None:
            raise ValueError("queue_redis_url_ref is required for Redis execution")
        return self

    @property
    def sqlite_path(self) -> Path | None:
        prefix = "sqlite+aiosqlite:///"
        if not self.database_url.startswith(prefix):
            return None
        raw_path = self.database_url.removeprefix(prefix)
        if raw_path == ":memory:":
            return None
        return Path(raw_path)


@lru_cache(maxsize=1)
def get_settings() -> ServiceSettings:
    return ServiceSettings()


__all__ = [
    "AppEnvironment",
    "ExecutionBackend",
    "ServiceSettings",
    "get_settings",
    "validate_secret_ref",
]
