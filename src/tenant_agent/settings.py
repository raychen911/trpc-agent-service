"""Process-level settings.

Tenant-owned settings never belong here. They are immutable, versioned tenant
configuration documents loaded through the control plane.
"""

from __future__ import annotations

import socket
from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from tenant_agent.resources import default_bootstrap_path


class ServiceRole(StrEnum):
    ALL = "all"
    CHANNEL = "channel-adapter"
    GATEWAY = "gateway"
    WORKER = "worker"
    ADMIN = "admin-api"
    OUTBOX = "outbox"


class Settings(BaseSettings):
    """Node configuration supplied through environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="TAP_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    environment: str = Field(default="development", pattern="^(development|test|production)$")
    service_name: str = "tenant-agent-platform"
    service_role: ServiceRole = ServiceRole.ALL
    node_id: str = Field(default_factory=socket.gethostname, min_length=1, max_length=253)
    host: str = "0.0.0.0"
    port: int = 8080

    control_database_url: SecretStr = SecretStr("sqlite+aiosqlite:///./tenant-agent.db")
    redis_url: SecretStr | None = None
    redis_cluster: bool = False
    bootstrap_config_path: Path | None = Field(default_factory=default_bootstrap_path)
    auto_create_schema: bool = True
    storage_adapter_cache_max_entries: int = Field(default=256, ge=1, le=10_000)
    config_preflight_timeout_seconds: float = Field(default=30.0, gt=0, le=300)

    session_hmac_key: SecretStr = SecretStr("development-only-change-me")
    admin_bearer_token: SecretStr = SecretStr("development-admin-token")
    internal_bearer_token: SecretStr = SecretStr("development-internal-token")
    gateway_internal_url: str | None = None
    secret_file_root: Path = Path("/run/secrets")
    secret_cache_ttl_seconds: float = Field(default=60.0, ge=0, le=3_600)

    broker_mode: str = Field(default="inline", pattern="^(inline|redis-streams)$")
    redis_stream: str = "tap:{inbound}:v1"
    redis_consumer_group: str = "agent-workers"
    broker_global_queue_limit: int = Field(default=100_000, ge=1_000, le=10_000_000)
    broker_tenant_queue_limit: int = Field(default=10_000, ge=100, le=1_000_000)
    worker_poll_ms: int = 2_000
    worker_claim_idle_ms: int = 60_000
    worker_concurrency: int = Field(default=32, ge=1, le=1_024)
    worker_duplicate_defer_seconds: float = Field(default=1.0, ge=0, le=60)
    worker_retry_initial_seconds: float = Field(default=0.5, ge=0, le=60)
    worker_retry_max_seconds: float = Field(default=30.0, ge=0, le=300)
    worker_max_attempts: int = Field(default=8, ge=1, le=100)
    session_lock_timeout_seconds: float = 120.0
    processing_lease_seconds: int = 180
    idempotency_ttl_seconds: int = 7 * 24 * 60 * 60

    model_timeout_seconds: float = 90.0
    runner_cache_max_entries: int = Field(default=256, ge=1)
    runner_cache_ttl_seconds: float = Field(default=300.0, gt=0)
    delivery_timeout_seconds: float = 10.0
    outbox_poll_seconds: float = 1.0
    outbox_max_attempts: int = 8
    outbox_concurrency: int = Field(default=16, ge=1, le=256)
    audit_maintenance_interval_seconds: float = Field(default=3_600.0, gt=0)
    audit_export_timeout_seconds: float = Field(default=10.0, gt=0)
    audit_export_batch_size: int = Field(default=500, ge=1, le=10_000)
    audit_maintenance_max_batches_per_tenant: int = Field(default=20, ge=1, le=1_000)
    operational_retention_max_batches: int = Field(default=1_000, ge=1, le=10_000)
    shutdown_grace_seconds: float = Field(default=120.0, gt=0, le=600)

    otlp_endpoint: str | None = None
    otlp_headers: SecretStr | None = None
    trace_sample_ratio: float = Field(default=1.0, ge=0.0, le=1.0)
    log_level: str = "INFO"
    expose_metrics: bool = True
    enable_browser_ui: bool = True


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
