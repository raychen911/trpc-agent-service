"""Validated process settings shared by all runtime roles."""

from __future__ import annotations

import math
import tempfile
from enum import StrEnum
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlsplit

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from trpc_service.config.secrets import SecretRef

SecretRefValue = Annotated[SecretRef, NoDecode]


class Environment(StrEnum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


class SchedulerVersion(StrEnum):
    """Select exactly one Redis scheduler wire version for this process."""

    V1 = "v1"
    V2 = "v2"


class Role(StrEnum):
    GATEWAY = "gateway"
    ADMIN = "admin"
    WORKER = "worker"
    OUTBOX_DISPATCHER = "outbox-dispatcher"
    CHANNEL_DISPATCHER = "channel-dispatcher"
    POST_TURN_PROJECTOR = "post-turn-projector"
    WECOM_CONNECTOR = "wecom-connector"
    SESSION_RECOVERY = "session-recovery"
    ARTIFACT_GC = "artifact-gc"


# These names are part of the deployment contract.  They are deliberately
# constants instead of settings so an accidentally supplied DSN cannot weaken
# the role split or make a tenant process impersonate the worker account.
RUNTIME_DATABASE_ROLE = "trpc_runtime"
WORKER_DATABASE_ROLE = "trpc_worker"


class ServiceSettings(BaseSettings):
    """Environment-driven settings with secret references, not secret values."""

    model_config = SettingsConfigDict(
        env_prefix="TRPC_SERVICE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    environment: Environment = Environment.DEVELOPMENT
    host: str = "0.0.0.0"  # noqa: S104 - container listener is intentional
    port: int = Field(default=8080, ge=1, le=65535)
    log_level: str = "INFO"
    database_pool_min_size: int = Field(default=2, ge=1)
    database_pool_max_size: int = Field(default=20, ge=2, le=256)
    database_dsn_ref: SecretRefValue = SecretRef(uri="env://TRPC_SERVICE_DATABASE_DSN")
    database_password_ref: SecretRefValue | None = None
    # Only cross-tenant runtime roles receive this reference.  Keeping it
    # optional makes the absence of the worker secret observable and lets the
    # CLI fail closed before opening a pool for a global role.
    worker_database_dsn_ref: SecretRefValue | None = None
    worker_database_password_ref: SecretRefValue | None = None
    redis_url_ref: SecretRefValue = SecretRef(uri="env://TRPC_SERVICE_REDIS_URL")
    redis_password_ref: SecretRefValue | None = None
    session_hmac_ref: SecretRefValue = SecretRef(uri="env://TRPC_SERVICE_SESSION_HMAC_KEY")
    emergency_queue_key_ref: SecretRefValue = SecretRef(
        uri="env://TRPC_SERVICE_EMERGENCY_QUEUE_KEY"
    )
    emergency_queue_key_version: str = Field(default="v1", pattern=r"^[A-Za-z0-9._-]{1,32}$")
    emergency_queue_previous_key_refs: dict[str, SecretRef] = Field(default_factory=dict)
    tenant_secret_root: Path | None = None
    tenant_secret_env_names: tuple[str, ...] = ()
    model_endpoint_hosts: tuple[str, ...] = ()
    feishu_allow_stale_binding_cache: bool = False
    oidc_issuer: str | None = None
    oidc_audience: str | None = None
    oidc_algorithms: tuple[str, ...] = ("RS256",)
    oidc_jwks_ttl_seconds: int = Field(default=300, ge=30, le=86400)
    allow_development_token: bool = True
    development_token_ref: SecretRefValue = SecretRef(uri="env://TRPC_SERVICE_DEVELOPMENT_TOKEN")
    capture_content: bool = False
    callback_body_limit_bytes: int = Field(default=2 * 1024 * 1024, ge=1024)
    lease_seconds: int = Field(default=60, ge=10, le=3600)
    worker_concurrency: int = Field(default=1, ge=1, le=256)
    worker_poll_seconds: float = Field(default=5.0, ge=0.05, le=60)
    recovery_batch_size: int = Field(default=25, ge=1, le=500)
    recovery_poll_seconds: float = Field(default=5.0, ge=0.1, le=300)
    # A published SessionReady event may be replayed again only after this
    # bounded cooldown.  Rolling replay repairs repeated Redis loss while
    # preventing a healthy dispatcher from being reset on every recovery pass.
    recovery_ready_replay_cooldown_seconds: int = Field(default=30, ge=5, le=86_400)
    artifact_gc_batch_size: int = Field(default=100, ge=1, le=1_000)
    artifact_gc_poll_seconds: float = Field(default=60.0, ge=1.0, le=3_600)
    artifact_staging_ttl_seconds: int = Field(default=86_400, ge=60, le=31_536_000)
    runtime_state_dir: Path = Field(
        default_factory=lambda: Path(tempfile.gettempdir()) / "trpc-agent-service"
    )
    workspace_root: Path = Field(
        default_factory=lambda: Path(tempfile.gettempdir()) / "trpc-agent-workspaces"
    )
    heartbeat_interval_seconds: float = Field(default=5.0, ge=1.0, le=60.0)
    liveness_max_age_seconds: float = Field(default=30.0, ge=5.0, le=300.0)
    shutdown_grace_seconds: float = Field(default=50.0, ge=1.0, le=600.0)
    scheduler_version: SchedulerVersion = SchedulerVersion.V2
    redis_stream: str = "trpc:session-ready:v2"
    redis_consumer_group: str = "trpc-session-ready-v2"
    redis_reclaim_after_ms: int = Field(default=60_000, ge=1_000, le=3_600_000)
    # The worker uses a finite XREAD BLOCK (5s by default).  Keep the
    # client socket timeout above the queue's 5s compatibility default so a
    # Redis read cannot be cut off before its BLOCK interval expires.
    redis_socket_connect_timeout_seconds: float = Field(default=3.0, gt=0, le=60)
    redis_socket_timeout_seconds: float = Field(default=10.0, ge=6.0, le=300)
    # ACK happens after the PostgreSQL claim and is best effort.  It must be
    # finite and shorter than the mailbox lease so an unknown ACK outcome
    # cannot outlive the claim's normal execution window.
    redis_ack_timeout_seconds: float = Field(default=3.0, gt=0, le=60)
    otlp_endpoint: str | None = None
    s3_endpoint: str | None = None
    s3_bucket: str = "trpc-artifacts"
    s3_access_key: str | None = None
    s3_secret_key_ref: SecretRefValue | None = None
    media_download_max_bytes: int = Field(
        default=20 * 1024 * 1024,
        ge=1024,
        le=100 * 1024 * 1024,
    )
    media_download_timeout_seconds: float = Field(default=30, ge=1, le=300)
    online_tests_enabled: bool = False
    feishu_send_api_root: str = "https://open.feishu.cn"
    offline_agent_delay_seconds: float = Field(default=0.0, ge=0, le=5)
    fault_injection_enabled: bool = False
    fault_injection_run_id: str | None = None
    fault_injection_run_token_ref: SecretRefValue = SecretRef(
        uri="env://TRPC_SERVICE_FAULT_INJECTION_RUN_TOKEN"
    )

    @model_validator(mode="before")
    @classmethod
    def apply_scheduler_defaults(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        version = values.get("scheduler_version", SchedulerVersion.V2)
        version_value = getattr(version, "value", version)
        if version_value == SchedulerVersion.V1.value:
            defaults = {
                "redis_stream": "trpc:inbound:v1",
                "redis_consumer_group": "trpc-workers-v1",
            }
        elif version_value == SchedulerVersion.V2.value:
            defaults = {
                "redis_stream": "trpc:session-ready:v2",
                "redis_consumer_group": "trpc-session-ready-v2",
            }
        else:
            return values
        normalized = dict(values)
        for name, default in defaults.items():
            normalized.setdefault(name, default)
        return normalized

    @model_validator(mode="after")
    def validate_database_pool(self) -> ServiceSettings:
        if self.database_pool_max_size < self.database_pool_min_size:
            raise ValueError("database pool max size must be greater than or equal to min size")
        return self

    @model_validator(mode="after")
    def validate_worker_database_reference(self) -> ServiceSettings:
        if self.worker_database_password_ref is not None and self.worker_database_dsn_ref is None:
            raise ValueError(
                "worker database password reference requires a worker database DSN reference"
            )
        return self

    @model_validator(mode="after")
    def validate_redis_timeouts(self) -> ServiceSettings:
        for name in (
            "redis_socket_connect_timeout_seconds",
            "redis_socket_timeout_seconds",
            "redis_ack_timeout_seconds",
        ):
            if not math.isfinite(getattr(self, name)):
                raise ValueError(f"{name} must be finite")
        if self.redis_ack_timeout_seconds >= self.lease_seconds:
            raise ValueError("redis ACK timeout must be shorter than the session lease")
        if self.liveness_max_age_seconds <= self.heartbeat_interval_seconds:
            raise ValueError("liveness max age must exceed the heartbeat interval")
        return self

    @model_validator(mode="after")
    def validate_scheduler_transport(self) -> ServiceSettings:
        """Reject known cross-version Redis transport wiring mistakes."""

        v1_stream = "trpc:inbound:v1"
        v1_group = "trpc-workers-v1"
        v2_stream = "trpc:session-ready:v2"
        v2_group = "trpc-session-ready-v2"
        if self.scheduler_version == SchedulerVersion.V1 and (
            self.redis_stream == v2_stream or self.redis_consumer_group == v2_group
        ):
            raise ValueError("v1 scheduler cannot use the v2 Redis stream or consumer group")
        if self.scheduler_version == SchedulerVersion.V2 and (
            self.redis_stream == v1_stream or self.redis_consumer_group == v1_group
        ):
            raise ValueError("v2 scheduler cannot use the v1 Redis stream or consumer group")
        return self

    @model_validator(mode="after")
    def validate_feishu_send_api_root(self) -> ServiceSettings:
        try:
            parsed = urlsplit(self.feishu_send_api_root)
            port = parsed.port
        except ValueError:
            raise ValueError("Feishu send API root is invalid") from None
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path.endswith("/")
            or "//" in parsed.path
            or any(part in {".", ".."} for part in parsed.path.split("/"))
            or port is not None
        ):
            raise ValueError("Feishu send API root must be a canonical HTTPS origin or path")
        if self.feishu_send_api_root != "https://open.feishu.cn" and not self.online_tests_enabled:
            raise ValueError("custom Feishu send API root requires online tests")
        return self

    @model_validator(mode="after")
    def enforce_production_safety(self) -> ServiceSettings:
        if self.emergency_queue_key_version in self.emergency_queue_previous_key_refs:
            raise ValueError("current emergency key version cannot also be a previous version")
        if self.tenant_secret_root is not None and not self.tenant_secret_root.is_absolute():
            raise ValueError("tenant secret root must be an absolute path")
        if self.environment == Environment.PRODUCTION:
            if self.fault_injection_enabled:
                raise ValueError("fault injection is forbidden in production")
            if self.allow_development_token:
                raise ValueError("development authentication must be disabled in production")
            if not self.oidc_issuer or not self.oidc_audience:
                raise ValueError("OIDC issuer and audience are required in production")
            if self.capture_content:
                raise ValueError("content capture is forbidden in production")
            if self.feishu_allow_stale_binding_cache:
                raise ValueError("stale Feishu binding cache is forbidden in production")
            refs = (
                self.database_dsn_ref,
                self.redis_url_ref,
                self.session_hmac_ref,
                self.emergency_queue_key_ref,
                self.development_token_ref,
                self.fault_injection_run_token_ref,
                self.worker_database_dsn_ref,
                self.worker_database_password_ref,
                *self.emergency_queue_previous_key_refs.values(),
            )
            if any(ref is not None and ref.uri.startswith("literal://") for ref in refs):
                raise ValueError("literal secret references are forbidden in production")
        if self.fault_injection_enabled and not (
            self.fault_injection_run_id and self.fault_injection_run_id.strip()
        ):
            raise ValueError("fault injection run_id is required when fault injection is enabled")
        return self


@lru_cache(maxsize=1)
def get_settings() -> ServiceSettings:
    return ServiceSettings()


__all__ = [
    "RUNTIME_DATABASE_ROLE",
    "WORKER_DATABASE_ROLE",
    "Environment",
    "Role",
    "SchedulerVersion",
    "ServiceSettings",
    "get_settings",
]
