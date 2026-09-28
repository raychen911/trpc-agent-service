"""Application settings loaded from environment variables and local `.env`."""

from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
import os
import re
import socket

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL, make_url

from trpc_service.config.llm import BailianEmbeddingConfig, BailianModelConfig
from trpc_service.config.models import normalize_channel_type
from trpc_service.config.runtime import LeasedWorkerConfig
from trpc_service.config.storage import (
    InMemoryBackendConfig,
    PostgreSQLBackendConfig,
    StorageBackendConfig,
    StorageProfileConfig,
)


class Settings(BaseSettings):
    """Runtime configuration shared by the API, migrations, and scripts."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="TRPC_SERVICE_",
        extra="ignore",
    )

    service_name: str = "trpc-agent-service"
    environment: str = "development"
    database_url: str = "postgresql+asyncpg://trpc@postgres:5432/trpc_agent"
    database_password_file: Path | None = None
    session_cache_url: SecretStr = SecretStr("")
    session_cache_ttl_seconds: int = Field(default=30 * 60, ge=60, le=24 * 60 * 60)
    session_cache_max_events: int = Field(default=40, ge=2, le=200)
    api_prefix: str = "/api/v1"
    http_max_body_bytes: int = Field(default=1024 * 1024, ge=1024, le=10 * 1024 * 1024)
    database_pool_size: int = Field(default=5, ge=1, le=100)
    database_max_overflow: int = Field(default=5, ge=0, le=100)
    database_pool_timeout: float = Field(default=10, gt=0, le=60)
    readiness_timeout_seconds: float = Field(default=2, gt=0, le=30)
    host: str = "127.0.0.1"
    port: int = 8000
    log_level: str = "INFO"
    log_file: Path | None = None
    log_max_bytes: int = Field(default=20 * 1024 * 1024, ge=1024 * 1024)
    log_backup_count: int = Field(default=7, ge=1, le=100)
    telemetry_enabled: bool = False
    otlp_http_endpoint: str = Field(
        default="http://127.0.0.1:4318",
        pattern=r"^https?://[^\s]+$",
    )
    auto_create_schema: bool = False
    node_id: str = ""
    local_worker_nodes: int = Field(default=2, ge=1, le=64)
    worker_concurrency_per_node: int = Field(default=1, ge=1, le=256)
    worker_scaler_mode: str = Field(
        default="local_process",
        pattern=r"^(local_process|kubernetes)$",
    )
    worker_pool_reconcile_interval_seconds: float = Field(default=1, gt=0, le=60)
    worker_run_dir: Path = Path(".run/workers")
    worker_log_dir: Path = Path(".run")
    kubernetes_api_url: str = Field(
        default="https://kubernetes.default.svc",
        pattern=r"^https://[^\s]+$",
    )
    kubernetes_namespace_file: Path = Path(
        "/var/run/secrets/kubernetes.io/serviceaccount/namespace")
    kubernetes_token_file: Path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
    kubernetes_ca_file: Path = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
    kubernetes_worker_deployment: str = Field(
        default="agent-worker",
        pattern=r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$",
    )
    worker_concurrency: int = Field(default=1, ge=0, le=256)
    worker_lease_seconds: int = Field(default=180, ge=3, le=3600)
    worker_poll_interval_seconds: float = Field(default=0.25, gt=0, le=30)
    worker_retry_base_seconds: float = Field(default=5, ge=0, le=300)
    worker_retry_max_seconds: float = Field(default=300, gt=0, le=3600)
    retry_jitter_ratio: float = Field(default=0.2, ge=0, le=0.5)
    worker_max_attempts: int = Field(default=5, ge=1, le=100)
    delivery_concurrency: int = Field(default=1, ge=0, le=64)
    delivery_lease_seconds: int = Field(default=60, ge=3, le=3600)
    delivery_poll_interval_seconds: float = Field(default=0.5, gt=0, le=30)
    delivery_retry_base_seconds: float = Field(default=5, ge=0, le=300)
    delivery_retry_max_seconds: float = Field(default=900, gt=0, le=7200)
    delivery_max_attempts: int = Field(default=8, ge=1, le=100)
    delivery_channel_types: str = ""
    runtime_role: str = Field(default="api", pattern=r"^(api|worker|channel|supervisor)$")
    channel_reconcile_interval_seconds: float = Field(default=5, gt=0, le=300)
    node_heartbeat_interval_seconds: float = Field(default=5, gt=0, le=60)
    node_stale_after_seconds: int = Field(default=20, ge=2, le=600)
    admin_bootstrap_token: SecretStr = SecretStr("")
    admin_bootstrap_token_file: Path | None = None
    tenant_secret_master_key: SecretStr = SecretStr("")
    tenant_secret_master_key_file: Path | None = None
    management_session_ttl_seconds: int = Field(default=8 * 60 * 60, ge=300, le=7 * 24 * 60 * 60)
    management_login_max_attempts: int = Field(default=5, ge=1, le=20)
    management_login_concurrency: int = Field(default=2, ge=1, le=16)
    management_login_per_minute: int = Field(default=20, ge=5, le=1000)
    management_login_lock_seconds: int = Field(default=15 * 60, ge=30, le=24 * 60 * 60)
    secure_cookies: bool = False
    agent_instruction: str = "你是一个可靠、简洁的企业级智能助手。"
    model_timeout_seconds: float = Field(default=120, gt=0, le=1800)
    mcp_private_allowed_hosts: str = ""
    approval_ttl_seconds: int = Field(default=600, ge=30, le=86_400)
    workspace_root: Path = Path("data/workspaces")
    workspace_retention_seconds: int = Field(
        default=7 * 24 * 60 * 60,
        ge=60 * 60,
        le=365 * 24 * 60 * 60,
    )
    workspace_cleanup_interval_seconds: int = Field(
        default=5 * 60,
        ge=0,
        le=24 * 60 * 60,
    )
    llm: BailianModelConfig = BailianModelConfig()
    embedding: BailianEmbeddingConfig = BailianEmbeddingConfig()
    # This exact alias allows the standard DASHSCOPE_API_KEY name to coexist
    # with the TRPC_SERVICE_ prefix used by all non-secret application settings.
    dashscope_api_key: SecretStr = Field(
        default=SecretStr(""),
        validation_alias="DASHSCOPE_API_KEY",
    )
    storage_backends: dict[str, StorageBackendConfig] = {
        "inmemory": InMemoryBackendConfig(),
    }
    storage_profile: StorageProfileConfig = StorageProfileConfig(
        session="inmemory",
        memory="inmemory",
        summary="inmemory",
        knowledge="inmemory",
        artifact="inmemory",
        audit="inmemory",
    )

    @field_validator("api_prefix")
    @classmethod
    def validate_api_prefix(cls, value: str) -> str:
        if not re.fullmatch(r"/(?:[a-zA-Z0-9_-]+/)*[a-zA-Z0-9_-]+", value):
            raise ValueError("API prefix must be an absolute path without a trailing slash")
        return value

    @model_validator(mode="after")
    def validate_production(self) -> "Settings":
        if self.environment.casefold() == "production":
            if not self.secure_cookies or self.auto_create_schema:
                raise ValueError("production requires secure cookies and migrated schemas")
            self.validate_execution_backends({})
        return self

    def validate_execution_backends(self, configured: Mapping[str, object]) -> None:
        """Keep transaction facts visible to the primary SQL delivery queue."""

        # Isolated SQLite fixtures intentionally exercise in-memory adapters.
        primary = make_url(self.database_url)
        if primary.get_backend_name() == "sqlite" and self.environment.casefold() != "production":
            return
        profile = StorageProfileConfig.model_validate(
            dict(configured) or self.storage_profile.model_dump())
        backend = self.storage_backends.get(profile.session)
        if not isinstance(backend, PostgreSQLBackendConfig):
            raise ValueError("Session/Inbox/Outbox must use the primary PostgreSQL backend")
        target = make_url(backend.url)

        def identity(url: URL) -> tuple[object, ...]:
            return (url.get_backend_name(), (url.host or "").casefold(), url.port
                    or 5432, url.database, url.query.get("server_settings"))

        if identity(primary) != identity(target):
            raise ValueError("Session/Inbox/Outbox must share the primary database")

    @property
    def resolved_database_url(self) -> URL:
        """Return the database URL after injecting an optional file-based password."""

        url = make_url(self.database_url)
        if self.database_password_file is None:
            return url
        password = self.database_password_file.read_text(encoding="utf-8").strip()
        if password == "":
            raise ValueError("database password file is empty")
        return url.set(password=password)

    @property
    def resolved_session_cache_url(self) -> str | None:
        """Return an optional Redis URL without exposing it in settings output."""

        value = self.session_cache_url.get_secret_value().strip()
        if not value:
            return None
        parsed = make_url(value)
        if parsed.drivername not in {"redis", "rediss"}:
            raise ValueError("Session cache URL must use redis:// or rediss://")
        if parsed.host is None:
            raise ValueError("Session cache URL must include a host")
        return value

    @property
    def resolved_admin_bootstrap_token(self) -> str:
        """Resolve the initial platform administrator token without logging it."""

        direct = self.admin_bootstrap_token.get_secret_value().strip()
        if direct:
            return direct
        if self.admin_bootstrap_token_file is None:
            return ""
        token = self.admin_bootstrap_token_file.read_text(encoding="utf-8").strip()
        if token == "":
            raise ValueError("admin bootstrap token file is empty")
        return token

    @property
    def resolved_tenant_secret_master_key(self) -> bytes | None:
        """Resolve the platform-owned AES-256 key used by the local SecretStore."""

        direct = self.tenant_secret_master_key.get_secret_value().strip()
        if not direct and self.tenant_secret_master_key_file is not None:
            direct = self.tenant_secret_master_key_file.read_text(encoding="utf-8").strip()
        if not direct:
            return None
        if re.fullmatch(r"[0-9a-fA-F]{64}", direct) is None:
            raise ValueError("tenant SecretStore master key must be 32 bytes encoded as hex")
        return bytes.fromhex(direct)

    @property
    def resolved_node_id(self) -> str:
        """Return an operator-supplied ID or a process-unique local fallback."""

        configured = self.node_id.strip()
        if configured:
            return configured
        return f"{socket.gethostname()}-{os.getpid()}"

    @property
    def agent_worker_runtime(self) -> LeasedWorkerConfig:
        """Group the Agent Worker lease and retry policy for composition."""

        return LeasedWorkerConfig(
            lease_seconds=self.worker_lease_seconds,
            poll_interval_seconds=self.worker_poll_interval_seconds,
            retry_base_seconds=self.worker_retry_base_seconds,
            retry_max_seconds=self.worker_retry_max_seconds,
            retry_jitter_ratio=self.retry_jitter_ratio,
            max_attempts=self.worker_max_attempts,
        )

    @property
    def delivery_worker_runtime(self) -> LeasedWorkerConfig:
        """Group the Delivery Worker lease and retry policy for composition."""

        return LeasedWorkerConfig(
            lease_seconds=self.delivery_lease_seconds,
            poll_interval_seconds=self.delivery_poll_interval_seconds,
            retry_base_seconds=self.delivery_retry_base_seconds,
            retry_max_seconds=self.delivery_retry_max_seconds,
            retry_jitter_ratio=self.retry_jitter_ratio,
            max_attempts=self.delivery_max_attempts,
        )

    @property
    def resolved_delivery_channel_types(self) -> tuple[str, ...]:
        """Keep each runtime role from claiming another process's transports."""

        configured = tuple(
            dict.fromkeys(
                normalize_channel_type(value) for value in self.delivery_channel_types.split(",")
                if value.strip()))
        if configured:
            return configured
        if self.runtime_role == "channel":
            return ("wecom", "feishu")
        # Real IM transports are owned by the dedicated Channel Runtime.
        # API and Agent Worker processes must not claim delivery without an
        # explicitly configured provider transport.
        return ()

    @property
    def resolved_mcp_private_allowed_hosts(self) -> tuple[str, ...]:
        """Return operator-approved private MCP host names without wildcard expansion."""

        return tuple(
            dict.fromkeys(host.strip().casefold()
                          for host in self.mcp_private_allowed_hosts.split(",") if host.strip()))


@lru_cache
def get_settings() -> Settings:
    """Build settings once per process so all components see one snapshot."""

    return Settings()
