import os
import socket
from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from trpc_service.version import __version__


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables or a local .env file."""

    model_config = SettingsConfigDict(
        env_prefix="TRPC_SERVICE_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "trpc-agent-service"
    app_version: str = __version__
    environment: Literal["development", "test", "staging", "production"] = "development"
    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1, le=65535)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    debug: bool = False
    database_url: str = "sqlite+pysqlite:///./data/trpc_service.db"
    auto_create_schema: bool = True
    conversation_backend: Literal["inmemory", "sql"] = "sql"
    coordination_backend: Literal["inmemory", "redis"] = "inmemory"
    redis_url: SecretStr | None = None
    artifact_backend: Literal["local", "minio"] = "local"
    artifact_root: str = "./data/artifacts"
    minio_endpoint: str | None = None
    minio_access_key: SecretStr | None = None
    minio_secret_key: SecretStr | None = None
    minio_bucket: str = "trpc-agent-artifacts"
    minio_secure: bool = False
    outbox_worker_enabled: bool = True
    outbox_poll_seconds: float = Field(default=0.5, gt=0, le=60)
    node_id: str = Field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    node_base_url: str = "http://127.0.0.1:8000"
    public_base_url: str = "http://127.0.0.1:8000"
    node_capacity: int = Field(default=100, ge=1, le=100_000)
    node_ttl_seconds: float = Field(default=15, gt=2, le=300)
    node_heartbeat_seconds: float = Field(default=5, gt=0.5, le=60)
    gateway_internal_secret: SecretStr = SecretStr("development-only-change-me")
    gateway_forward_timeout_seconds: float = Field(default=180, gt=1, le=600)
    runner_timeout_seconds: float = Field(default=120, gt=1, le=1800)
    runner_max_llm_calls: int = Field(default=20, ge=1, le=500)
    runner_max_tool_calls: int = Field(default=20, ge=0, le=500)
    otel_enabled: bool = False
    otel_service_name: str = "trpc-agent-service"
    otel_exporter_otlp_endpoint: str | None = None
    prometheus_enabled: bool = True
    inbound_worker_enabled: bool = True
    inbound_poll_seconds: float = Field(default=0.25, gt=0, le=60)
    admin_oidc_enabled: bool = False
    admin_oidc_issuer: str | None = None
    admin_oidc_audience: str | None = None
    admin_oidc_jwks_url: str | None = None
    admin_oidc_role_claim: str = "roles"
    admin_oidc_tenant_claim: str = "tenant_ids"
    internal_tls_ca_file: str | None = None
    internal_tls_cert_file: str | None = None
    internal_tls_key_file: str | None = None
    tls_server_cert_file: str | None = None
    tls_server_key_file: str | None = None
    tls_client_ca_file: str | None = None
    vault_address: str | None = None
    vault_token: SecretStr | None = None
    vault_namespace: str | None = None
    aws_region: str | None = None
    vector_backend: Literal["inmemory", "qdrant"] = "inmemory"
    qdrant_url: str | None = None
    qdrant_api_key: SecretStr | None = None
    qdrant_collection: str = "trpc_agent_vectors"
    release_track: Literal["stable", "canary"] = "stable"
    canary_tenant_ids: str = ""

    @model_validator(mode="after")
    def validate_backends(self) -> "Settings":
        if self.coordination_backend == "redis" and self.redis_url is None:
            raise ValueError("redis_url is required when coordination_backend=redis")
        if self.artifact_backend == "minio" and not all(
            (self.minio_endpoint, self.minio_access_key, self.minio_secret_key)
        ):
            raise ValueError(
                "minio_endpoint, minio_access_key and minio_secret_key are required "
                "when artifact_backend=minio"
            )
        if self.node_heartbeat_seconds >= self.node_ttl_seconds:
            raise ValueError("node_heartbeat_seconds must be less than node_ttl_seconds")
        if self.admin_oidc_enabled and not all(
            (self.admin_oidc_issuer, self.admin_oidc_audience, self.admin_oidc_jwks_url)
        ):
            raise ValueError("OIDC issuer, audience and jwks_url are required")
        if any((self.internal_tls_cert_file, self.internal_tls_key_file)) and not all(
            (self.internal_tls_ca_file, self.internal_tls_cert_file, self.internal_tls_key_file)
        ):
            raise ValueError("internal mTLS requires CA, certificate and private key")
        if any((self.tls_server_cert_file, self.tls_server_key_file)) and not all(
            (self.tls_server_cert_file, self.tls_server_key_file)
        ):
            raise ValueError("TLS server certificate and private key must be configured together")
        if self.vector_backend == "qdrant" and not self.qdrant_url:
            raise ValueError("qdrant_url is required when vector_backend=qdrant")
        if (
            self.environment == "production"
            and self.gateway_internal_secret.get_secret_value() == "development-only-change-me"
        ):
            raise ValueError("gateway_internal_secret must be changed in production")
        if self.environment == "production" and self.conversation_backend != "sql":
            raise ValueError("production requires conversation_backend=sql")
        return self


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide immutable settings instance."""

    return Settings()
