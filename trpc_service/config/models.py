# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Typed configuration models for the multi-tenant service."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

from trpc_service._compat import StrEnum


class ServiceRole(StrEnum):
    """Process roles supported by the shared service binary."""

    GATEWAY = "gateway"
    WORKER = "worker"
    DELIVERY = "delivery"
    ADMIN = "admin"
    WECOM = "wecom"


class BackendType(StrEnum):
    """Storage backends understood by the initial implementation."""

    MEMORY = "memory"
    REDIS = "redis"
    SQL = "sql"
    EXTERNAL = "external"


class ChannelType(StrEnum):
    """Inbound and outbound channel types."""

    WEB = "web"
    WECOM = "wecom"
    WECOM_KF = "wecom_kf"
    TELEGRAM = "telegram"


class TenantStatus(StrEnum):
    """Tenant lifecycle states."""

    ACTIVE = "active"
    SUSPENDED = "suspended"
    DELETED = "deleted"


class ModelConfig(BaseModel):
    """Configuration needed to build an SDK model instance."""

    model_config = ConfigDict(extra="forbid")

    provider: str = Field(default="openai-compatible", pattern="^(openai-compatible|anthropic|litellm)$")
    model_name: str = ""
    base_url: str = ""
    api_key_ref: str = ""
    api_key: SecretStr | None = Field(default=None, exclude=True, repr=False)
    timeout_seconds: float = Field(default=60.0, gt=0)
    max_retries: int = Field(default=1, ge=0, le=10)
    input_cost_per_million_usd: float = Field(default=0, ge=0)
    output_cost_per_million_usd: float = Field(default=0, ge=0)


class ToolPolicy(BaseModel):
    """Tenant-owned tool allow and review policy."""

    model_config = ConfigDict(extra="forbid")

    allowed: list[str] = Field(default_factory=list)
    denied: list[str] = Field(default_factory=list)
    confirmation_required: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_disjoint_policy(self) -> "ToolPolicy":
        """Reject contradictory allow and deny entries."""
        overlap = set(self.allowed) & set(self.denied)
        if overlap:
            raise ValueError(f"tools cannot be both allowed and denied: {sorted(overlap)}")
        missing = set(self.confirmation_required) - set(self.allowed)
        if missing:
            raise ValueError(f"confirmation-required tools must also be allowed: {sorted(missing)}")
        return self


class RuntimePolicy(BaseModel):
    """Limits applied to one Agent App runtime."""

    model_config = ConfigDict(extra="forbid")

    max_concurrent_runs: int = Field(default=20, ge=1)
    max_run_seconds: float = Field(default=180.0, gt=0)
    group_session_mode: str = Field(default="per_user", pattern="^(per_user|shared)$")
    enable_post_turn_processing: bool = True
    defer_post_turn_processing: bool = False
    estimated_output_tokens: int = Field(default=2048, ge=0)
    summary_enabled: bool = True
    summary_event_threshold: int = Field(default=30, ge=2)
    summary_keep_recent: int = Field(default=10, ge=1)

    @model_validator(mode="after")
    def validate_strict_post_turn(self):
        if not self.enable_post_turn_processing or self.defer_post_turn_processing:
            raise ValueError("platform requires synchronous post-turn; use summary_enabled to disable only summary")
        if self.summary_enabled and self.summary_keep_recent >= self.summary_event_threshold:
            raise ValueError("summary_keep_recent must be smaller than summary_event_threshold")
        return self


class AgentAppConfig(BaseModel):
    """Immutable configuration snapshot for one tenant Agent App."""

    model_config = ConfigDict(extra="forbid")

    app_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    name: str = Field(default="Agent App", min_length=1, max_length=128)
    agent_name: str = Field(default="assistant", min_length=1, max_length=128)
    description: str = "A helpful assistant."
    instruction: str = "You are a helpful assistant."
    enabled: bool = True
    model: ModelConfig = Field(default_factory=ModelConfig)
    tools: ToolPolicy = Field(default_factory=ToolPolicy)
    runtime: RuntimePolicy = Field(default_factory=RuntimePolicy)


class StoragePolicy(BaseModel):
    """Backend selection for stateful SDK services."""

    model_config = ConfigDict(extra="forbid")

    session: BackendType = BackendType.MEMORY
    memory: BackendType = BackendType.MEMORY
    redis_url: str = "redis://127.0.0.1:6379/0"
    sql_url: str = "sqlite:///./data/trpc_agent_service.db"
    session_ttl_seconds: int = Field(default=86400, ge=1)
    memory_ttl_seconds: int = Field(default=604800, ge=1)
    external_provider: str = ""


class AuditPolicy(BaseModel):
    """Audit and privacy behavior for one tenant."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    pii_masking: str = Field(default="standard", pattern="^(off|standard|strict)$")
    retention_days: int = Field(default=180, ge=1)


class BudgetPolicy(BaseModel):
    """Simple daily quota configuration."""

    model_config = ConfigDict(extra="forbid")

    daily_requests: int = Field(default=10000, ge=1)
    daily_input_tokens: int = Field(default=1_000_000, ge=1)
    daily_output_tokens: int = Field(default=500_000, ge=1)
    daily_cost_usd: float = Field(default=100.0, ge=0)


class ChannelBindingConfig(BaseModel):
    """Maps an external channel account to a tenant Agent App."""

    model_config = ConfigDict(extra="forbid")

    binding_id: str = Field(min_length=1, max_length=128)
    channel: ChannelType
    app_id: str
    external_account_id: str = ""
    secret_ref: str = ""
    webhook_secret_ref: str = ""
    corp_id: str = ""
    open_kfid: str = ""
    encoding_aes_key_ref: str = ""
    enabled: bool = True
    options: dict[str, Any] = Field(default_factory=dict)


class TenantConfig(BaseModel):
    """Published, immutable tenant configuration snapshot."""

    model_config = ConfigDict(extra="forbid")

    tenant_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    name: str = Field(default="Tenant", min_length=1, max_length=128)
    version: int = Field(default=1, ge=1)
    status: TenantStatus = TenantStatus.ACTIVE
    api_token_ref: str = ""
    apps: dict[str, AgentAppConfig] = Field(default_factory=dict)
    channels: list[ChannelBindingConfig] = Field(default_factory=list)
    storage: StoragePolicy = Field(default_factory=StoragePolicy)
    audit: AuditPolicy = Field(default_factory=AuditPolicy)
    budget: BudgetPolicy = Field(default_factory=BudgetPolicy)

    @field_validator("apps")
    @classmethod
    def validate_app_keys(cls, apps: dict[str, AgentAppConfig]) -> dict[str, AgentAppConfig]:
        """Keep mapping keys and embedded app identifiers consistent."""
        mismatches = [key for key, app in apps.items() if key != app.app_id]
        if mismatches:
            raise ValueError(f"app mapping keys must match app_id: {mismatches}")
        return apps

    @model_validator(mode="after")
    def validate_channel_apps(self) -> "TenantConfig":
        """Ensure every channel references an existing Agent App."""
        binding_ids = [binding.binding_id for binding in self.channels]
        if len(binding_ids) != len(set(binding_ids)):
            raise ValueError("channel binding_id values must be unique within a tenant")
        missing = sorted({binding.app_id for binding in self.channels if binding.app_id not in self.apps})
        if missing:
            raise ValueError(f"channel bindings reference unknown apps: {missing}")
        return self


class ServiceSettings(BaseModel):
    """Process-level settings loaded from environment variables."""

    model_config = ConfigDict(extra="forbid")

    environment: str = "development"
    host: str = "127.0.0.1"
    port: int = Field(default=8080, ge=1, le=65535)
    roles: set[ServiceRole] = Field(default_factory=lambda: set(ServiceRole))
    config_file: str = "examples/config/tenants.yaml"
    log_level: str = "INFO"
    worker_id: str = ""
    admin_token: SecretStr | None = Field(default=None, exclude=True, repr=False)
    redis_url: str = "redis://127.0.0.1:6379/15"
    postgres_url: str = "postgresql://trpc_agent:trpc_agent@127.0.0.1:5432/trpc_agent"
    otlp_endpoint: str = ""
