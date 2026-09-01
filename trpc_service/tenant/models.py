"""Strongly validated tenant configuration contract."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

_LOGICAL_ID = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
_AGENT_NAME = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]{0,63}\Z")


class ChannelType(StrEnum):
    """Supported first-party channel adapters."""

    WECOM = "wecom"
    TELEGRAM = "telegram"


class ModelRoute(BaseModel):
    """Tenant-approved model route and hard turn limits."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: str
    model: str
    api_key_ref: str | None = None
    timeout_seconds: int = Field(default=60, ge=1, le=600)
    token_ceiling: int = Field(default=8_000, ge=128, le=1_000_000)
    temperature: float = Field(default=0.2, ge=0.0, le=2.0)
    fallback_models: tuple[str, ...] = ()

    @model_validator(mode="after")
    def route_names_are_not_blank(self) -> ModelRoute:
        if not self.provider.strip() or not self.model.strip():
            raise ValueError("model provider and model must not be blank")
        if self.api_key_ref is not None and not self.api_key_ref.startswith("secret://"):
            raise ValueError("model api_key_ref must be a secret:// reference")
        return self


class ToolPolicy(BaseModel):
    """Tenant tool visibility, approval, and budget policy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    allowed: frozenset[str] = frozenset()
    requires_approval: frozenset[str] = frozenset()
    max_calls_per_turn: int = Field(default=12, ge=0, le=128)
    max_cost_per_turn: float = Field(default=1.0, ge=0.0)

    @model_validator(mode="after")
    def approval_tools_must_be_allowed(self) -> ToolPolicy:
        if not self.requires_approval.issubset(self.allowed):
            raise ValueError("requires_approval must be a subset of allowed tools")
        return self


class ChannelSpec(BaseModel):
    """Channel binding configuration containing references, never raw secrets."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    binding_id: str
    app_id: str
    app_revision: int = Field(ge=1)
    channel: ChannelType
    external_account_id: str
    callback_path: str
    public_callback_id: str
    route_rule: dict[str, Any] = Field(default_factory=dict)
    secret_refs: dict[str, str]
    identity_policy: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True

    @model_validator(mode="after")
    def route_is_canonical(self) -> ChannelSpec:
        for field_name in ("binding_id", "app_id", "public_callback_id"):
            if _LOGICAL_ID.fullmatch(getattr(self, field_name)) is None:
                raise ValueError(f"{field_name} is not a safe logical identifier")
        expected_path = f"/v1/channels/{self.channel.value}/{self.public_callback_id}/callback"
        if self.callback_path != expected_path:
            raise ValueError(f"callback_path must equal {expected_path}")
        return self

    @model_validator(mode="after")
    def secret_values_must_be_references(self) -> ChannelSpec:
        for name, value in self.secret_refs.items():
            if not value.startswith("secret://"):
                raise ValueError(f"{name} must be a secret:// reference")
        required = {
            ChannelType.WECOM: {"token", "aes_key"},
            ChannelType.TELEGRAM: {"webhook_secret", "bot_token"},
        }[self.channel]
        missing = required.difference(self.secret_refs)
        if missing:
            raise ValueError(f"missing channel secret references: {sorted(missing)}")
        return self


class StorageSpec(BaseModel):
    """Tenant-selectable projection backends."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    session: Literal["postgresql", "redis", "inmemory"] = "postgresql"
    memory: Literal["postgresql", "redis", "external"] = "postgresql"
    summary: Literal["postgresql", "redis"] = "postgresql"
    knowledge: Literal["pgvector", "qdrant", "milvus", "inmemory"] = "pgvector"
    artifact: Literal["s3", "minio", "local"] = "s3"


class AuditPolicy(BaseModel):
    """Tenant audit retention and export policy."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scope: str = "all_tools"
    retention_days: int = Field(default=180, ge=1, le=3_650)
    export: str = "restricted"
    capture_prompt_content: bool = False


class AgentAppSpec(BaseModel):
    """Immutable published Agent app configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    app_id: str
    revision: int = Field(ge=1)
    name: str
    prompt: str
    model: ModelRoute
    tools: ToolPolicy = ToolPolicy()
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def framework_identifiers_are_valid(self) -> AgentAppSpec:
        if _LOGICAL_ID.fullmatch(self.app_id) is None:
            raise ValueError("app_id is not a safe logical identifier")
        if _AGENT_NAME.fullmatch(self.name) is None or self.name == "user":
            raise ValueError("Agent name must be a non-reserved Python identifier")
        if not self.prompt.strip():
            raise ValueError("Agent prompt must not be blank")
        return self


class TenantSpec(BaseModel):
    """Complete revisioned tenant specification."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: int = Field(default=1, ge=1)
    tenant_id: str
    revision: int = Field(ge=1)
    display_name: str
    status: str = "active"
    apps: tuple[AgentAppSpec, ...] = Field(min_length=1)
    channels: tuple[ChannelSpec, ...]
    storage: StorageSpec = StorageSpec()
    audit: AuditPolicy = AuditPolicy()
    budget: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def unique_logical_identifiers(self) -> TenantSpec:
        if _LOGICAL_ID.fullmatch(self.tenant_id) is None:
            raise ValueError("tenant_id is not a safe logical identifier")
        if self.status not in {"active", "suspended", "disabled"}:
            raise ValueError("unsupported tenant status")
        if not self.display_name.strip():
            raise ValueError("display_name must not be blank")
        if any(value < 0 for value in self.budget.values()):
            raise ValueError("budget values must not be negative")
        app_ids = [app.app_id for app in self.apps]
        binding_ids = [channel.binding_id for channel in self.channels]
        public_callback_ids = [channel.public_callback_id for channel in self.channels]
        callback_paths = [channel.callback_path for channel in self.channels]
        external_accounts = [
            (channel.channel, channel.external_account_id) for channel in self.channels
        ]
        if len(app_ids) != len(set(app_ids)):
            raise ValueError("app_id values must be unique within a TenantSpec")
        if len(binding_ids) != len(set(binding_ids)):
            raise ValueError("binding_id values must be unique within a TenantSpec")
        if len(public_callback_ids) != len(set(public_callback_ids)):
            raise ValueError("public_callback_id values must be unique within a TenantSpec")
        if len(callback_paths) != len(set(callback_paths)):
            raise ValueError("callback_path values must be unique within a TenantSpec")
        if len(external_accounts) != len(set(external_accounts)):
            raise ValueError("channel external accounts must be unique within a TenantSpec")
        app_revisions = {(app.app_id, app.revision) for app in self.apps}
        missing_apps = {
            (channel.app_id, channel.app_revision)
            for channel in self.channels
            if (channel.app_id, channel.app_revision) not in app_revisions
        }
        if missing_apps:
            raise ValueError(f"channels reference unknown app revisions: {sorted(missing_apps)}")
        return self
