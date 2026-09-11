"""HTTP boundary schemas.  They intentionally never accept a tenant from a callback body."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TenantCreate(ApiModel):
    tenant_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{1,63}$")
    display_name: str = Field(min_length=1, max_length=200)
    audit_policy: dict[str, Any] = Field(default_factory=dict)
    budget_policy: dict[str, Any] = Field(default_factory=dict)


class TenantResponse(ApiModel):
    tenant_id: str
    display_name: str
    status: str
    audit_policy: dict[str, Any] = Field(default_factory=dict)
    budget_policy: dict[str, Any] = Field(default_factory=dict)
    routing_epoch: int
    security_epoch: int
    execution_mode: str
    tool_denylist: list[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class AgentCreate(ApiModel):
    agent_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{1,63}$")
    name: str = Field(min_length=1, max_length=200)


class ReleaseCreate(ApiModel):
    version: int = Field(ge=1)
    app_config: dict[str, Any] = Field(default_factory=dict)
    release_model_config: dict[str, Any] = Field(
        default_factory=dict, validation_alias="model_config", serialization_alias="model_config"
    )
    tool_policy: dict[str, Any] = Field(default_factory=dict)
    knowledge_config: dict[str, Any] = Field(default_factory=dict)
    created_by: str = Field(default="admin", min_length=1, max_length=128)
    change_reason: str = Field(default="API release", min_length=1, max_length=500)


class ChannelCreate(ApiModel):
    binding_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{1,127}$")
    agent_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{1,63}$")
    provider: Literal["mock", "telegram", "wecom", "wecom_aibot"]
    external_account_id: str = Field(min_length=1, max_length=256)
    webhook_key: str | None = Field(default=None, min_length=16, max_length=512)
    secret_ref: str = Field(default="env://mock-channel-secret", min_length=1, max_length=512)
    capabilities: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_transport_requirements(self) -> ChannelCreate:
        if self.provider != "wecom_aibot" and not self.webhook_key:
            raise ValueError("webhook_key is required for callback-based channels")
        return self


class BudgetCreate(ApiModel):
    budget_name: str = Field(default="model_tokens", pattern=r"^[A-Za-z0-9_.-]{1,64}$")
    limit_units: int = Field(ge=0)
    unit: Literal["cost_micros", "tokens", "tool_units"] = "tokens"
    period_start: datetime | None = None
    period_end: datetime | None = None


class MigrationCreate(ApiModel):
    source_profile: str = Field(min_length=1, max_length=128)
    target_profile: str = Field(min_length=1, max_length=128)


class MigrationAction(ApiModel):
    action: Literal[
        "prepare", "backfill", "catch_up", "drain", "verify", "cutover", "rollback", "cancel"
    ]


class KnowledgeDocumentCreate(ApiModel):
    """Tenant-local text to persist and project into the active retrieval profile."""

    document_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{1,127}$")
    knowledge_base_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{1,127}$")
    content: str = Field(min_length=1, max_length=200_000)
    acl: dict[str, Any] = Field(default_factory=dict)


class RunRequest(ApiModel):
    input: str = Field(min_length=1, max_length=100_000)
    session_key: str | None = Field(default=None, max_length=512)
    subject_id: str = Field(default="api-user", min_length=1, max_length=256)
    idempotency_key: str | None = Field(default=None, max_length=512)


class OperationResponse(ApiModel):
    request_id: str
    status: str
    inbox_id: str | None = None
    execution_id: str | None = None
    reply: str | None = None
    trace_id: str


class ResolutionRequest(ApiModel):
    action: Literal["retry", "accepted", "failed", "manual_review"]
    note: str = Field(default="", max_length=2000)


class Page(ApiModel):
    items: list[dict[str, Any]]
    next_cursor: str | None = None
