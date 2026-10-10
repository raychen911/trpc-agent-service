"""Immutable tenant and policy models."""

from __future__ import annotations

import ipaddress
from collections.abc import Collection
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trpc_service.config.secrets import SecretRef

_REGISTERED_BACKENDS = frozenset(
    {"inmemory", "redis", "postgresql", "s3", "pgvector", "external_memory"}
)
_BLOCKED_MODEL_HOSTS = frozenset(
    {
        "localhost",
        "localhost.localdomain",
        "metadata.google.internal",
        "metadata.google.com",
        "instance-data.ec2.internal",
    }
)


def validate_model_base_url(
    value: str,
    *,
    allowed_hosts: Collection[str] | None = None,
    resolved_addresses: Collection[str] | None = None,
) -> str:
    """Reject URL forms that can turn a model endpoint into an SSRF primitive.

    DNS resolution is intentionally not performed in the model validator.  A
    deployment can pass an explicit endpoint allow-list to the agent loader;
    this pure validation keeps tests deterministic and avoids a DNS rebinding
    race between validation and the actual client connection.
    """

    if not isinstance(value, str) or len(value) > 2048:
        raise ValueError("model endpoint URL is invalid")
    try:
        parsed = urlsplit(value)
    except ValueError as exc:
        raise ValueError("model endpoint URL is invalid") from exc
    try:
        host = (parsed.hostname or "").lower().rstrip(".")
    except ValueError as exc:
        raise ValueError("model endpoint URL is invalid") from exc
    if parsed.scheme.lower() != "https" or not host:
        raise ValueError("model endpoint must use HTTPS")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("model endpoint userinfo is forbidden")
    try:
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError("model endpoint port is invalid")
    except ValueError as exc:
        raise ValueError("model endpoint port is invalid") from exc
    if parsed.query or parsed.fragment:
        raise ValueError("model endpoint query and fragment are forbidden")
    if host in _BLOCKED_MODEL_HOSTS or host.endswith(".localhost"):
        raise ValueError("model endpoint host is not allowed")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address is not None and (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_reserved
        or address.is_unspecified
        or address.is_multicast
    ):
        raise ValueError("model endpoint address is not allowed")
    if allowed_hosts is not None:
        allowed = {str(item).lower().rstrip(".") for item in allowed_hosts}
        if host not in allowed:
            raise ValueError("model endpoint host is not registered")
    if resolved_addresses is not None:
        for resolved_value in resolved_addresses:
            try:
                resolved = ipaddress.ip_address(str(resolved_value))
            except ValueError as exc:
                raise ValueError("model endpoint DNS result is invalid") from exc
            if (
                resolved.is_private
                or resolved.is_loopback
                or resolved.is_link_local
                or resolved.is_reserved
                or resolved.is_unspecified
                or resolved.is_multicast
            ):
                raise ValueError("model endpoint DNS result is not allowed")
    return value


def validate_storage_backend(value: str, *, registered: Collection[str] | None = None) -> str:
    available = _REGISTERED_BACKENDS if registered is None else set(registered)
    if value not in available:
        raise ValueError("storage backend is not registered")
    return value


class Channel(StrEnum):
    WECOM_AI_BOT = "wecom_ai_bot"
    FEISHU = "feishu"


class ConversationKind(StrEnum):
    DIRECT = "direct"
    GROUP = "group"


class ToolRisk(StrEnum):
    IDEMPOTENT = "idempotent"
    NON_IDEMPOTENT = "non_idempotent"
    UNKNOWN = "unknown"


class TenantStatus(StrEnum):
    ACTIVE = "active"
    SUSPENDED = "suspended"


class ImmutableModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class TenantContext(ImmutableModel):
    """Identity and pinned configuration propagated across the whole turn."""

    tenant_id: str = Field(min_length=1, max_length=128)
    app_id: str = Field(min_length=1, max_length=128)
    config_version: int = Field(ge=1)
    channel_binding_id: str = Field(min_length=1, max_length=256)
    principal_id: str = Field(min_length=1, max_length=256)
    session_id: str = Field(min_length=1, max_length=256)
    request_id: str = Field(min_length=1, max_length=128)
    trace_id: str = Field(min_length=1, max_length=64)


class ModelPolicy(ImmutableModel):
    provider: str
    model: str
    api_key_ref: SecretRef | None = None
    base_url: str | None = None
    timeout_seconds: float = Field(default=60, gt=0, le=600)
    temperature: float | None = Field(default=None, ge=0, le=2)
    top_p: float | None = Field(default=None, ge=0, le=1)
    stop_sequences: tuple[str, ...] = Field(default=(), max_length=8)
    reasoning_effort: Literal["none", "minimal", "low", "medium", "high", "xhigh", "max"] | None = (
        None
    )
    thinking_budget_tokens: int | None = None
    fallback_model: str | None = None

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, value: str | None) -> str | None:
        if value is not None:
            validate_model_base_url(value)
        return value

    @field_validator("stop_sequences")
    @classmethod
    def validate_stop_sequences(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item or len(item) > 256 for item in value):
            raise ValueError("model stop sequences must contain 1 to 256 characters")
        return value

    @field_validator("thinking_budget_tokens")
    @classmethod
    def validate_thinking_budget(cls, value: int | None) -> int | None:
        if value is not None and value not in {-1, 0} and value < 1024:
            raise ValueError("thinking budget must be -1, 0, or at least 1024 tokens")
        return value

    @model_validator(mode="after")
    def validate_reasoning_controls(self) -> ModelPolicy:
        provider = self.provider.lower()
        if self.reasoning_effort is not None and provider != "openai":
            raise ValueError("reasoning_effort requires the openai provider")
        if self.thinking_budget_tokens is not None and provider not in {"openai", "anthropic"}:
            raise ValueError("thinking_budget_tokens requires the openai or anthropic provider")
        if self.reasoning_effort is not None and self.thinking_budget_tokens is not None:
            raise ValueError("reasoning_effort and thinking_budget_tokens are mutually exclusive")
        return self


class BudgetPolicy(ImmutableModel):
    max_tokens_per_turn: int = Field(default=32_000, ge=1)
    monthly_cost_units: int = Field(default=1_000_000, ge=0)
    max_parallel_turns: int = Field(default=20, ge=1)


class ToolPolicy(ImmutableModel):
    allow: frozenset[str] = frozenset()
    require_confirmation: frozenset[str] = frozenset()
    classifications: dict[str, ToolRisk] = Field(default_factory=dict)


class AuditPolicy(ImmutableModel):
    retention_days: int = Field(default=180, ge=1, le=3650)
    record_content: bool = False

    @field_validator("record_content")
    @classmethod
    def reject_content_audit(cls, value: bool) -> bool:
        if value:
            raise ValueError("message and tool content cannot be persisted in audit logs")
        return value


class MediaPolicy(ImmutableModel):
    """Bound inbound media work before it reaches storage or a model."""

    enabled: bool = True
    max_items_per_turn: int = Field(default=4, ge=1, le=16)
    max_bytes_per_item: int = Field(default=20 * 1024 * 1024, ge=1024, le=100 * 1024 * 1024)
    max_total_bytes: int = Field(default=32 * 1024 * 1024, ge=1024, le=200 * 1024 * 1024)
    max_pdf_pages: int = Field(default=40, ge=1, le=500)
    max_extracted_chars: int = Field(default=60_000, ge=256, le=500_000)
    inline_images: bool = True


class StorageSelection(ImmutableModel):
    """Descriptive storage selection used by revisions and migration evidence.

    Production workers must validate this value as a
    :class:`ProductionStorageSelection` before constructing built-in services.
    The wider backend vocabulary is retained here for offline migration source
    descriptions; it does not imply a runnable adapter exists.
    """

    profile_id: str
    session_backend: str = "postgresql"
    memory_backend: str = "postgresql"
    artifact_backend: str = "s3"
    knowledge_backend: str = "pgvector"

    @field_validator("session_backend", "memory_backend", "artifact_backend", "knowledge_backend")
    @classmethod
    def validate_backend(cls, value: str) -> str:
        return validate_storage_backend(value)


class ProductionStorageSelection(StorageSelection):
    """Storage backends implemented by the built-in production worker."""

    session_backend: Literal["postgresql"] = "postgresql"
    memory_backend: Literal["postgresql"] = "postgresql"
    artifact_backend: Literal["s3"] = "s3"
    knowledge_backend: Literal["pgvector"] = "pgvector"


class TenantConfig(ImmutableModel):
    """An immutable configuration revision."""

    tenant_id: str
    app_id: str
    version: int = Field(ge=1)
    model: ModelPolicy
    tools: ToolPolicy = ToolPolicy()
    budget: BudgetPolicy = BudgetPolicy()
    audit: AuditPolicy = AuditPolicy()
    media: MediaPolicy = MediaPolicy()
    storage: StorageSelection
    instructions: str = ""
    policy_version: int = Field(default=1, ge=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def validate_model_token_budget(self) -> TenantConfig:
        thinking_budget = self.model.thinking_budget_tokens
        if thinking_budget is not None and thinking_budget > 0:
            if thinking_budget >= self.budget.max_tokens_per_turn:
                raise ValueError("thinking budget must be smaller than max_tokens_per_turn")
        return self


class ChannelBinding(ImmutableModel):
    binding_id: str
    tenant_id: str
    app_id: str
    channel: Channel
    account_id: str = Field(min_length=1, max_length=256)
    secret_refs: dict[str, SecretRef] = Field(default_factory=dict)
    enabled: bool = True
    control_version: int = Field(default=1, ge=1)
    capabilities: frozenset[str] = frozenset()

    @field_validator("secret_refs")
    @classmethod
    def validate_secret_ref_keys(cls, value: dict[str, SecretRef]) -> dict[str, SecretRef]:
        allowed = {"app_secret", "verification_token", "encrypt_key", "bot_secret"}
        unknown = set(value).difference(allowed)
        if unknown:
            raise ValueError("channel secret reference name is not supported")
        return value


class TenantRecord(ImmutableModel):
    tenant_id: str
    display_name: str
    status: TenantStatus = TenantStatus.ACTIVE
    control_version: int = Field(default=1, ge=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class AuditEntry(ImmutableModel):
    tenant_id: str
    channel: Channel | None = None
    user_id: str | None = None
    session_id: str | None = None
    agent_name: str | None = None
    tool_name: str | None = None
    decision: str
    latency_ms: int | None = Field(default=None, ge=0)
    error_type: str | None = None
    cost_units: int = Field(default=0, ge=0)
    trace_id: str
    config_version: int | None = None
    policy_version: int | None = None
    idempotency_key: str | None = None
    redaction_applied: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


__all__ = [
    "AuditEntry",
    "AuditPolicy",
    "BudgetPolicy",
    "Channel",
    "ChannelBinding",
    "ConversationKind",
    "MediaPolicy",
    "ModelPolicy",
    "ProductionStorageSelection",
    "StorageSelection",
    "TenantConfig",
    "TenantContext",
    "TenantRecord",
    "TenantStatus",
    "ToolPolicy",
    "ToolRisk",
    "validate_model_base_url",
]
