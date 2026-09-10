"""Validated domain records used at service boundaries."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from trpc_service.config.settings import validate_secret_ref


def utc_now() -> datetime:
    return datetime.now(UTC)


class ChannelType(StrEnum):
    HTTP = "http"
    WECOM = "wecom"
    TELEGRAM = "telegram"


class ChannelMode(StrEnum):
    WEBHOOK = "webhook"
    PULL = "pull"


class TenantStatus(StrEnum):
    ACTIVE = "active"
    DISABLED = "disabled"


class StorageBackend(StrEnum):
    SQLITE = "sqlite"
    REDIS = "redis"


class EventType(StrEnum):
    USER_MESSAGE = "user_message"
    AGENT_MESSAGE = "agent_message"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    SYSTEM = "system"


class AuditDecision(StrEnum):
    ALLOWED = "allowed"
    BLOCKED = "blocked"
    CONFIRM_REQUIRED = "confirm_required"
    ERROR = "error"


class OutboxStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    RETRY = "retry"
    COMPLETED = "completed"
    DEAD_LETTER = "dead_letter"
    PUBLISH_FAILED = "publish_failed"


class Record(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra="forbid")


class TenantStorageConfig(Record):
    session_backend: StorageBackend = StorageBackend.SQLITE
    memory_backend: StorageBackend = StorageBackend.SQLITE
    redis_url_ref: str | None = None

    @model_validator(mode="after")
    def validate_backend_reference(self) -> TenantStorageConfig:
        uses_redis = StorageBackend.REDIS in {
            self.session_backend,
            self.memory_backend,
        }
        if uses_redis and self.redis_url_ref is None:
            raise ValueError("redis_url_ref is required when a Redis backend is selected")
        if self.redis_url_ref is not None:
            validate_secret_ref(self.redis_url_ref)
        return self


class TenantRecord(Record):
    tenant_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    name: str = Field(min_length=1, max_length=200)
    status: TenantStatus = TenantStatus.ACTIVE
    audit_policy: dict[str, Any] = Field(default_factory=dict)
    storage_config: TenantStorageConfig = Field(default_factory=TenantStorageConfig)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class AgentAppRecord(Record):
    tenant_id: str
    app_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    name: str = Field(min_length=1, max_length=200)
    system_prompt: str = Field(min_length=1, max_length=20_000)
    model_config_data: dict[str, Any] = Field(default_factory=dict)
    tool_policy: dict[str, Any] = Field(default_factory=dict)
    active_config_version: int = Field(default=1, ge=1)
    is_active: bool = True
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class ChannelBindingRecord(Record):
    tenant_id: str
    app_id: str = Field(default="assistant", min_length=1, max_length=64)
    binding_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    channel_type: ChannelType
    connection_mode: ChannelMode = ChannelMode.WEBHOOK
    account_id: str = Field(min_length=1, max_length=200)
    token_ref: str | None = None
    secret_ref: str | None = None
    aes_key_ref: str | None = None
    webhook_path: str = ""
    is_active: bool = True
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class InboundMessageRecord(Record):
    inbound_id: str
    tenant_id: str
    binding_id: str
    external_message_id: str
    session_id: str
    trace_id: str
    status: str = "accepted"
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)


class SessionRecord(Record):
    tenant_id: str
    session_id: str
    app_id: str
    principal_id: str
    channel_type: ChannelType
    state: dict[str, Any] = Field(default_factory=dict)
    version: int = Field(default=0, ge=0)
    last_event_sequence: int = Field(default=0, ge=0)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class SessionEventRecord(Record):
    event_id: str
    tenant_id: str
    session_id: str
    sequence: int = Field(ge=1)
    event_type: EventType
    payload: dict[str, Any] = Field(default_factory=dict)
    trace_id: str
    created_at: datetime = Field(default_factory=utc_now)


class MemoryRecord(Record):
    memory_id: str
    tenant_id: str
    principal_id: str
    content: str
    source_event_id: str | None = None
    metadata_data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)


class SummaryRecord(Record):
    summary_id: str
    tenant_id: str
    session_id: str
    content: str
    source_end_sequence: int = Field(ge=0)
    version: int = Field(default=1, ge=1)
    created_at: datetime = Field(default_factory=utc_now)


class KnowledgeRecord(Record):
    knowledge_id: str
    tenant_id: str
    app_id: str | None = None
    title: str = Field(min_length=1, max_length=500)
    content: str = Field(min_length=1)
    metadata_data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class ArtifactRecord(Record):
    artifact_id: str
    tenant_id: str
    session_id: str | None = None
    filename: str = Field(min_length=1, max_length=500)
    media_type: str = Field(default="application/octet-stream", max_length=200)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(min_length=64, max_length=64)
    storage_uri: str
    metadata_data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)


class ExecutionOutboxRecord(Record):
    outbox_id: str
    trace_id: str
    tenant_id: str
    payload: dict[str, Any]
    status: OutboxStatus = OutboxStatus.QUEUED
    attempts: int = Field(default=0, ge=0)
    error_type: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class AuditLogRecord(Record):
    log_id: str
    trace_id: str
    tenant_id: str
    channel: ChannelType
    user_id: str
    session_id: str
    agent_name: str
    tool_name: str | None = None
    decision: AuditDecision = AuditDecision.ALLOWED
    latency_ms: int = Field(default=0, ge=0)
    error_type: str | None = None
    cost: float = Field(default=0, ge=0)
    created_at: datetime = Field(default_factory=utc_now)


__all__ = [
    "AgentAppRecord",
    "ArtifactRecord",
    "AuditDecision",
    "AuditLogRecord",
    "ChannelBindingRecord",
    "ChannelMode",
    "ChannelType",
    "EventType",
    "ExecutionOutboxRecord",
    "InboundMessageRecord",
    "KnowledgeRecord",
    "MemoryRecord",
    "OutboxStatus",
    "SessionEventRecord",
    "SessionRecord",
    "SummaryRecord",
    "StorageBackend",
    "TenantRecord",
    "TenantStorageConfig",
    "TenantStatus",
    "utc_now",
]
