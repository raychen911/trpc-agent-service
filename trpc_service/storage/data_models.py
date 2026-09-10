"""Tenant-scoped, immutable data objects used by phase-seven repositories."""
from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .canonical import canonical_json, content_digest, safe_size_bytes


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must include UTC timezone")
    return value.astimezone(timezone.utc)


class DataScope(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    tenant_id: str = Field(min_length=1)
    agent_id: str | None = None
    trace_id: UUID
    owner_trace_id: UUID | None = None
    execution_trace_id: UUID | None = None
    fence_generation: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _validate_tenant(self) -> "DataScope":
        if not self.tenant_id.strip():
            raise ValueError("tenant scope is required")
        return self

    def diagnostic(self) -> dict[str, Any]:
        return {"tenant_digest": content_digest(self.tenant_id)[:16], "trace_id": str(self.trace_id)}


class Authority(StrEnum):
    POSTGRES = "POSTGRES"
    REDIS_LEGACY = "REDIS_LEGACY"


class SessionStream(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    tenant_id: str = Field(min_length=1)
    session_key: str = Field(min_length=1)
    watermark: int = Field(default=0, ge=0)
    authority: Authority = Authority.POSTGRES
    rollback_eligible: bool = True
    generation: int = Field(default=1, ge=0)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def _normalise_time(self) -> "SessionStream":
        object.__setattr__(self, "created_at", _utc(self.created_at))
        object.__setattr__(self, "updated_at", _utc(self.updated_at))
        return self


class SessionEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    tenant_id: str = Field(min_length=1)
    session_key: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    sequence: int = Field(ge=1)
    event_type: str = Field(default="message", min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    content_digest: str | None = None
    trace_id: UUID | None = None
    owner_trace_id: UUID | None = None
    execution_trace_id: UUID | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime | None = None

    @model_validator(mode="before")
    @classmethod
    def _legacy_names(cls, values: Any) -> Any:
        if isinstance(values, dict):
            values = dict(values)
            if "session_key" not in values and "key" in values:
                values["session_key"] = values["key"]
            if "payload" not in values and "value" in values:
                values["payload"] = values["value"]
        return values

    @model_validator(mode="after")
    def _normalise(self) -> "SessionEvent":
        object.__setattr__(self, "created_at", _utc(self.created_at))
        object.__setattr__(self, "updated_at", _utc(self.updated_at) if self.updated_at else self.created_at)
        if self.content_digest is None:
            object.__setattr__(self, "content_digest", content_digest(self.payload))
        return self

    @property
    def key(self) -> str:
        return self.session_key

    @property
    def value(self) -> dict[str, Any]:
        return self.payload


class MemoryRecord(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    tenant_id: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    memory_key: str = Field(min_length=1)
    content: dict[str, Any] = Field(default_factory=dict)
    version: int = Field(default=1, ge=1)
    content_digest: str | None = None
    source_event_watermark: int | None = Field(default=None, ge=0)
    max_bytes: int = Field(default=65_536, ge=1)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    trace_id: UUID | None = None

    @model_validator(mode="before")
    @classmethod
    def _legacy_names(cls, values: Any) -> Any:
        if isinstance(values, dict):
            values = dict(values)
            if "memory_key" not in values and "key" in values:
                values["memory_key"] = values["key"]
            if "content" not in values and "value" in values:
                values["content"] = values["value"]
        return values

    @model_validator(mode="after")
    def _normalise(self) -> "MemoryRecord":
        object.__setattr__(self, "updated_at", _utc(self.updated_at))
        if self.content_digest is None:
            object.__setattr__(self, "content_digest", content_digest(self.content))
        return self

    @property
    def key(self) -> str:
        return self.memory_key

    @property
    def value(self) -> dict[str, Any]:
        return self.content


class SummaryRecord(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    tenant_id: str = Field(min_length=1)
    session_key: str = Field(min_length=1)
    content: dict[str, Any] = Field(default_factory=dict)
    event_watermark: int = Field(default=0, ge=0)
    version: int = Field(default=1, ge=1)
    content_digest: str | None = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    trace_id: UUID | None = None

    @model_validator(mode="before")
    @classmethod
    def _legacy_names(cls, values: Any) -> Any:
        if isinstance(values, dict):
            values = dict(values)
            if "session_key" not in values and "key" in values:
                values["session_key"] = values["key"]
            if "content" not in values and "value" in values:
                values["content"] = values["value"]
            if "event_watermark" not in values and "event_sequence" in values:
                values["event_watermark"] = values["event_sequence"]
        return values

    @model_validator(mode="after")
    def _normalise(self) -> "SummaryRecord":
        object.__setattr__(self, "updated_at", _utc(self.updated_at))
        if self.content_digest is None:
            object.__setattr__(self, "content_digest", content_digest(self.content))
        return self

    @property
    def key(self) -> str:
        return self.session_key

    @property
    def value(self) -> dict[str, Any]:
        return self.content

    @property
    def event_sequence(self) -> int:
        return self.event_watermark


class ArtifactStatus(StrEnum):
    PUBLISHED = "PUBLISHED"
    DELETED = "DELETED"


class UploadStatus(StrEnum):
    STAGED = "STAGED"
    VERIFIED = "VERIFIED"
    PUBLISHED = "PUBLISHED"
    ORPHANED = "ORPHANED"
    DELETED = "DELETED"


class ArtifactMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    tenant_id: str = Field(min_length=1)
    artifact_id: str = Field(min_length=1)
    storage_ref: str = Field(min_length=1)
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    byte_size: int = Field(ge=0)
    media_type: str = Field(default="application/octet-stream", min_length=1)
    status: ArtifactStatus = ArtifactStatus.PUBLISHED
    version: int = Field(default=1, ge=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ArtifactUpload(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    tenant_id: str = Field(min_length=1)
    upload_id: str = Field(min_length=1)
    artifact_id: str = Field(min_length=1)
    temp_ref_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: UploadStatus = UploadStatus.STAGED
    expected_metadata_version: int | None = Field(default=None, ge=1)
    expires_at: datetime
    generation: int = Field(default=1, ge=0)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ArtifactRecord(ArtifactMetadata):
    """Backward-compatible artifact name from the local prototype."""
    artifact_id: str = Field(default="legacy", min_length=1)

    @model_validator(mode="before")
    @classmethod
    def _legacy_names(cls, values: Any) -> Any:
        if isinstance(values, dict):
            values = dict(values)
            if "artifact_id" not in values and "key" in values:
                values["artifact_id"] = values["key"]
            if "byte_size" not in values:
                values["byte_size"] = safe_size_bytes(values.get("value", values.get("content", {})))
            if "content_digest" not in values and "value" in values:
                values["content_digest"] = content_digest(values["value"])
        return values

    @property
    def key(self) -> str:
        return self.artifact_id


class KnowledgeStatus(StrEnum):
    PENDING_INDEX = "PENDING_INDEX"
    INDEXED = "INDEXED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    DELETED = "DELETED"


class KnowledgeDocument(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    tenant_id: str = Field(min_length=1)
    document_id: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)
    content_digest: str = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")
    embedding_ref: str | None = None
    index_status: KnowledgeStatus = KnowledgeStatus.PENDING_INDEX
    version: int = Field(default=1, ge=1)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class MigrationState(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    tenant_id: str = Field(min_length=1)
    stream: str = Field(min_length=1)
    state: str = "PLANNED"
    authority: Authority = Authority.REDIS_LEGACY
    source_watermark: int | None = Field(default=None, ge=0)
    copied_watermark: int = Field(default=0, ge=0)
    source_digest: str | None = None
    target_digest: str | None = None
    rollback_eligible: bool = True
    generation: int = Field(default=1, ge=0)
    lease_owner_digest: str | None = None
    failure_code: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DataRecoveryMarker(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    marker_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    operation: str = Field(min_length=1)
    stage: str = Field(min_length=1)
    result_digest: str | None = None
    generation: int = Field(default=1, ge=0)
    review_reason: str | None = None
    confirmed: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class EventMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    tenant_id: str
    session_key: str
    event_id: str
    sequence: int
    event_type: str
    content_digest: str
    trace_id: UUID | None = None
    created_at: datetime


class MemoryMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    tenant_id: str
    namespace: str
    memory_key: str
    version: int
    content_digest: str
    byte_size: int
    source_event_watermark: int | None = None
    updated_at: datetime


class SummaryMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    tenant_id: str
    session_key: str
    version: int
    event_watermark: int
    content_digest: str
    updated_at: datetime


__all__ = [
    "Authority", "ArtifactMetadata", "ArtifactRecord", "ArtifactStatus", "ArtifactUpload",
    "DataRecoveryMarker", "DataScope", "EventMetadata", "KnowledgeDocument", "KnowledgeStatus",
    "MemoryMetadata", "MemoryRecord", "MigrationState", "SessionEvent", "SessionStream",
    "SummaryMetadata", "SummaryRecord", "UploadStatus", "canonical_json", "content_digest",
    "safe_size_bytes",
]
