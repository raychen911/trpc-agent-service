"""Safe tenant-scoped unified audit read model."""
from __future__ import annotations

from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StrictInt


class UnifiedAuditRecord(BaseModel):
    """Public audit projection.  Deliberately no bodies, digests or secrets."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    tenant_id: str
    channel: str | None
    user_id: str | None
    session_id: str | None
    agent_name: str | None
    tool_name: str | None
    decision: str
    latency_ms: StrictInt | None = Field(default=None, ge=0)
    error_type: str | None
    error_code: str | None
    cost_microunits: StrictInt | None = Field(default=None, ge=0)
    trace_id: str | None
    request_id: UUID
    config_version: StrictInt
    occurred_at: AwareDatetime


class UnifiedAuditPage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    events: tuple[UnifiedAuditRecord, ...]


__all__ = ["UnifiedAuditPage", "UnifiedAuditRecord"]
