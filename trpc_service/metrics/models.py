"""Validated metrics snapshots with explicit tenant or pre-auth scope."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trpc_service.audit.models import PreAuthScope, TenantScope
from trpc_service.channels.contracts import Channel


class ChannelMetricEvent(BaseModel):
    """One bounded, pseudonymous channel observation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    node_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
    tenant: str = Field(pattern=r"^sha256:[0-9a-f]{16}$")
    channel: Channel
    stage: str
    outcome: str
    duration_ms: float = Field(ge=0)
    attempt_no: int | None = Field(default=None, ge=1, le=4)
    generation: int | None = Field(default=None, gt=0)

    @field_validator("stage")
    @classmethod
    def bounded_stage(cls, value: str) -> str:
        if value not in {"adapter_connection", "filter", "takeover", "delivery"}:
            raise ValueError("unsupported channel metric stage")
        return value

    @field_validator("outcome")
    @classmethod
    def bounded_outcome(cls, value: str) -> str:
        if value not in {
            "ready", "standby", "not_ready", "accepted", "ignored", "rejected",
            "retrying", "success", "failed", "unknown", "stale", "acquired", "lost",
        }:
            raise ValueError("unsupported channel metric outcome")
        return value


class DataMetricEvent(BaseModel):
    """Low-cardinality data metric; tenant/user/session/trace are forbidden."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    resource_type: str = Field(pattern=r"^[a-z][a-z0-9_]{0,31}$")
    backend_type: str = Field(pattern=r"^[a-z][a-z0-9_]{0,31}$")
    operation: str = Field(pattern=r"^[a-z][a-z0-9_]{0,31}$")
    outcome: str = Field(pattern=r"^[a-z][a-z0-9_]{0,31}$")
    duration_ms: float = Field(ge=0)


class MetricSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    scope: TenantScope | PreAuthScope
    request_count: int = Field(default=0, ge=0)
    error_count: int = Field(default=0, ge=0)
    stage_latency_ms: dict[str, float] = Field(default_factory=dict)
    agent_latency_ms: float = Field(default=0, ge=0)
    state_backend_latency_ms: float = Field(default=0, ge=0)
    channel_delivery_count: int = Field(default=0, ge=0)
    token_count: int = Field(default=0, ge=0)
    tenant_cost: Decimal = Field(default=Decimal("0"), ge=0)
    model_metric_status: Literal["not_applicable"] = "not_applicable"
    tool_metric_status: Literal["not_applicable"] = "not_applicable"
    im_metric_status: Literal["not_applicable"] = "not_applicable"

    @model_validator(mode="after")
    def validate_counts_and_latencies(self) -> Self:
        if self.error_count > self.request_count:
            raise ValueError("error_count cannot exceed request_count")
        if any(value < 0 for value in self.stage_latency_ms.values()):
            raise ValueError("stage latency cannot be negative")
        return self
