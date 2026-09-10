"""Async ports for the observability subsystem (contracts/observability-contracts.md).

Ports are vendor-neutral: implementations may target OTLP/OTel, PostgreSQL or
in-process structures, but they must raise the stable operations errors and
never leak backend details across these boundaries (DEC-001/DEC-002).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from trpc_service.observability.models import (
    AlertIncident,
    DependencyObservation,
    DiagnosticSpan,
    PlatformHealthSnapshot,
    RoleReadinessSnapshot,
    TelemetryEnvelope,
    TrustedCorrelationContext,
)
from trpc_service.observability.operational import OperationalEvent


@runtime_checkable
class CorrelationContextPort(Protocol):
    """Builds and propagates the trusted correlation identity."""

    async def start_root(
        self, channel: str, external_message_digest: str
    ) -> TrustedCorrelationContext: ...
    async def bind_tenant(
        self, context: TrustedCorrelationContext, tenant_scope: str
    ) -> TrustedCorrelationContext: ...
    async def link_attempt(
        self, context: TrustedCorrelationContext, execution_trace_id: str
    ) -> TrustedCorrelationContext: ...
    async def inject(
        self, context: TrustedCorrelationContext, carrier: dict[str, str]
    ) -> dict[str, str]: ...
    async def extract(self, carrier: dict[str, str]) -> TrustedCorrelationContext: ...


@runtime_checkable
class TelemetryRecorderPort(Protocol):
    """Records stage spans, metrics and operational events (fail-open)."""

    async def start_stage(
        self,
        context: TrustedCorrelationContext,
        stage: str,
        attributes: dict[str, Any] | None = None,
    ) -> DiagnosticSpan: ...
    async def finish_stage(
        self,
        span: DiagnosticSpan,
        outcome: str,
        error_type: str | None = None,
        retryable: bool = False,
    ) -> DiagnosticSpan: ...
    async def record_metric(
        self, metric_name: str, value: float, labels: dict[str, str] | None = None
    ) -> None: ...
    async def record_operational(self, event: OperationalEvent) -> None: ...
    async def flush(self) -> None: ...
    async def shutdown(self) -> None: ...


@runtime_checkable
class TelemetryExporterPort(Protocol):
    """Exports validated safe envelopes; collapses faults to stable results."""

    async def export(self, envelopes: list[TelemetryEnvelope]) -> str:
        """Returns one of ``success`` / ``retryable_failure`` / ``permanent_failure``."""
        ...


@runtime_checkable
class SamplingPolicyPort(Protocol):
    """Outcome-aware tail sampling decisions (DEC-001)."""

    async def decide(
        self,
        scope_digest: str,
        trace_digest: str,
        classification: str,
        config_version: int | None = None,
    ) -> str:
        """Returns ``keep_full`` or ``sample_out``; critical classes never drop."""
        ...


@runtime_checkable
class TelemetryBufferPort(Protocol):
    """Bounded in-memory priority buffer; never persists to disk or backends."""

    def offer(self, envelope: TelemetryEnvelope) -> bool: ...
    async def take_batch(self, max_count: int) -> list[TelemetryEnvelope]: ...
    def ack(self, envelope_ids: list[str]) -> None: ...
    def retry(self, envelope_ids: list[str]) -> None: ...
    def drop(self, reason: str, count: int) -> None: ...
    async def snapshot(self) -> dict[str, int]: ...


@runtime_checkable
class HealthProbePort(Protocol):
    """Role-level readiness matrix and path-level aggregation (DEC-003)."""

    async def probe_liveness(self) -> str: ...
    async def probe_dependency(self, role: str, dependency: str) -> DependencyObservation: ...
    async def evaluate_role(self, node_digest: str, role: str) -> RoleReadinessSnapshot: ...
    async def aggregate(self) -> PlatformHealthSnapshot: ...


@runtime_checkable
class AlertRepository(Protocol):
    """Deduplicated alert incident authority backed by PostgreSQL."""

    async def observe(
        self,
        rule_id: str,
        severity: str,
        scope_digest: str,
        stable_reason: str,
        evidence_digest: str | None = None,
        observed_at: datetime | None = None,
    ) -> AlertIncident: ...


@runtime_checkable
class AlertNotifierPort(Protocol):
    """At-least-once notification for firing/recovering/resolved incidents."""

    async def notify(self, incident: AlertIncident) -> str: ...


@runtime_checkable
class DiagnosticQueryPort(Protocol):
    """Authorised diagnostic lookup; never fabricates complete traces."""

    async def query(
        self, scope_digest: str, trace_digest: str | None = None
    ) -> dict[str, Any]:
        """Returns safe stage records plus a ``partial_telemetry`` marker on outage."""
        ...
