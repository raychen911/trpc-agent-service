"""Telemetry recorder and diagnostic query services (US1, DEC-001/DEC-002).

``TelemetryRecorder`` is the fail-open in-process implementation of
``TelemetryRecorderPort``: telemetry failures never propagate to business
callers.  ``DiagnosticQueryService`` implements ``DiagnosticQueryPort`` with
authorization first, a minimal access audit before any store read, tenant
scope isolation and an honest ``partial_telemetry`` marker on outage.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
from time import monotonic_ns
from typing import Any, Callable

from trpc_service.observability import taxonomy
from trpc_service.observability.context import PLATFORM_SCOPE, scope_digest_of
from trpc_service.observability.models import (
    DiagnosticSpan,
    TelemetryEnvelope,
    TrustedCorrelationContext,
    validate_span_attributes,
)
from trpc_service.observability.operational import OperationalEvent
from trpc_service.operations.operations_errors import DiagnosticAccessDenied


def node_digest_of(node_id: str) -> str:
    """Digest of a node identity; raw node names never leave as labels."""
    return sha256(f"node:{node_id}".encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True, slots=True)
class RecordedStage:
    """Store-level stage record shared by the recorder and query services."""

    trace_digest: str
    scope_digest: str
    stage: str
    outcome: str
    component: str
    error_type: str | None
    retryable: bool
    role: str
    node_digest: str
    start_ns: int
    end_ns: int
    generation: int | None = None
    configuration_version: int | None = None


@dataclass(slots=True)
class StageHandle:
    """Open stage started via the async port surface."""

    context: TrustedCorrelationContext
    stage: str
    start_ns: int
    attributes: dict[str, Any] = field(default_factory=dict)


def _correlation_scope_digest(correlation: TrustedCorrelationContext) -> str:
    if correlation.tenant_scope == PLATFORM_SCOPE:
        return scope_digest_of(PLATFORM_SCOPE)
    return correlation.tenant_scope


class TelemetryRecorder:
    """In-memory, fail-open recorder; the local authority for stage spans."""

    def __init__(
        self,
        *,
        exporter: Any | None = None,
        buffer: Any | None = None,
        sampling: Any | None = None,
    ) -> None:
        self._stages: list[RecordedStage] = []
        self._operational: list[OperationalEvent] = []
        self._metrics: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._drop_counters: dict[str, int] = {}
        self._exporter = exporter
        self._buffer = buffer
        self._sampling = sampling
        self._degraded = False
        self._span_counter = 0
        self._last_export: Any = None

    @property
    def exporter_health(self) -> str:
        """Vendor-neutral exit health: ok | retrying | down | unused."""

        if self._exporter is None:
            return "unused"
        result = self._last_export
        if result is None:
            return "ok"
        return {
            "success": "ok",
            "retryable_failure": "retrying",
            "permanent_failure": "down",
        }.get(getattr(result, "action", ""), "ok")

    # --- store surface -----------------------------------------------------

    @property
    def degraded(self) -> bool:
        return self._degraded

    def mark_degraded(self) -> None:
        """Simulate or record a telemetry outage (tests and health wiring)."""
        self._degraded = True

    def spans_for(
        self, scope_digest: str, trace_digest: str | None = None
    ) -> list[RecordedStage]:
        if self._degraded:
            return []
        return [
            span
            for span in self._stages
            if span.scope_digest == scope_digest
            and (trace_digest is None or span.trace_digest == trace_digest)
        ]

    @staticmethod
    def scope_digest(tenant_id: str) -> str:
        return scope_digest_of(tenant_id)

    def drop_counters(self) -> dict[str, int]:
        """Recorder drops merged with buffer/exporter drops (FR-009)."""

        merged = dict(self._drop_counters)
        for key, value in getattr(self._buffer, "drop_counters", lambda: {})().items():
            merged[key] = merged.get(key, 0) + value
        return merged

    # --- sync instrumentation surface (fail-open by construction) ----------

    def record_stage_now(
        self,
        correlation: TrustedCorrelationContext,
        stage: str,
        outcome: str,
        *,
        error_type: str | None = None,
        retryable: bool = False,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        try:
            self._append_stage(correlation, stage, outcome, error_type, retryable, attributes)
        except Exception:
            self._drop_counters["record_failed"] = self._drop_counters.get("record_failed", 0) + 1

    def _append_stage(
        self,
        correlation: TrustedCorrelationContext,
        stage: str,
        outcome: str,
        error_type: str | None,
        retryable: bool,
        attributes: dict[str, Any] | None,
    ) -> None:
        taxonomy.validate_stage(stage)
        taxonomy.validate_outcome(outcome)
        safe_attributes: dict[str, Any] = {}
        if attributes:
            try:
                safe_attributes = validate_span_attributes(dict(attributes))
            except ValueError:
                safe_attributes = {}
        now_ns = monotonic_ns()
        self._span_counter += 1
        self._stages.append(
            RecordedStage(
                trace_digest=correlation.trace_digest,
                scope_digest=_correlation_scope_digest(correlation),
                stage=stage,
                outcome=outcome,
                component=taxonomy.stage_component(stage),
                error_type=error_type,
                retryable=retryable,
                role=correlation.role,
                node_digest=node_digest_of(correlation.node_id),
                start_ns=now_ns,
                end_ns=now_ns,
                generation=None,
                configuration_version=None,
            )
        )
        del safe_attributes  # reserved for the sanitizing exporter path

    # --- async port surface (TelemetryRecorderPort) -------------------------

    async def start_stage(
        self,
        context: TrustedCorrelationContext,
        stage: str,
        attributes: dict[str, Any] | None = None,
    ) -> StageHandle:
        taxonomy.validate_stage(stage)
        return StageHandle(
            context=context,
            stage=stage,
            start_ns=monotonic_ns(),
            attributes=dict(attributes or {}),
        )

    async def finish_stage(
        self,
        handle: StageHandle,
        outcome: str,
        error_type: str | None = None,
        retryable: bool = False,
    ) -> RecordedStage:
        taxonomy.validate_outcome(outcome)
        self.record_stage_now(
            handle.context, handle.stage, outcome,
            error_type=error_type, retryable=retryable, attributes=handle.attributes,
        )
        return self._stages[-1]

    async def record_metric(
        self, metric_name: str, value: float, labels: dict[str, str] | None = None
    ) -> None:
        key = (metric_name, tuple(sorted((labels or {}).items())))
        self._metrics[key] = self._metrics.get(key, 0.0) + float(value)

    async def record_operational(self, event: OperationalEvent) -> None:
        self._operational.append(event)

    async def flush(self) -> dict[str, int]:
        """Drain the priority buffer through the OTLP adapter (fail-open)."""

        exported = {"stages": len(self._stages), "operational": len(self._operational),
                    "metrics": len(self._metrics)}
        if self._buffer is not None:
            critical = self._buffer.drain_critical()
            normal = self._buffer.drain_normal()
            if self._exporter is not None and (critical or normal):
                try:
                    # Critical first, then ordinary — bounded by the adapter.
                    for batch in (critical, normal):
                        if not batch:
                            continue
                        self._last_export = await self._exporter.export(batch)
                        action = getattr(self._last_export, "action", "")
                        if action == "retryable_failure":
                            for envelope in batch:
                                reoffered = TelemetryEnvelope(
                                    envelope_id=envelope.envelope_id,
                                    signal_type=envelope.signal_type,
                                    priority=envelope.priority,
                                    scope_digest=envelope.scope_digest,
                                    payload=envelope.payload,
                                    trace_digest=envelope.trace_digest,
                                    attempt_count=envelope.attempt_count + 1,
                                    created_at=envelope.created_at,
                                    expires_at=envelope.expires_at,
                                )
                                self._buffer.offer(reoffered)
                except Exception:
                    # Fail-open: export failures never propagate (FR-009).
                    self._drop_counters["record_failed"] = (
                        self._drop_counters.get("record_failed", 0) + 1
                    )
            exported["buffer_exported"] = len(critical) + len(normal)
        exported["dropped"] = sum(self.drop_counters().values())
        return exported

    async def shutdown(self) -> dict[str, int]:
        return await self.flush()


class DiagnosticQueryService:
    """Authorized, scope-isolated diagnostic lookup (DiagnosticQueryPort)."""

    def __init__(
        self,
        store: Any,
        *,
        authorizer: Callable[[str], bool] | None = None,
        access_audit: Callable[[str, str | None], Any] | None = None,
    ) -> None:
        self._store = store
        self._authorizer = authorizer
        self._access_audit = access_audit

    async def query(
        self, scope_digest: str, trace_digest: str | None = None
    ) -> dict[str, Any]:
        if self._authorizer is not None and not self._authorizer(scope_digest):
            raise DiagnosticAccessDenied()
        # Minimal access audit BEFORE any diagnostic store read (FR-016).
        if self._access_audit is not None:
            await self._access_audit(scope_digest, trace_digest)
        partial = bool(getattr(self._store, "degraded", False))
        spans = self._store.spans_for(scope_digest, trace_digest) if not partial else []
        entries = [self._render(span, scope_digest) for span in spans]
        if trace_digest is not None and not partial:
            recorded = {span.stage for span in spans}
            for stage in taxonomy.STAGES:
                if stage in recorded:
                    continue
                entries.append(
                    {
                        "stage": stage,
                        "component": taxonomy.stage_component(stage),
                        "outcome": "not_applicable",
                        "error_type": None,
                        "retryable": False,
                        "role": None,
                        "node_digest": None,
                        "trace_digest": trace_digest,
                        "scope_digest": scope_digest,
                        "generation": None,
                        "configuration_version": None,
                        "start_ns": None,
                        "end_ns": None,
                    }
                )
        return {
            "trace_reference": trace_digest,
            "scope_digest": scope_digest,
            "stages": entries,
            "partial_telemetry": partial,
            "evidence_complete": not partial,
        }

    @staticmethod
    def _render(span: Any, scope_digest: str) -> dict[str, Any]:
        return {
            "stage": span.stage,
            "component": span.component,
            "outcome": span.outcome,
            "error_type": span.error_type,
            "retryable": span.retryable,
            "role": span.role,
            "node_digest": span.node_digest,
            "trace_digest": span.trace_digest,
            "scope_digest": scope_digest,
            "generation": getattr(span, "generation", None),
            "configuration_version": getattr(span, "configuration_version", None),
            "start_ns": span.start_ns,
            "end_ns": span.end_ns,
        }
