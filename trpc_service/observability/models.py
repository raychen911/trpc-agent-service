"""Observability domain models with safety invariants (data-model section 2-3).

Every model is a frozen value object: identifiers stay opaque, tenant and node
names appear only as digests where they would otherwise leak into externally
exported telemetry, and no field ever carries message bodies, URLs, secrets or
raw exception text.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar

from trpc_service.observability import taxonomy


# --- Attribute hygiene -------------------------------------------------------
# Keys that must never appear on a DiagnosticSpan (data-model 2.2 exclusions).
_FORBIDDEN_ATTRIBUTE_STEMS: tuple[str, ...] = (
    "input",
    "output",
    "url",
    "secret",
    "api_key",
    "password",
    "token",
    "user_id",
    "tenant_id",
    "message_text",
    "request_body",
    "response_body",
)


def _is_forbidden_attribute_key(key: str) -> bool:
    lowered = key.lower()
    if lowered.startswith("state."):
        return True
    return any(stem in lowered for stem in _FORBIDDEN_ATTRIBUTE_STEMS)


def validate_span_attributes(attributes: dict[str, Any]) -> dict[str, Any]:
    """Reject attributes whose keys hint at payloads, secrets or raw ids."""
    for key in attributes:
        if _is_forbidden_attribute_key(str(key)):
            raise ValueError(f"forbidden diagnostic attribute key {key!r}")
    return attributes


# --- 2.1 Trusted correlation context ----------------------------------------


@dataclass(frozen=True, slots=True)
class TrustedCorrelationContext:
    """Request-scoped correlation identity (never persisted as-is)."""

    request_trace_id: str
    tenant_scope: str
    trace_digest: str
    node_id: str
    role: str
    otel_trace_id: str | None = None
    first_claim_trace_id: str | None = None
    owner_trace_id: str | None = None
    execution_trace_id: str | None = None
    configuration_snapshot_id: str | None = None
    route_generation: int | None = None

    def __post_init__(self) -> None:
        taxonomy.validate_role(self.role)
        if self.route_generation is not None and self.route_generation <= 0:
            raise ValueError("route_generation must be positive")


# --- 2.2 Diagnostic span ------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DiagnosticSpan:
    """Safe stage-level span produced before any network export."""

    trace_id: str
    span_id: str
    trace_digest: str
    scope_digest: str
    component: str
    stage: str
    start_ns: int
    end_ns: int
    outcome: str
    role: str
    node_digest: str
    parent_span_id: str | None = None
    error_type: str | None = None
    retryable: bool = False
    configuration_version: int | None = None
    generation: int | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        taxonomy.validate_stage(self.stage)
        taxonomy.validate_component(self.component)
        taxonomy.validate_outcome(self.outcome)
        taxonomy.validate_role(self.role)
        if self.end_ns < self.start_ns:
            raise ValueError("end_ns must not precede start_ns")
        if self.error_type is not None and len(self.error_type) > 64:
            raise ValueError("error_type must be a bounded stable code")
        if self.generation is not None and self.generation <= 0:
            raise ValueError("generation must be positive")
        if self.configuration_version is not None and self.configuration_version <= 0:
            raise ValueError("configuration_version must be positive")
        validate_span_attributes(self.attributes)


# --- 2.3 Telemetry envelope ----------------------------------------------------


@dataclass(frozen=True, slots=True)
class TelemetryEnvelope:
    """Bounded, safe signal envelope flowing through the priority buffer."""

    SIGNAL_TYPES: ClassVar[tuple[str, ...]] = (
        "trace",
        "metric",
        "log",
        "critical_summary",
        "drop_counter",
    )
    PRIORITIES: ClassVar[tuple[str, ...]] = ("critical", "normal")

    envelope_id: str
    signal_type: str
    priority: str
    scope_digest: str
    payload: dict[str, Any] = field(default_factory=dict)
    trace_digest: str | None = None
    attempt_count: int = 0
    created_at: datetime | None = None
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.signal_type not in self.SIGNAL_TYPES:
            raise ValueError(f"unknown signal type {self.signal_type!r}")
        if self.priority not in self.PRIORITIES:
            raise ValueError(f"unknown priority {self.priority!r}")
        if not self.scope_digest:
            raise ValueError("scope_digest must not be empty")
        if not 0 <= self.attempt_count <= 3:
            raise ValueError("attempt_count must stay within 0..3")

    @property
    def exhausted(self) -> bool:
        return self.attempt_count >= 3

    def with_attempt(self) -> "TelemetryEnvelope":
        return TelemetryEnvelope(
            envelope_id=self.envelope_id,
            signal_type=self.signal_type,
            priority=self.priority,
            scope_digest=self.scope_digest,
            payload=self.payload,
            trace_digest=self.trace_digest,
            attempt_count=min(self.attempt_count + 1, 3),
            created_at=self.created_at,
            expires_at=self.expires_at,
        )


# --- 2.4 Critical diagnostic summary ------------------------------------------


@dataclass(frozen=True, slots=True)
class CriticalDiagnosticSummary:
    """Fixed-size minimal summary emitted when critical space is exhausted.

    It must never be presented as a complete trace or a substitute for the
    formal audit trail (DEC-002).
    """

    trace_digest: str
    scope_digest: str
    component: str
    stage: str
    error_type: str
    retryable: bool
    configuration_version: int | None
    occurred_at: datetime


# --- 2.5 Metric definition ------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MetricDefinition:
    """Central, low-cardinality metric declaration.

    Tenant/user/session/message/trace values or digests are never allowed as
    label keys; exact tenant aggregation is handled by repository partition
    keys, not OTel labels.
    """

    name: str
    description: str
    unit: str
    instrument_type: str
    allowed_label_keys: tuple[str, ...] = ()
    label_domains: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.instrument_type not in ("counter", "gauge", "histogram"):
            raise ValueError(f"unknown instrument type {self.instrument_type!r}")
        for label_key in self.allowed_label_keys:
            if _is_forbidden_attribute_key(label_key):
                raise ValueError(f"forbidden metric label key {label_key!r}")


@dataclass(frozen=True, slots=True)
class MetricObservation:
    """A numeric observation bound to a registered metric definition."""

    metric_name: str
    value: float
    labels: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.metric_name:
            raise ValueError("metric_name must reference a registered definition")


# --- 3.1 Dependency observation -------------------------------------------------


@dataclass(frozen=True, slots=True)
class DependencyObservation:
    """Point-in-time observation of one platform-managed dependency."""

    DEPENDENCY_STATES: ClassVar[tuple[str, ...]] = ("up", "degraded", "down", "unknown")

    role: str
    dependency: str
    state: str
    stable_reason: str
    observed_at: datetime
    expires_at: datetime
    latency_bucket: str | None = None

    def __post_init__(self) -> None:
        taxonomy.validate_role(self.role)
        taxonomy.validate_dependency(self.dependency)
        if self.state not in self.DEPENDENCY_STATES:
            raise ValueError(f"unknown dependency state {self.state!r}")
        if self.expires_at < self.observed_at:
            raise ValueError("expires_at must not precede observed_at")


# --- 3.2 Role readiness snapshot -------------------------------------------------


@dataclass(frozen=True, slots=True)
class RoleReadinessSnapshot:
    """Readiness verdict for one node in one platform role (DEC-003)."""

    READINESS: ClassVar[tuple[str, ...]] = ("ready", "unready")
    SERVICE_STATES: ClassVar[tuple[str, ...]] = ("ready", "degraded", "unready")

    node_digest: str
    role: str
    liveness: str
    readiness: str
    service_state: str
    changed_at: datetime
    observed_at: datetime
    dependency_states: dict[str, str] = field(default_factory=dict)
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        taxonomy.validate_role(self.role)
        if self.readiness not in self.READINESS:
            raise ValueError(f"unknown readiness {self.readiness!r}")
        if self.service_state not in self.SERVICE_STATES:
            raise ValueError(f"unknown service state {self.service_state!r}")
        if self.liveness not in self.READINESS:
            raise ValueError(f"unknown liveness {self.liveness!r}")


# --- 3.3 Platform health snapshot --------------------------------------------------


@dataclass(frozen=True, slots=True)
class PlatformHealthSnapshot:
    """Path-level aggregated platform health (at least one safe path => degraded)."""

    SERVICE_STATES: ClassVar[tuple[str, ...]] = ("ready", "degraded", "unready")

    state: str
    generated_at: datetime
    available_paths: tuple[str, ...] = ()
    unavailable_paths: tuple[str, ...] = ()
    role_counts: dict[str, int] = field(default_factory=dict)
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.state not in self.SERVICE_STATES:
            raise ValueError(f"unknown platform state {self.state!r}")
        if self.state == "ready" and self.unavailable_paths:
            raise ValueError("ready platform cannot report unavailable paths")


# --- 3.4 Alert incident ------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AlertIncident:
    """Deduplicated alert state persisted in PostgreSQL."""

    STATES: ClassVar[tuple[str, ...]] = ("pending", "firing", "recovering", "resolved")

    incident_id: str
    fingerprint: str
    rule_id: str
    severity: str
    scope_digest: str
    state: str
    first_observed_at: datetime
    last_observed_at: datetime
    state_version: int = 1
    occurrence_count: int = 1
    evidence_digest: str | None = None
    last_notification_id: str | None = None
    resolved_at: datetime | None = None
    stable_reason: str = "unknown"

    def __post_init__(self) -> None:
        if self.state not in self.STATES:
            raise ValueError(f"unknown alert state {self.state!r}")
        if self.state_version < 1:
            raise ValueError("state_version must be positive")
        if self.occurrence_count < 0:
            raise ValueError("occurrence_count must be non-negative")
        if self.last_observed_at < self.first_observed_at:
            raise ValueError("last_observed_at must not precede first_observed_at")
        if (self.resolved_at is not None) != (self.state == "resolved"):
            raise ValueError("resolved_at is only valid in the resolved state")
