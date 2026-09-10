"""Operations domain models with safety invariants (data-model section 4-5).

Configuration snapshots are immutable and secret-ref-only; release state is a
bounded machine; capacity comparisons enforce the dual acceptance gates of
DEC-005; drain states only move forward.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar

# --- Secret hygiene for configuration payloads -------------------------------
# Secrets must be referenced (``*_ref``) and never embedded verbatim.
_SECRET_KEY_SUBSTRINGS: tuple[str, ...] = (
    "secret",
    "api_key",
    "apikey",
    "password",
    "credential",
)
_SECRET_VALUE_PREFIXES: tuple[str, ...] = ("sk-", "xoxb-", "ghp_", "bearer ")


def _key_is_plain_secret(key: str) -> bool:
    lowered = key.lower()
    if lowered.endswith("_ref"):
        return False
    if any(stem in lowered for stem in _SECRET_KEY_SUBSTRINGS):
        return True
    return lowered == "token" or lowered.endswith("_token")


def _contains_plain_secret(value: Any) -> bool:
    if isinstance(value, dict):
        for key, item in value.items():
            if _key_is_plain_secret(str(key)):
                return True
            if _contains_plain_secret(item):
                return True
        return False
    if isinstance(value, (list, tuple)):
        return any(_contains_plain_secret(item) for item in value)
    if isinstance(value, str):
        return value.lower().startswith(_SECRET_VALUE_PREFIXES)
    return False


# --- 4.1 Configuration snapshot (immutable, secret-ref-only) -------------------


@dataclass(frozen=True, slots=True)
class ConfigurationSnapshot:
    """Immutable, tenant-scoped configuration snapshot."""

    snapshot_id: str
    tenant_id: str
    sequence: int
    contract_version: str
    min_runtime_contract: str
    agent_config_ref: str
    governance_policy_ref: str
    data_backend_profile_ref: str
    payload_digest: str
    change_summary: str
    created_by_digest: str
    created_at: datetime
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.sequence <= 0:
            raise ValueError("sequence must be positive")
        if len(self.change_summary) > 512:
            raise ValueError("change_summary must stay bounded")
        if _contains_plain_secret(self.payload):
            raise ValueError(
                "snapshot payload must reference secrets indirectly (secret_ref)"
            )


# --- 4.2 Canary release ---------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CanaryRelease:
    """Versioned canary release with a bounded state machine (DEC-004)."""

    STATES: ClassVar[tuple[str, ...]] = (
        "draft",
        "validated",
        "canary",
        "completed",
        "paused_quality",
        "paused_insufficient_sample",
        "rolling_back",
        "rolled_back",
        "failed",
        "failed_requires_repair",
    )

    release_id: str
    candidate_snapshot_id: str
    rollback_snapshot_id: str
    created_by_digest: str
    created_at: datetime
    cohorts: tuple[str, ...] = ()
    observation_window: int = 300
    minimum_sample: int = 100
    quality_gates: tuple[dict[str, Any], ...] = ()
    hard_gate_types: tuple[str, ...] = ()
    state: str = "draft"
    revision: int = 1
    owner_fence_generation: int = 0
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.state not in self.STATES:
            raise ValueError(f"unknown release state {self.state!r}")
        if self.revision < 1:
            raise ValueError("revision must be positive")
        if self.observation_window <= 0 or self.minimum_sample <= 0:
            raise ValueError("observation_window and minimum_sample must be positive")


# --- 4.3 Tenant config route (current authority for new requests) ---------------


@dataclass(frozen=True, slots=True)
class TenantConfigRoute:
    """Per-tenant routing between stable and candidate snapshots."""

    tenant_id: str
    stable_snapshot_id: str
    route_generation: int = 1
    candidate_snapshot_id: str | None = None
    release_id: str | None = None
    owner_fence_generation: int = 0
    hard_gate_latched: bool = False
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.route_generation < 1:
            raise ValueError("route_generation must be positive")
        if self.hard_gate_latched and self.candidate_snapshot_id is not None:
            raise ValueError("hard gate latch must clear candidate routing")


# --- 4.4 Execution config pin (immutable) -----------------------------------------


@dataclass(frozen=True, slots=True)
class ExecutionConfigPin:
    """Pins one execution to one snapshot via idempotency key digest."""

    tenant_id: str
    idempotency_key_digest: str
    content_fingerprint: str
    snapshot_id: str
    route_generation: int
    release_id: str | None = None
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.route_generation < 1:
            raise ValueError("route_generation must be positive")


# --- 4.5 Release gate signal / transition event -----------------------------------


@dataclass(frozen=True, slots=True)
class ReleaseGateSignal:
    """Append-only gate observation recorded during a canary window."""

    SEVERITIES: ClassVar[tuple[str, ...]] = ("hard", "quality")

    signal_id: str
    tenant_id: str
    release_id: str
    signal_digest: str
    gate_type: str
    severity: str
    observation_window: int = 300
    sample_count: int = 0
    observed_value: float | None = None
    evidence_digest: str | None = None
    observed_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.severity not in self.SEVERITIES:
            raise ValueError(f"unknown gate severity {self.severity!r}")
        if self.observation_window <= 0:
            raise ValueError("observation_window must be positive")


@dataclass(frozen=True, slots=True)
class ReleaseTransitionEvent:
    """Append-only release transition recorded with the formal audit trail."""

    event_id: str
    release_id: str
    command_id: str
    from_state: str
    to_state: str
    from_revision: int
    to_revision: int
    actor_digest: str
    reason_code: str
    evidence_digest: str | None = None
    occurred_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.to_revision < self.from_revision:
            raise ValueError("transitions must not rewrite history backwards")


# --- 4.6 Rollback decision ----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RollbackDecision:
    """Records a rollback decision without deleting completed side effects."""

    decision_id: str
    release_id: str
    command_id: str
    actor_digest: str
    reason_code: str
    target_snapshot_id: str
    affected_tenant_count: int = 0
    from_revision: int = 1
    to_revision: int = 1
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.affected_tenant_count < 0:
            raise ValueError("affected_tenant_count must be non-negative")


# --- 5.1 Capacity scenario -----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CapacityScenario:
    """Immutable load description; the formal scenario is pinned (DEC-005)."""

    FORMAL_TENANT_COUNT: ClassVar[int] = 2
    FORMAL_WORKER_COUNT: ClassVar[int] = 2
    FORMAL_CONCURRENT_SESSIONS: ClassVar[int] = 100
    FORMAL_MESSAGES_PER_SESSION: ClassVar[int] = 10
    FORMAL_TOTAL_MESSAGES: ClassVar[int] = 1_000

    scenario_version: str
    tenant_count: int
    worker_count: int
    concurrent_sessions: int
    messages_per_session: int
    message_size_bucket: str = "medium"
    duplication_rate: float = 0.0
    tool_ratio: float = 0.0
    data_rw_ratio: float = 1.0
    seed: int = 0
    warmup_rounds: int = 1
    measurement_rounds: int = 1

    def __post_init__(self) -> None:
        for name in ("tenant_count", "worker_count", "concurrent_sessions", "messages_per_session"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")

    def total_messages(self) -> int:
        return self.concurrent_sessions * self.messages_per_session

    def is_formal_acceptance(self) -> bool:
        return (
            self.tenant_count == self.FORMAL_TENANT_COUNT
            and self.worker_count == self.FORMAL_WORKER_COUNT
            and self.concurrent_sessions == self.FORMAL_CONCURRENT_SESSIONS
            and self.messages_per_session == self.FORMAL_MESSAGES_PER_SESSION
            and self.total_messages() == self.FORMAL_TOTAL_MESSAGES
        )


# --- 5.2 Capacity run / comparison -----------------------------------------------------


@dataclass(frozen=True, slots=True)
class CapacityRun:
    """One measured capacity run; results carry no sensitive payloads."""

    run_id: str
    environment_fingerprint: str
    scenario_version: str
    telemetry_mode: str
    started_at: datetime
    finished_at: datetime
    successful_count: int = 0
    rejected_count: int = 0
    failed_count: int = 0
    lost_results: int = 0
    cross_tenant_leaks: int = 0
    unexplained_duplicates: int = 0
    throughput: float = 0.0
    p50_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    cpu_peak: float = 0.0
    memory_peak: float = 0.0
    redis_peak: float = 0.0
    postgres_peak: float = 0.0
    bottleneck: str | None = None


@dataclass(frozen=True, slots=True)
class CapacityComparison:
    """Dual-gate acceptance: correctness counts zero and overhead <= 10%."""

    MAX_RELATIVE_OVERHEAD_PCT: ClassVar[float] = 10.0

    lost_results: int
    cross_tenant_leaks: int
    unexplained_duplicates: int
    throughput_delta_pct: float
    p50_delta_pct: float
    p95_delta_pct: float
    p99_delta_pct: float
    baseline_run_id: str | None = None
    enabled_run_id: str | None = None

    def correctness_gate(self) -> bool:
        return (
            self.lost_results == 0
            and self.cross_tenant_leaks == 0
            and self.unexplained_duplicates == 0
        )

    def overhead_gate(self) -> bool:
        overhead = max(
            self.throughput_delta_pct,
            self.p50_delta_pct,
            self.p95_delta_pct,
            self.p99_delta_pct,
        )
        return overhead <= self.MAX_RELATIVE_OVERHEAD_PCT

    def passed(self) -> bool:
        return self.correctness_gate() and self.overhead_gate()


# --- 5.3 Drain snapshot -----------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DrainSnapshot:
    """Node-level drain state; states only move forward."""

    STATES: ClassVar[tuple[str, ...]] = ("accepting", "draining", "drained", "timed_out")

    node_digest: str
    role: str
    state: str
    deadline: datetime
    started_at: datetime
    inflight_count: int = 0
    completed_count: int = 0
    handed_off_count: int = 0
    unknown_count: int = 0
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.state not in self.STATES:
            raise ValueError(f"unknown drain state {self.state!r}")
        for name in ("inflight_count", "completed_count", "handed_off_count", "unknown_count"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")


_DRAIN_TRANSITIONS: dict[str, frozenset[str]] = {
    "accepting": frozenset({"draining", "drained"}),
    "draining": frozenset({"drained", "timed_out"}),
    "drained": frozenset(),
    "timed_out": frozenset(),
}


def drain_transition(current: str, target: str) -> str:
    """Forward-only drain state transition; raises on invalid moves."""
    if target not in _DRAIN_TRANSITIONS.get(current, frozenset()):
        raise ValueError(f"drain state cannot move from {current!r} to {target!r}")
    return target
