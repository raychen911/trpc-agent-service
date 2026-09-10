"""Transactional in-memory operations store (offline authority for US4).

Implements the snapshot/route/release repository semantics defined by the
operations contracts on top of one process-local transactional store:
every command (release transition, route change, latch, pin, decision,
audit row) commits atomically, rolls back on failure and supports simulated
crash points for recovery testing (DEC-004).
"""

from __future__ import annotations

import copy
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from trpc_service.operations.canonical import canonical_payload_digest, contract_rank
from trpc_service.operations.models import (
    CanaryRelease,
    ConfigurationSnapshot,
    ExecutionConfigPin,
    ReleaseGateSignal,
    ReleaseTransitionEvent,
    RollbackDecision,
    TenantConfigRoute,
    _contains_plain_secret,
)
from trpc_service.operations.operations_errors import (
    ReleaseConflict,
    ReleaseNotFound,
    ReleaseStateUnavailable,
    SnapshotDigestMismatch,
    SnapshotInvalid,
    StaleReleaseFence,
)


class SimulatedCrash(RuntimeError):
    """Raised at a simulated crash point; never leaks to business callers."""


class AuditWriteUnavailable(RuntimeError):
    """The formal audit trail refused the write; the command must abort."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uuid() -> str:
    return str(uuid.uuid4())


class InMemoryOperationsStore:
    """Single-authority, transaction-scoped in-memory operations store."""

    def __init__(self) -> None:
        self.available: bool = True
        self.snapshots: dict[tuple[str, str], ConfigurationSnapshot] = {}
        self.releases: dict[str, CanaryRelease] = {}
        self.routes: dict[str, TenantConfigRoute] = {}
        self.pins: dict[tuple[str, str], ExecutionConfigPin] = {}
        self._gate_signals: dict[tuple[str, str], ReleaseGateSignal] = {}
        self._transition_events: list[ReleaseTransitionEvent] = []
        self._rollback_decisions: list[RollbackDecision] = []
        self.audit_records: list[dict[str, Any]] = []
        self.command_results: dict[tuple[str, str], tuple[str, CanaryRelease]] = {}
        # Test seams -----------------------------------------------------
        self.audit_failure_countdown: int | None = None
        self.crash_mode: str | None = None  # "before_commit" | "after_commit"
        self.degraded_notes: list[str] = []
        self._tx_depth: int = 0

    # --- availability -----------------------------------------------------

    def set_available(self, available: bool) -> None:
        self.available = available

    def _require_available(self) -> None:
        if not self.available:
            raise ReleaseStateUnavailable("operations authority unavailable")

    # --- transactions -----------------------------------------------------

    def _checkpoint(self) -> dict[str, Any]:
        return {
            "snapshots": copy.deepcopy(self.snapshots),
            "releases": copy.deepcopy(self.releases),
            "routes": copy.deepcopy(self.routes),
            "pins": copy.deepcopy(self.pins),
            "gate_signals": copy.deepcopy(self._gate_signals),
            "transition_events": copy.deepcopy(self._transition_events),
            "rollback_decisions": copy.deepcopy(self._rollback_decisions),
            "audit_records": copy.deepcopy(self.audit_records),
            "command_results": copy.deepcopy(self.command_results),
        }

    def _restore(self, checkpoint: dict[str, Any]) -> None:
        self.snapshots = checkpoint["snapshots"]
        self.releases = checkpoint["releases"]
        self.routes = checkpoint["routes"]
        self.pins = checkpoint["pins"]
        self._gate_signals = checkpoint["gate_signals"]
        self._transition_events = checkpoint["transition_events"]
        self._rollback_decisions = checkpoint["rollback_decisions"]
        self.audit_records = checkpoint["audit_records"]
        self.command_results = checkpoint["command_results"]

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """Atomic unit of work: rollback on error, optional crash points."""

        if self._tx_depth == 0:
            checkpoint = self._checkpoint()
            self._tx_depth = 1
            try:
                yield
            except BaseException:
                self._tx_depth = 0
                self._restore(checkpoint)
                raise
            if self.crash_mode == "before_commit":
                self.crash_mode = None
                self._tx_depth = 0
                self._restore(checkpoint)
                raise SimulatedCrash("crash before commit")
            self._tx_depth = 0
            if self.crash_mode == "after_commit":
                self.crash_mode = None
                raise SimulatedCrash("crash after commit (response lost)")
        else:
            self._tx_depth += 1
            try:
                yield
            finally:
                self._tx_depth -= 1

    # --- formal audit ------------------------------------------------------

    async def record_audit(self, entry: dict[str, Any]) -> None:
        self._require_available()
        if self.audit_failure_countdown is not None:
            if self.audit_failure_countdown <= 1:
                self.audit_failure_countdown = None
                raise AuditWriteUnavailable("formal audit refused the write")
            self.audit_failure_countdown -= 1
        self.audit_records.append(dict(entry))

    # --- snapshots -----------------------------------------------------------

    async def create_snapshot(
        self, snapshot: ConfigurationSnapshot
    ) -> ConfigurationSnapshot:
        self._require_available()
        if _contains_plain_secret(snapshot.payload):
            raise SnapshotInvalid("payload may only reference secrets indirectly")
        digest = canonical_payload_digest(snapshot.payload)
        if digest != snapshot.payload_digest:
            raise SnapshotDigestMismatch("declared digest does not match payload")
        key = (snapshot.tenant_id, snapshot.snapshot_id)
        existing = self.snapshots.get(key)
        if existing is not None:
            if existing.payload_digest != snapshot.payload_digest:
                raise SnapshotDigestMismatch("immutable insert conflict")
            return existing
        await self.record_audit(
            {
                "action": "snapshot_create",
                "tenant_id": snapshot.tenant_id,
                "snapshot_id": snapshot.snapshot_id,
                "actor_digest": snapshot.created_by_digest,
                "occurred_at": _now().isoformat(),
            }
        )
        self.snapshots[key] = snapshot
        return snapshot

    async def get_snapshot(
        self, tenant_id: str, snapshot_id: str
    ) -> ConfigurationSnapshot | None:
        self._require_available()
        snapshot = self.snapshots.get((tenant_id, snapshot_id))
        if snapshot is None:
            return None
        if canonical_payload_digest(snapshot.payload) != snapshot.payload_digest:
            raise SnapshotDigestMismatch("stored snapshot failed digest verification")
        return snapshot

    async def verify_compatible(
        self, tenant_id: str, snapshot_id: str, runtime_contract: str
    ) -> bool:
        snapshot = await self.get_snapshot(tenant_id, snapshot_id)
        if snapshot is None:
            return False
        return contract_rank(snapshot.min_runtime_contract) <= contract_rank(
            runtime_contract
        )

    # --- releases ---------------------------------------------------------------

    async def create_release(self, release: CanaryRelease) -> CanaryRelease:
        self._require_available()
        if release.release_id in self.releases:
            existing = self.releases[release.release_id]
            if existing == release:
                return existing
            raise ReleaseConflict("release id already in use")
        await self.record_audit(
            {
                "action": "release_create",
                "release_id": release.release_id,
                "actor_digest": release.created_by_digest,
                "occurred_at": _now().isoformat(),
            }
        )
        self.releases[release.release_id] = release
        return release

    async def get_release(self, release_id: str) -> CanaryRelease:
        self._require_available()
        release = self.releases.get(release_id)
        if release is None:
            raise ReleaseNotFound("release not found")
        return release

    async def apply_transition(
        self,
        *,
        release_id: str,
        command_id: str,
        action: str,
        to_state: str,
        expected_revision: int,
        fence_generation: int,
        actor_digest: str,
        reason_code: str,
        evidence_digest: str | None = None,
    ) -> CanaryRelease:
        self._require_available()
        release = self.releases.get(release_id)
        if release is None:
            raise ReleaseNotFound("release not found")
        if fence_generation < release.owner_fence_generation:
            raise StaleReleaseFence("writer fence below the highest seen generation")
        if expected_revision != release.revision:
            raise ReleaseConflict("stale expected_revision for CAS transition")
        new_release = CanaryRelease(
            release_id=release.release_id,
            candidate_snapshot_id=release.candidate_snapshot_id,
            rollback_snapshot_id=release.rollback_snapshot_id,
            created_by_digest=release.created_by_digest,
            created_at=release.created_at,
            cohorts=release.cohorts,
            observation_window=release.observation_window,
            minimum_sample=release.minimum_sample,
            quality_gates=release.quality_gates,
            hard_gate_types=release.hard_gate_types,
            state=to_state,
            revision=release.revision + 1,
            owner_fence_generation=max(
                fence_generation, release.owner_fence_generation
            ),
            updated_at=_now(),
        )
        self.releases[release_id] = new_release
        event = ReleaseTransitionEvent(
            event_id=_uuid(),
            release_id=release_id,
            command_id=command_id,
            from_state=release.state,
            to_state=to_state,
            from_revision=release.revision,
            to_revision=new_release.revision,
            actor_digest=actor_digest,
            reason_code=reason_code,
            evidence_digest=evidence_digest,
            occurred_at=_now(),
        )
        self._transition_events.append(event)
        await self.record_audit(
            {
                "action": action,
                "release_id": release_id,
                "command_id": command_id,
                "actor_digest": actor_digest,
                "reason_code": reason_code,
                "to_state": to_state,
                "occurred_at": _now().isoformat(),
            }
        )
        return new_release

    async def transition_events(
        self, release_id: str
    ) -> list[ReleaseTransitionEvent]:
        self._require_available()
        return [
            event for event in self._transition_events if event.release_id == release_id
        ]

    async def command_result(
        self, release_id: str, command_id: str
    ) -> tuple[str, CanaryRelease] | None:
        self._require_available()
        return self.command_results.get((release_id, command_id))

    async def record_command_result(
        self, release_id: str, command_id: str, action: str, release: CanaryRelease
    ) -> None:
        self._require_available()
        self.command_results[(release_id, command_id)] = (action, release)

    # --- routes ---------------------------------------------------------------

    async def set_route(self, route: TenantConfigRoute) -> TenantConfigRoute:
        self._require_available()
        self.routes[route.tenant_id] = route
        return route

    async def get_route(self, tenant_id: str) -> TenantConfigRoute | None:
        self._require_available()
        return self.routes.get(tenant_id)

    async def compare_and_route(
        self,
        tenant_id: str,
        expected_generation: int,
        candidate_snapshot_id: str | None,
        release_id: str | None = None,
        fence_generation: int = 0,
    ) -> TenantConfigRoute:
        self._require_available()
        route = self.routes.get(tenant_id)
        if route is None:
            raise ReleaseStateUnavailable("tenant has no authoritative route")
        if route.route_generation != expected_generation:
            raise ReleaseConflict("route generation moved on")
        if candidate_snapshot_id is not None and route.hard_gate_latched:
            raise ReleaseConflict("cannot route candidate while hard gate latched")
        if fence_generation < route.owner_fence_generation:
            raise StaleReleaseFence("route writer fenced out")
        updated = TenantConfigRoute(
            tenant_id=tenant_id,
            stable_snapshot_id=route.stable_snapshot_id,
            route_generation=route.route_generation + 1,
            candidate_snapshot_id=candidate_snapshot_id,
            release_id=release_id if candidate_snapshot_id else None,
            owner_fence_generation=max(fence_generation, route.owner_fence_generation),
            hard_gate_latched=route.hard_gate_latched,
            updated_at=_now(),
        )
        self.routes[tenant_id] = updated
        return updated

    async def promote_route(
        self, tenant_id: str, expected_generation: int, new_stable_snapshot_id: str
    ) -> TenantConfigRoute:
        """Candidate promoted to stable after a completed release."""

        self._require_available()
        route = self.routes.get(tenant_id)
        if route is None:
            raise ReleaseStateUnavailable("tenant has no authoritative route")
        if route.route_generation != expected_generation:
            raise ReleaseConflict("route generation moved on")
        updated = TenantConfigRoute(
            tenant_id=tenant_id,
            stable_snapshot_id=new_stable_snapshot_id,
            route_generation=route.route_generation + 1,
            candidate_snapshot_id=None,
            release_id=None,
            owner_fence_generation=route.owner_fence_generation,
            hard_gate_latched=False,
            updated_at=_now(),
        )
        self.routes[tenant_id] = updated
        return updated

    async def latch_hard_gate(
        self,
        tenant_id: str,
        *,
        signal: ReleaseGateSignal | None = None,
        actor_digest: str,
        reason_code: str = "hard_gate_triggered",
    ) -> TenantConfigRoute | None:
        """Persist the latch: candidate routing stops immediately (DEC-004)."""

        self._require_available()
        if signal is not None:
            await self.record_gate_signal(signal)
        route = self.routes.get(tenant_id)
        if route is None:
            raise ReleaseStateUnavailable("tenant has no authoritative route")
        if route.hard_gate_latched and route.candidate_snapshot_id is None:
            return route  # idempotent: latch applied exactly once
        updated = TenantConfigRoute(
            tenant_id=tenant_id,
            stable_snapshot_id=route.stable_snapshot_id,
            route_generation=route.route_generation + 1,
            candidate_snapshot_id=None,
            release_id=None,
            owner_fence_generation=route.owner_fence_generation,
            hard_gate_latched=True,
            updated_at=_now(),
        )
        self.routes[tenant_id] = updated
        await self.record_audit(
            {
                "action": "hard_gate_latch",
                "tenant_id": tenant_id,
                "actor_digest": actor_digest,
                "reason_code": reason_code,
                "occurred_at": _now().isoformat(),
            }
        )
        return updated

    # --- pins -------------------------------------------------------------------

    async def get_pin(
        self, tenant_id: str, idempotency_key_digest: str
    ) -> ExecutionConfigPin | None:
        return self.pins.get((tenant_id, idempotency_key_digest))

    async def create_pin(self, pin: ExecutionConfigPin) -> ExecutionConfigPin:
        self._require_available()
        key = (pin.tenant_id, pin.idempotency_key_digest)
        existing = self.pins.get(key)
        if existing is not None:
            if existing.content_fingerprint != pin.content_fingerprint:
                raise SnapshotDigestMismatch("pin fingerprint conflict")
            return existing
        stored = ExecutionConfigPin(
            tenant_id=pin.tenant_id,
            idempotency_key_digest=pin.idempotency_key_digest,
            content_fingerprint=pin.content_fingerprint,
            snapshot_id=pin.snapshot_id,
            route_generation=pin.route_generation,
            release_id=pin.release_id,
            created_at=_now(),
        )
        self.pins[key] = stored
        return stored

    # --- gate signals -----------------------------------------------------------

    async def record_gate_signal(
        self, signal: ReleaseGateSignal
    ) -> ReleaseGateSignal:
        self._require_available()
        key = (signal.tenant_id, signal.signal_digest)
        existing = self._gate_signals.get(key)
        if existing is not None:
            return existing
        self._gate_signals[key] = signal
        return signal

    async def gate_signals(self, release_id: str) -> list[ReleaseGateSignal]:
        self._require_available()
        return [
            signal for signal in self._gate_signals.values()
            if signal.release_id == release_id
        ]

    def gate_signals_sync(self, release_id: str) -> list[ReleaseGateSignal]:
        """Sync mirror for supervisors already holding the release object."""

        self._require_available()
        return [
            signal for signal in self._gate_signals.values()
            if signal.release_id == release_id
        ]

    # --- rollback decisions ---------------------------------------------------------

    async def record_rollback_decision(self, decision: RollbackDecision) -> None:
        self._require_available()
        self._rollback_decisions.append(decision)

    async def rollback_decisions(self, release_id: str) -> list[RollbackDecision]:
        self._require_available()
        return [
            decision for decision in self._rollback_decisions
            if decision.release_id == release_id
        ]
