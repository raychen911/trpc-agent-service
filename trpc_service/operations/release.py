"""Canary release coordination: bounded state machine, CAS commands, fences.

``ReleaseCoordinator`` executes one idempotent release command per
``command_id`` inside a single store transaction: state transition(s), route
changes, rollback decisions and the formal audit commit together or not at
all. ``ReleaseSupervisor`` maps gate verdicts onto commands — a hard gate
rolls back automatically (DEC-004), quality verdicts pause for humans.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from trpc_service.operations.models import CanaryRelease, RollbackDecision
from trpc_service.operations.operations_errors import ReleaseConflict

# Eight port commands plus the two failure branches of the bounded machine.
RELEASE_ACTIONS: tuple[str, ...] = (
    "create_release",
    "validate",
    "start_canary",
    "advance",
    "pause",
    "resume",
    "rollback",
    "repair",
    "fail",
    "require_repair",
)

INSUFFICIENT_SAMPLE_REASON = "insufficient_sample"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ReleaseStateMachine:
    """Bounded transition table for ``CanaryRelease`` (DRAFT -> COMPLETED)."""

    _TRANSITIONS: dict[tuple[str, str], str] = {
        ("validate", "draft"): "validated",
        ("start_canary", "validated"): "canary",
        ("advance", "canary"): "completed",
        ("resume", "paused_quality"): "canary",
        ("resume", "paused_insufficient_sample"): "canary",
        ("complete_rollback", "rolling_back"): "rolled_back",
        ("repair", "failed_requires_repair"): "draft",
        ("require_repair", "failed"): "failed_requires_repair",
    }

    _ROLLBACK_SOURCES: frozenset[str] = frozenset(
        {"validated", "canary", "paused_quality", "paused_insufficient_sample"}
    )
    _FAIL_SOURCES: frozenset[str] = frozenset(
        {
            "draft",
            "validated",
            "canary",
            "paused_quality",
            "paused_insufficient_sample",
        }
    )

    @classmethod
    def target_state(
        cls, action: str, current: str, reason_code: str = ""
    ) -> str:
        if action == "pause":
            if current != "canary":
                raise ReleaseConflict(f"cannot pause from {current!r}")
            if reason_code == INSUFFICIENT_SAMPLE_REASON:
                return "paused_insufficient_sample"
            return "paused_quality"
        if action == "rollback":
            if current not in cls._ROLLBACK_SOURCES:
                raise ReleaseConflict(f"cannot roll back from {current!r}")
            return "rolling_back"
        if action == "fail":
            if current not in cls._FAIL_SOURCES:
                raise ReleaseConflict(f"cannot fail from {current!r}")
            return "failed"
        target = cls._TRANSITIONS.get((action, current))
        if target is None:
            raise ReleaseConflict(
                f"illegal transition {action!r} from {current!r}"
            )
        return target

    @classmethod
    def plan(
        cls, action: str, current: str, reason_code: str = ""
    ) -> list[tuple[str, str]]:
        """Ordered transition steps for one command (rollback is two-phase).

        ``target_state`` validates legality first: illegal commands raise
        ``ReleaseConflict`` before any side effect exists.
        """

        if action == "rollback":
            cls.target_state("rollback", current)
            reason = reason_code or "operator_requested"
            return [
                ("rolling_back", reason),
                ("rolled_back", "rollback_completed"),
            ]
        target = cls.target_state(action, current, reason_code)
        defaults = {
            "validate": "validated",
            "start_canary": "canary_started",
            "advance": "gates_passed",
            "resume": "authorized_resume",
            "fail": "failed",
            "require_repair": "repair_required",
            "repair": "repaired",
        }
        reason = reason_code or defaults.get(action, action)
        return [(target, reason)]


class ReleaseCoordinator:
    """Executes idempotent, fenced release commands atomically."""

    def __init__(self, store: Any) -> None:
        self._store = store

    async def create_release(self, release: CanaryRelease) -> CanaryRelease:
        return await self._store.create_release(release)

    async def execute(
        self,
        release_id: str,
        command_id: str,
        action: str,
        expected_revision: int,
        actor_digest: str,
        *,
        reason_code: str = "",
        fence_generation: int = 0,
        evidence_digest: str | None = None,
        cache: Any = None,
    ) -> CanaryRelease:
        existing = await self._store.command_result(release_id, command_id)
        if existing is not None:
            stored_action, stored_release = existing
            if stored_action != action:
                raise ReleaseConflict("command_id reused for a different action")
            return stored_release

        release = await self._store.get_release(release_id)
        if fence_generation < release.owner_fence_generation:
            from trpc_service.operations.operations_errors import StaleReleaseFence

            raise StaleReleaseFence("writer fence below the highest seen generation")
        if expected_revision != release.revision:
            raise ReleaseConflict("stale expected_revision for CAS transition")
        plan = ReleaseStateMachine.plan(action, release.state, reason_code)

        current = release
        async with self._store.transaction():
            for to_state, step_reason in plan:
                current = await self._store.apply_transition(
                    release_id=release_id,
                    command_id=command_id,
                    action=action,
                    to_state=to_state,
                    expected_revision=current.revision,
                    fence_generation=fence_generation,
                    actor_digest=actor_digest,
                    reason_code=step_reason,
                    evidence_digest=evidence_digest,
                )
            if action == "rollback":
                await self._apply_rollback(
                    release, command_id, actor_digest,
                    reason_code or "operator_requested",
                )
            elif action == "advance":
                await self._promote_routes(release)
            await self._store.record_command_result(
                release_id, command_id, action, current
            )

        if cache is not None:
            await self._refresh_cache(release, cache)
        return current

    # --- command internals ---------------------------------------------------

    async def _apply_rollback(
        self,
        release: CanaryRelease,
        command_id: str,
        actor_digest: str,
        reason_code: str,
    ) -> None:
        affected = 0
        for tenant_id in release.cohorts:
            route = await self._store.get_route(tenant_id)
            if route is None:
                continue
            if (
                route.candidate_snapshot_id == release.candidate_snapshot_id
                or route.hard_gate_latched
            ):
                affected += 1
                await self._store.latch_hard_gate(
                    tenant_id, actor_digest=actor_digest, reason_code=reason_code
                )
        await self._store.record_rollback_decision(
            RollbackDecision(
                decision_id=str(uuid.uuid4()),
                release_id=release.release_id,
                command_id=command_id,
                actor_digest=actor_digest,
                reason_code=reason_code,
                target_snapshot_id=release.rollback_snapshot_id,
                affected_tenant_count=affected,
                from_revision=release.revision,
                to_revision=release.revision + 2,
                created_at=_now(),
            )
        )

    async def _promote_routes(self, release: CanaryRelease) -> None:
        for tenant_id in release.cohorts:
            route = await self._store.get_route(tenant_id)
            if route is None:
                continue
            if route.candidate_snapshot_id == release.candidate_snapshot_id:
                await self._store.promote_route(
                    tenant_id, route.route_generation, release.candidate_snapshot_id
                )

    async def _refresh_cache(self, release: CanaryRelease, cache: Any) -> None:
        """Post-commit refresh only: cache failures never touch authority."""

        for tenant_id in release.cohorts:
            try:
                route = await self._store.get_route(tenant_id)
                if route is not None:
                    await cache.refresh(route)
            except Exception:  # noqa: BLE001 - degraded cache is non-fatal
                self._note_degraded(tenant_id)

    def _note_degraded(self, tenant_id: str) -> None:
        notes = getattr(self._store, "degraded_notes", None)
        if notes is not None:
            notes.append(f"route_cache_refresh_failed:{tenant_id}")


class ReleaseSupervisor:
    """Maps gate verdicts onto release commands (DEC-004 automation)."""

    def __init__(self, store: Any, coordinator: ReleaseCoordinator) -> None:
        self._store = store
        self._coordinator = coordinator

    def evaluate_release(self, release: CanaryRelease) -> str:
        """Synchronous verdict over the recorded signals of one release."""

        from trpc_service.operations.gates import GateEvaluator

        signals = self._store.gate_signals_sync(release.release_id)
        hard = [signal for signal in signals if signal.severity == "hard"]
        quality = [signal for signal in signals if signal.severity == "quality"]
        window_elapsed = False
        if signals:
            latest = max(
                signal.observed_at or _now() for signal in signals
            )
            window_elapsed = (
                latest - release.created_at
            ).total_seconds() >= release.observation_window
        sample_count = max(
            (signal.sample_count for signal in quality), default=0
        )
        evaluator = GateEvaluator(
            observation_window=release.observation_window,
            minimum_sample=release.minimum_sample,
            quality_gates=release.quality_gates,
        )
        return evaluator.evaluate(
            hard_signals=hard,
            quality_signals=quality,
            window_elapsed=window_elapsed,
            sample_count=sample_count,
        )

    async def apply_quality_verdict(
        self,
        release_id: str,
        command_id: str,
        verdict: str,
        *,
        actor_digest: str = "system",
        fence_generation: int = 0,
    ) -> CanaryRelease:
        release = await self._store.get_release(release_id)
        if verdict == "quality_pause":
            return await self._coordinator.execute(
                release_id, command_id, "pause", release.revision, actor_digest,
                reason_code="quality_threshold_breached",
                fence_generation=fence_generation,
            )
        if verdict == "insufficient_sample":
            return await self._coordinator.execute(
                release_id, command_id, "pause", release.revision, actor_digest,
                reason_code=INSUFFICIENT_SAMPLE_REASON,
                fence_generation=fence_generation,
            )
        if verdict == "pass":
            return await self._coordinator.execute(
                release_id, command_id, "advance", release.revision, actor_digest,
                fence_generation=fence_generation,
            )
        if verdict == "hard_stop":
            return await self.apply_hard_gate(
                release_id, command_id,
                actor_digest=actor_digest, fence_generation=fence_generation,
            )
        raise ReleaseConflict(f"unknown gate verdict {verdict!r}")

    async def apply_hard_gate(
        self,
        release_id: str,
        command_id: str,
        *,
        actor_digest: str = "system",
        fence_generation: int = 0,
    ) -> CanaryRelease:
        """Hard gate: automatic rollback, no human in the loop (DEC-004)."""

        release = await self._store.get_release(release_id)
        return await self._coordinator.execute(
            release_id, command_id, "rollback", release.revision, actor_digest,
            reason_code="hard_gate_triggered",
            fence_generation=fence_generation,
        )
