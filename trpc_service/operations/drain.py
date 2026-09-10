"""Drain lifecycle: forward-only node shutdown with takeover safety (FR-028).

``InMemoryDrainController`` tracks accepting -> draining -> drained |
timed_out; ``begin`` atomically withdraws readiness; repeated SIGTERM is
idempotent; after the deadline unprovable work is marked unknown and never
auto-replayed. ``WorkerPool`` coordinates claims, fenced takeover and
exactly-once business effects across two workers (DEC-003).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from trpc_service.operations.models import DrainSnapshot, drain_transition


def _now() -> datetime:
    return datetime.now(timezone.utc)


class InMemoryDrainController:
    """Process-local drain controller (DrainControllerPort semantics)."""

    def __init__(self) -> None:
        self._states: dict[str, str] = {}
        self._deadlines: dict[str, datetime] = {}
        self._started: dict[str, datetime] = {}
        self._inflight: dict[str, set[str]] = {}
        self._completed: dict[str, set[str]] = {}
        self._handed_off: dict[str, set[str]] = {}
        self._unknown: dict[str, set[str]] = {}
        self._completed_at: dict[str, datetime] = {}

    async def accepts_new_claims(self, node_digest: str) -> bool:
        return self._states.get(node_digest, "accepting") == "accepting"

    async def begin(self, node_digest: str, role: str, deadline: datetime) -> DrainSnapshot:
        current = self._states.get(node_digest, "accepting")
        if current == "draining":
            return await self.snapshot(node_digest)  # idempotent repeat SIGTERM
        if current != "accepting":
            raise ValueError(f"cannot begin drain from {current!r}")
        drain_transition("accepting", "draining")
        self._states[node_digest] = "draining"
        self._deadlines[node_digest] = deadline
        self._started[node_digest] = _now()
        self._inflight.setdefault(node_digest, set())
        self._completed.setdefault(node_digest, set())
        self._handed_off.setdefault(node_digest, set())
        self._unknown.setdefault(node_digest, set())
        return await self.snapshot(node_digest)

    async def register_inflight(self, node_digest: str, execution_id: str) -> None:
        self._inflight.setdefault(node_digest, set()).add(execution_id)

    async def mark_completed(self, node_digest: str, execution_id: str) -> None:
        self._inflight.setdefault(node_digest, set()).discard(execution_id)
        self._completed.setdefault(node_digest, set()).add(execution_id)

    async def mark_handed_off(self, node_digest: str, execution_id: str) -> None:
        self._inflight.setdefault(node_digest, set()).discard(execution_id)
        self._handed_off.setdefault(node_digest, set()).add(execution_id)

    async def complete_or_handoff(self, node_digest: str) -> DrainSnapshot:
        current = self._states.get(node_digest, "accepting")
        if current == "drained":
            return await self.snapshot(node_digest)
        if current != "draining":
            raise ValueError(f"cannot complete drain from {current!r}")
        remaining = self._inflight.get(node_digest, set())
        if remaining:
            # Work that cannot prove its outcome yet stays inflight; the
            # deadline path (expire) marks it unknown instead of replaying.
            raise ValueError(f"{len(remaining)} executions still inflight")
        drain_transition("draining", "drained")
        self._states[node_digest] = "drained"
        self._completed_at[node_digest] = _now()
        return await self.snapshot(node_digest)

    async def expire(self, node_digest: str, *, at: datetime | None = None) -> DrainSnapshot:
        current = self._states.get(node_digest, "accepting")
        if current != "draining":
            raise ValueError(f"cannot expire drain from {current!r}")
        # Everything still inflight at the deadline is UNKNOWN, never replayed.
        remaining = self._inflight.get(node_digest, set())
        self._unknown.setdefault(node_digest, set()).update(remaining)
        self._inflight[node_digest] = set()
        drain_transition("draining", "timed_out")
        self._states[node_digest] = "timed_out"
        self._completed_at[node_digest] = at or _now()
        return await self.snapshot(node_digest)

    async def replay_candidates(self, node_digest: str) -> list[str]:
        """Non-idempotent side effects are NEVER auto-replay candidates."""

        return []

    async def snapshot(self, node_digest: str) -> DrainSnapshot:
        state = self._states.get(node_digest, "accepting")
        if state == "accepting":
            return DrainSnapshot(
                node_digest=node_digest,
                role="worker",
                state="accepting",
                deadline=_now(),
                started_at=_now(),
            )
        return DrainSnapshot(
            node_digest=node_digest,
            role="worker",
            state=state,
            deadline=self._deadlines[node_digest],
            started_at=self._started[node_digest],
            inflight_count=len(self._inflight.get(node_digest, set())),
            completed_count=len(self._completed.get(node_digest, set())),
            handed_off_count=len(self._handed_off.get(node_digest, set())),
            unknown_count=len(self._unknown.get(node_digest, set())),
            completed_at=self._completed_at.get(node_digest),
        )


class WorkerPool:
    """Two-worker claim/takeover coordination with exactly-once effects."""

    def __init__(self, controller: InMemoryDrainController) -> None:
        self._controller = controller
        self._workers: set[str] = set()
        self._owners: dict[str, tuple[str, int]] = {}  # exec -> (worker, fence)
        self._effects: dict[str, list[str]] = {}

    async def register(self, worker: str) -> None:
        self._workers.add(worker)

    async def claim(self, worker: str, execution_id: str) -> bool:
        if worker not in self._workers:
            return False
        if not await self._controller.accepts_new_claims(worker):
            return False  # draining workers stop claiming immediately
        if execution_id in self._owners:
            return False
        self._owners[execution_id] = (worker, 1)
        await self._controller.register_inflight(worker, execution_id)
        return True

    async def take_over(
        self, new_worker: str, old_worker: str, execution_id: str, fence: int
    ) -> bool:
        current = self._owners.get(execution_id)
        current_fence = current[1] if current else 0
        if fence <= current_fence:
            return False  # stale fence never steals the execution
        if old_worker in self._workers:
            await self._controller.mark_handed_off(old_worker, execution_id)
        self._owners[execution_id] = (new_worker, fence)
        return True

    async def mark_effect(self, execution_id: str, effect: str) -> None:
        self._effects.setdefault(execution_id, []).append(effect)

    async def apply_result(self, worker: str, execution_id: str, effect: str) -> bool:
        """Idempotent effect application: never applies the same effect twice."""

        applied = self._effects.get(execution_id, [])
        if effect in applied:
            return False
        applied.append(effect)
        self._effects[execution_id] = applied
        return True

    def owner_of(self, execution_id: str) -> tuple[str, int] | None:
        return self._owners.get(execution_id)

    def effect_log(self, execution_id: str) -> list[str]:
        return list(self._effects.get(execution_id, []))
