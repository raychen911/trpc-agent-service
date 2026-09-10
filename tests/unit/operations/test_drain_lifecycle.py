"""T069 RED: forward-only drain lifecycle (FR-028, DEC-003).

accepting -> draining -> drained | timed_out only; ``begin`` atomically
withdraws readiness and stops new claims; repeated SIGTERM/stop is
idempotent; after the deadline, work that cannot prove its outcome is
marked unknown and non-idempotent side effects are never replayed; the
snapshot carries inflight/completed/handed-off/unknown counts.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone

_NOW = datetime(2026, 9, 11, 0, 0, 0, tzinfo=timezone.utc)


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


drain = _load("trpc_service.operations.drain")


def _controller():
    return drain.InMemoryDrainController()


async def test_begin_is_atomic_and_stops_new_claims() -> None:
    controller = _controller()
    snapshot = await controller.begin("node-a", "worker", _NOW + timedelta(seconds=30))
    assert snapshot.state == "draining"
    assert snapshot.started_at is not None
    assert not await controller.accepts_new_claims("node-a"), (
        "begin atomically withdraws readiness / stops new claims"
    )
    # A fresh controller still accepts work.
    assert await controller.accepts_new_claims("node-b")


async def test_states_only_move_forward() -> None:
    controller = _controller()
    await controller.begin("node-a", "worker", _NOW + timedelta(seconds=30))
    snapshot = await controller.complete_or_handoff("node-a")
    assert snapshot.state == "drained"
    rejected = None
    try:
        await controller.begin("node-a", "worker", _NOW + timedelta(seconds=60))
    except ValueError:
        rejected = "rejected"
    assert rejected == "rejected", "a drained node cannot move backwards"
    final = await controller.snapshot("node-a")
    assert final.state == "drained"


async def test_repeated_begin_is_idempotent() -> None:
    controller = _controller()
    first = await controller.begin("node-a", "worker", _NOW + timedelta(seconds=30))
    second = await controller.begin("node-a", "worker", _NOW + timedelta(seconds=30))
    assert second.state == "draining"
    assert second.started_at == first.started_at, "repeat SIGTERM keeps the original window"


async def test_deadline_marks_unknown_and_never_replays() -> None:
    controller = _controller()
    await controller.begin("node-a", "worker", _NOW + timedelta(seconds=30))
    await controller.register_inflight("node-a", "exec-1")
    await controller.register_inflight("node-a", "exec-2")
    await controller.mark_completed("node-a", "exec-1")
    expired = await controller.expire("node-a", at=_NOW + timedelta(seconds=31))
    assert expired.state == "timed_out"
    assert expired.completed_count == 1
    assert expired.unknown_count == 1, (
        "work that cannot prove its outcome is marked unknown"
    )
    replay = await controller.replay_candidates("node-a")
    assert replay == [], "non-idempotent side effects are never auto-replayed"


async def test_handoff_counts_are_reported_in_snapshot() -> None:
    controller = _controller()
    await controller.begin("node-a", "worker", _NOW + timedelta(seconds=30))
    await controller.register_inflight("node-a", "exec-1")
    await controller.register_inflight("node-a", "exec-2")
    await controller.register_inflight("node-a", "exec-3")
    await controller.mark_completed("node-a", "exec-1")
    await controller.mark_handed_off("node-a", "exec-2")
    await controller.mark_handed_off("node-a", "exec-3")
    snapshot = await controller.complete_or_handoff("node-a")
    assert snapshot.state == "drained"
    assert snapshot.completed_count == 1
    assert snapshot.handed_off_count == 2
    assert snapshot.inflight_count == 0
    assert snapshot.completed_at is not None


async def test_drain_state_machine_transition_table() -> None:
    from trpc_service.operations.models import drain_transition

    assert drain_transition("accepting", "draining") == "draining"
    assert drain_transition("draining", "drained") == "drained"
    assert drain_transition("draining", "timed_out") == "timed_out"
    for illegal in (("drained", "accepting"), ("timed_out", "draining"), ("accepting", "timed_out")):
        rejected = None
        try:
            drain_transition(*illegal)
        except ValueError:
            rejected = "rejected"
        assert rejected == "rejected", f"{illegal} must be rejected"
