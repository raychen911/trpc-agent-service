"""T070 RED (e2e): worker drain takeover without duplicate side effects.

Worker-A withdraws readiness and stops accepting new claims; its in-flight
execution completes, is taken over by a higher fence, or is explicitly
marked unknown; Worker-B keeps serving the same tenant session; lease-expiry
takeover never produces duplicate business results (FR-026, FR-028, FR-034,
SC-008, DEC-003).
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


async def _workers():
    controller = drain.InMemoryDrainController()
    registry = drain.WorkerPool(controller)
    await registry.register("worker-a")
    await registry.register("worker-b")
    return controller, registry


async def test_draining_worker_stops_new_claims_but_peer_serves() -> None:
    controller, registry = await _workers()
    await controller.begin("worker-a", "worker", _NOW + timedelta(seconds=30))
    claimed = await registry.claim("worker-a", "exec-new")
    assert claimed is False, "draining worker must not claim new work"
    peer = await registry.claim("worker-b", "exec-new")
    assert peer is True, "Worker-B keeps serving the same tenant session"


async def test_inflight_completes_or_hands_off_or_marks_unknown() -> None:
    controller, registry = await _workers()
    await registry.claim("worker-a", "exec-1")
    await registry.claim("worker-a", "exec-2")
    await registry.claim("worker-a", "exec-3")
    await controller.begin("worker-a", "worker", _NOW + timedelta(seconds=30))
    await controller.mark_completed("worker-a", "exec-1")
    taken = await registry.take_over("worker-b", "worker-a", "exec-2", fence=5)
    assert taken is True, "a higher fence takes over the in-flight execution"
    snapshot = await controller.snapshot("worker-a")
    assert snapshot.completed_count == 1
    assert snapshot.handed_off_count == 1
    await controller.mark_handed_off("worker-a", "exec-3")
    final = await controller.complete_or_handoff("worker-a")
    assert final.state == "drained"
    assert final.handed_off_count == 2


async def test_lease_expiry_takeover_never_duplicates_business_results() -> None:
    controller, registry = await _workers()
    await registry.claim("worker-a", "exec-1")
    await registry.mark_effect("exec-1", "delivered")
    # Worker-A's lease expires; Worker-B takes over with a higher fence.
    taken = await registry.take_over("worker-b", "worker-a", "exec-1", fence=7)
    assert taken is True
    applied = await registry.apply_result("worker-b", "exec-1", "delivered")
    assert applied is False, (
        "an already-applied business effect must not be applied twice"
    )
    effects = registry.effect_log("exec-1")
    assert effects == ["delivered"], "exactly one business result recorded"


async def test_stale_fence_takeover_is_rejected() -> None:
    controller, registry = await _workers()
    await registry.claim("worker-a", "exec-1")
    await registry.take_over("worker-b", "worker-a", "exec-1", fence=7)
    stale = await registry.take_over("worker-a", "worker-b", "exec-1", fence=5)
    assert stale is False, "a lower fence must never steal the execution"
    assert registry.owner_of("exec-1") == ("worker-b", 7)
