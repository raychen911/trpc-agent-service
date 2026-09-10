"""T078 RED (e2e): four repeatable fault exercises with zero invariant breaks.

OTLP/Collector down, PostgreSQL config authority down (new executions fail
closed), ordinary telemetry outage during a hard-gate latch (new requests go
last-good), worker termination drain/takeover, and clock skew with monotonic
durations. After every recovery: zero cross-tenant leaks and zero
unexplained duplicate side effects (FR-029, FR-034, SC-010,
DEC-002/3/4).
"""

from __future__ import annotations

import importlib

import pytest


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


faults = _load("trpc_service.operations.fault_exercise")

pytestmark = pytest.mark.fault_exercise


async def _harness():
    harness = faults.FaultExerciseHarness()
    await harness.setup(tenants=("tenant-alpha", "tenant-beta"))
    return harness


async def test_exercise_otlp_collector_down_and_recover() -> None:
    harness = await _harness()
    await faults.exercise_collector_down(harness)
    report = harness.exercise_report("collector_down")
    assert report["detected_within_seconds"] <= 30
    assert report["business_impact"] == "none"
    assert report["recovery_verified"] is True
    assert report["cross_tenant_leaks"] == 0
    assert report["unexplained_duplicates"] == 0


async def test_exercise_postgres_authority_down_fails_closed() -> None:
    harness = await _harness()
    await faults.exercise_postgres_authority_down(harness)
    report = harness.exercise_report("postgres_authority_down")
    assert report["new_executions"] == "fail_closed"
    assert report["fallback_used"] is False, "no fallback to defaults or cache"
    assert report["recovery_verified"] is True
    assert report["cross_tenant_leaks"] == 0


async def test_exercise_telemetry_outage_hard_gate_still_latches() -> None:
    harness = await _harness()
    await faults.exercise_telemetry_outage_hard_gate(harness)
    report = harness.exercise_report("telemetry_outage_hard_gate")
    assert report["latch_source"] == "persistent_enforcement_point", (
        "the latch must not depend on droppable telemetry"
    )
    assert report["new_requests_route"] == "last_good"
    assert report["recovery_verified"] is True


async def test_exercise_worker_termination_drain_takeover() -> None:
    harness = await _harness()
    await faults.exercise_worker_termination(harness)
    report = harness.exercise_report("worker_termination")
    assert report["drain_state"] in ("drained", "timed_out")
    assert report["taken_over_by"] == "worker-b"
    assert report["unexplained_duplicates"] == 0
    assert report["recovery_verified"] is True


async def test_exercise_clock_skew_marks_wall_time_anomalies() -> None:
    harness = await _harness()
    await faults.exercise_clock_skew(harness, skew_seconds=-120)
    report = harness.exercise_report("clock_skew")
    assert report["duration_source"] == "monotonic", (
        "durations must come from a monotonic clock"
    )
    assert report["wall_time_anomaly_marked"] is True
    assert report["cross_tenant_leaks"] == 0


async def test_all_exercises_keep_tenant_isolation_invariant() -> None:
    harness = await _harness()
    await faults.exercise_collector_down(harness)
    await faults.exercise_postgres_authority_down(harness)
    await faults.exercise_telemetry_outage_hard_gate(harness)
    await faults.exercise_worker_termination(harness)
    summary = harness.invariant_summary()
    assert summary["exercises_run"] >= 4
    assert summary["cross_tenant_leaks"] == 0
    assert summary["unexplained_duplicates"] == 0
