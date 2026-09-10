"""T065 RED: capacity gate — telemetry off baseline vs on, dual-gate verdict.

Same machine, same topology, same initial data: warm-up, telemetry-off
baseline, telemetry-on run; throughput, p50/p95/p99, CPU/memory and
Redis/PostgreSQL pressure peaks recorded into a machine-readable report
with no payloads or secrets (FR-023, FR-024, FR-025, SC-007, SC-008,
DEC-005).
"""

from __future__ import annotations

import importlib

import pytest


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


capacity = _load("trpc_service.operations.capacity")

pytestmark = pytest.mark.capacity


async def test_harness_runs_warmup_baseline_and_enabled_modes() -> None:
    scenario = capacity.formal_scenario()
    harness = capacity.LocalCapacityHarness()
    run_id = await harness.prepare(scenario)
    assert run_id
    baseline = await harness.run(scenario, "off")
    enabled = await harness.run(scenario, "on")
    for run in (baseline, enabled):
        assert run.successful_count + run.rejected_count + run.failed_count >= 0
        assert run.throughput > 0
        assert run.p50_ms > 0 and run.p95_ms >= run.p50_ms and run.p99_ms >= run.p95_ms
        assert run.started_at <= run.finished_at
    comparison = await harness.compare(baseline, enabled)
    assert comparison is not None


async def test_correctness_counters_are_zero_tolerant() -> None:
    scenario = capacity.formal_scenario()
    harness = capacity.LocalCapacityHarness()
    await harness.prepare(scenario)
    baseline = await harness.run(scenario, "off")
    enabled = await harness.run(scenario, "on")
    for run in (baseline, enabled):
        assert run.cross_tenant_leaks == 0, "zero cross-tenant leakage is mandatory"
        assert run.unexplained_duplicates == 0
        assert run.lost_results == 0
    comparison = await harness.compare(baseline, enabled)
    assert comparison.correctness_gate(), "the harness itself must be correct"


async def test_capacity_report_is_machine_readable_and_secret_free() -> None:
    scenario = capacity.formal_scenario()
    harness = capacity.LocalCapacityHarness()
    await harness.prepare(scenario)
    baseline = await harness.run(scenario, "off")
    enabled = await harness.run(scenario, "on")
    report = await harness.report(scenario, baseline, enabled)
    assert report["evidence_class"] == "local_evidence"
    assert "environment_fingerprint" in report
    assert "verdict" in report
    rendered = repr(report)
    for sentinel in (
        "api_key", "im_token", "db_password", "response_url",
        "phone", "email", "message_body", "secret",
    ):
        assert sentinel not in rendered, "machine-readable report carries no secrets"
