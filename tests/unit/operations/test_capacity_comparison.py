"""T064 RED: dual-gate capacity comparison with environment equivalence.

Correctness is zero tolerance: any lost result, cross-tenant leak or
unexplained duplicate fails the gate. Relative performance degrades at most
10% on throughput or p50/p95/p99. Non-equivalent environments mark the
comparison invalid without relaxing thresholds; reports separate measured
facts, derived estimates and uncovered factors and never declare production
SLAs (FR-024, FR-025, NFR-001, NFR-006, DEC-005).
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone

_NOW = datetime(2026, 9, 11, 0, 0, 0, tzinfo=timezone.utc)


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


models = _load("trpc_service.operations.models")
capacity = _load("trpc_service.operations.capacity")


def _run(**overrides):
    base = dict(
        run_id="run-1",
        environment_fingerprint="env-a",
        scenario_version="formal-v1",
        telemetry_mode="off",
        started_at=_NOW,
        finished_at=_NOW,
        successful_count=1_000,
        throughput=100.0,
        p50_ms=10.0,
        p95_ms=20.0,
        p99_ms=30.0,
        cpu_peak=0.5,
        memory_peak=0.6,
        redis_peak=0.1,
        postgres_peak=0.1,
    )
    base.update(overrides)
    return models.CapacityRun(**base)


def _comparison(**overrides):
    base = dict(
        lost_results=0,
        cross_tenant_leaks=0,
        unexplained_duplicates=0,
        throughput_delta_pct=5.0,
        p50_delta_pct=3.0,
        p95_delta_pct=4.0,
        p99_delta_pct=4.0,
    )
    base.update(overrides)
    return models.CapacityComparison(**base)


def test_correctness_gate_is_zero_tolerance() -> None:
    assert _comparison().correctness_gate()
    for field in ("lost_results", "cross_tenant_leaks", "unexplained_duplicates"):
        assert not _comparison(**{field: 1}).correctness_gate(), (
            f"any non-zero {field} fails correctness"
        )


def test_overhead_gate_allows_at_most_ten_percent() -> None:
    assert _comparison().overhead_gate()
    for field in ("throughput_delta_pct", "p50_delta_pct", "p95_delta_pct", "p99_delta_pct"):
        assert not _comparison(**{field: 10.5}).overhead_gate(), (
            f"{field} above 10% fails the overhead gate"
        )


def test_both_gates_must_pass() -> None:
    assert _comparison().passed()
    assert not _comparison(lost_results=1).passed()
    assert not _comparison(throughput_delta_pct=11.0).passed()


def test_non_equivalent_environment_marks_invalid_without_relaxing() -> None:
    comparison = capacity.compare_runs(
        _run(environment_fingerprint="env-a"),
        _run(run_id="run-2", environment_fingerprint="env-b", telemetry_mode="on"),
    )
    assert comparison is not None
    assert capacity.environment_equivalent(
        _run(environment_fingerprint="env-a"),
        _run(environment_fingerprint="env-b"),
    ) is False
    verdict = capacity.acceptance_verdict(
        _run(environment_fingerprint="env-a"),
        _run(run_id="run-2", environment_fingerprint="env-b", telemetry_mode="on"),
    )
    assert verdict == "invalid", "non-equivalent environments invalidate the run"
    assert capacity.MAX_RELATIVE_OVERHEAD_PCT <= 10.0, (
        "invalidity must never relax thresholds"
    )


def test_delta_computation_is_relative_to_baseline() -> None:
    comparison = capacity.compare_runs(
        _run(throughput=100.0, p50_ms=10.0),
        _run(run_id="run-2", telemetry_mode="on", throughput=90.0, p50_ms=11.0),
    )
    assert comparison.throughput_delta_pct == 10.0
    assert comparison.p50_delta_pct == 10.0
    assert comparison.passed(), "exactly 10% overhead still passes"


def test_report_separates_facts_estimates_and_uncovered() -> None:
    report = capacity.build_capacity_report(
        _run(), _run(run_id="run-2", telemetry_mode="on")
    )
    assert report["evidence_class"] == "local_evidence"
    assert report["measured_facts"]
    assert report["derived_estimates"]
    assert report["uncovered_factors"], "report must name what it did not measure"
    rendered = repr(report).lower()
    for forbidden in ("production sla", "absolute throughput guarantee", "sla"):
        assert forbidden not in rendered, "local evidence must not declare production SLAs"
    for sentinel in ("api_key", "im_token", "db_password", "message_body", "phone"):
        assert sentinel not in rendered, "reports carry no payloads or secrets"
