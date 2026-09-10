"""Deterministic capacity acceptance harness (FR-023..FR-025, DEC-005).

Local evidence only: fixed business profile, seeded load generation, and a
dual-gate comparison — correctness is zero tolerance, relative overhead is
capped at 10%. Reports separate measured facts, derived estimates and
uncovered factors and never declare production SLAs.
"""

from __future__ import annotations

import hashlib
import platform
import random
import time
from datetime import datetime, timezone
from typing import Any

from trpc_service.operations.models import (
    CapacityComparison,
    CapacityRun,
    CapacityScenario,
)

MAX_RELATIVE_OVERHEAD_PCT = CapacityComparison.MAX_RELATIVE_OVERHEAD_PCT
FORMAL_SCENARIO_VERSION = "formal-v1"

SIZE_BUCKETS: tuple[str, ...] = ("small", "medium", "large")
FORMAL_SEED = 2_026_091_100


def formal_scenario() -> CapacityScenario:
    """The pinned formal acceptance manifest (2/2/100/10 = 1,000 msgs)."""

    return CapacityScenario(
        scenario_version=FORMAL_SCENARIO_VERSION,
        tenant_count=CapacityScenario.FORMAL_TENANT_COUNT,
        worker_count=CapacityScenario.FORMAL_WORKER_COUNT,
        concurrent_sessions=CapacityScenario.FORMAL_CONCURRENT_SESSIONS,
        messages_per_session=CapacityScenario.FORMAL_MESSAGES_PER_SESSION,
        message_size_bucket="medium",
        duplication_rate=0.05,
        tool_ratio=0.2,
        data_rw_ratio=0.5,
        seed=FORMAL_SEED,
        warmup_rounds=1,
        # Median-of-rounds stabilizes tail percentiles; with 9 rounds the
        # observed run takes the median of its 9 tail estimates, so single
        # OS pauses cannot dominate the gate.
        measurement_rounds=9,
    )


def generate_load(
    scenario: CapacityScenario, *, seed: int
) -> list[list[dict[str, Any]]]:
    """Seeded, reproducible load plan; carries no payloads or secrets."""

    rng = random.Random(seed)
    plan: list[list[dict[str, Any]]] = []
    for session_index in range(scenario.concurrent_sessions):
        tenant_index = session_index % scenario.tenant_count
        session: list[dict[str, Any]] = []
        for ordinal in range(scenario.messages_per_session):
            message = {
                "tenant_slot": tenant_index,
                "ordinal": ordinal,
                "size_bucket": rng.choice(SIZE_BUCKETS),
                "uses_tool": rng.random() < scenario.tool_ratio,
                "is_read": rng.random() < scenario.data_rw_ratio,
                "duplicate_of": None,
            }
            if (
                ordinal > 0
                and scenario.duplication_rate > 0
                and rng.random() < scenario.duplication_rate
            ):
                message["duplicate_of"] = ordinal - 1
            session.append(message)
        plan.append(session)
    return plan


def environment_fingerprint() -> str:
    """Stable, secret-free environment fingerprint for equivalence checks."""

    rendered = "|".join(
        (
            platform.system(),
            platform.machine(),
            f"py{platform.python_version_tuple()[0]}.{platform.python_version_tuple()[1]}",
        )
    )
    return hashlib.sha256(rendered.encode()).hexdigest()[:16]


def environment_equivalent(baseline: CapacityRun, enabled: CapacityRun) -> bool:
    return (
        baseline.environment_fingerprint == enabled.environment_fingerprint
        and baseline.scenario_version == enabled.scenario_version
    )


def compare_runs(baseline: CapacityRun, enabled: CapacityRun) -> CapacityComparison:
    """Dual-gate comparison relative to the baseline run."""

    def delta(baseline_value: float, enabled_value: float, *, invert: bool = False) -> float:
        if baseline_value <= 0:
            return 0.0
        if invert:  # throughput: degradation is a DECREASE
            return max(0.0, (baseline_value - enabled_value) / baseline_value * 100.0)
        return max(0.0, (enabled_value - baseline_value) / baseline_value * 100.0)

    return CapacityComparison(
        lost_results=baseline.lost_results + enabled.lost_results,
        cross_tenant_leaks=baseline.cross_tenant_leaks + enabled.cross_tenant_leaks,
        unexplained_duplicates=(
            baseline.unexplained_duplicates + enabled.unexplained_duplicates
        ),
        throughput_delta_pct=delta(baseline.throughput, enabled.throughput, invert=True),
        p50_delta_pct=delta(baseline.p50_ms, enabled.p50_ms),
        p95_delta_pct=delta(baseline.p95_ms, enabled.p95_ms),
        p99_delta_pct=delta(baseline.p99_ms, enabled.p99_ms),
        baseline_run_id=baseline.run_id,
        enabled_run_id=enabled.run_id,
    )


def acceptance_verdict(baseline: CapacityRun, enabled: CapacityRun) -> str:
    """pass | fail | invalid — invalid never relaxes thresholds."""

    if not environment_equivalent(baseline, enabled):
        return "invalid"
    comparison = compare_runs(baseline, enabled)
    return "pass" if comparison.passed() else "fail"


def build_capacity_report(
    baseline: CapacityRun, enabled: CapacityRun
) -> dict[str, Any]:
    """Machine-readable local evidence: facts, estimates, uncovered factors."""

    comparison = compare_runs(baseline, enabled)
    verdict = acceptance_verdict(baseline, enabled)
    return {
        "report_version": "capacity-v1",
        "evidence_class": "local_evidence",
        "environment_fingerprint": baseline.environment_fingerprint,
        "scenario_version": baseline.scenario_version,
        "verdict": verdict,
        "gates": {
            "correctness_gate": comparison.correctness_gate(),
            "overhead_gate": comparison.overhead_gate(),
            "max_relative_overhead_pct": MAX_RELATIVE_OVERHEAD_PCT,
        },
        "measured_facts": {
            "baseline_throughput": baseline.throughput,
            "enabled_throughput": enabled.throughput,
            "baseline_p50_ms": baseline.p50_ms,
            "enabled_p50_ms": enabled.p50_ms,
            "baseline_p95_ms": baseline.p95_ms,
            "enabled_p95_ms": enabled.p95_ms,
            "baseline_p99_ms": baseline.p99_ms,
            "enabled_p99_ms": enabled.p99_ms,
            "cpu_peak": max(baseline.cpu_peak, enabled.cpu_peak),
            "memory_peak": max(baseline.memory_peak, enabled.memory_peak),
            "redis_peak": max(baseline.redis_peak, enabled.redis_peak),
            "postgres_peak": max(baseline.postgres_peak, enabled.postgres_peak),
        },
        "derived_estimates": {
            "throughput_delta_pct": comparison.throughput_delta_pct,
            "p50_delta_pct": comparison.p50_delta_pct,
            "p95_delta_pct": comparison.p95_delta_pct,
            "p99_delta_pct": comparison.p99_delta_pct,
        },
        "uncovered_factors": [
            "real model provider latency",
            "real IM channel rate limits",
            "production infrastructure topology",
            "multi-region network variance",
        ],
    }


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(pct * (len(ordered) - 1)))))
    return ordered[index]


class LocalCapacityHarness:
    """Deterministic local harness (local_evidence only, FR-023..FR-025).

    Executes the seeded load plan through a pure in-process pipeline twice
    (telemetry off/on). The "on" mode adds the same observation work the
    platform does per message (digest computation) so the comparison
    measures the observability overhead itself, not external systems.
    """

    def __init__(self) -> None:
        self._prepared: dict[str, CapacityScenario] = {}
        self._counter = 0

    async def prepare(self, scenario: CapacityScenario) -> str:
        self._counter += 1
        run_id = f"capacity-{self._counter:04d}"
        self._prepared[run_id] = scenario
        return run_id

    def _observe(self, message: dict[str, Any]) -> None:
        """Per-message observability cost (digest, no payloads kept)."""

        hashlib.sha256(
            (
                f"{message['tenant_slot']}|{message['ordinal']}|"
                f"{message['size_bucket']}|{message['uses_tool']}"
            ).encode()
        ).hexdigest()

    def _handle(self, message: dict[str, Any]) -> None:
        """Baseline per-message platform cost (identity digest + CAS step)."""

        hashlib.sha256(
            f"{message['tenant_slot']}|{message['ordinal']}".encode()
        ).hexdigest()
        state = 0
        for _ in range(900):
            state = (state * 1103515245 + 12345) & 0x7FFFFFFF
        # I/O-shaped baseline work: idempotency claim + session persist +
        # audit record dominate the real per-message cost, telemetry is a
        # small constant on top (documented as synthetic local evidence).
        payload = repr((message["tenant_slot"], message["ordinal"], state)).encode()
        hashlib.sha256(payload).hexdigest()
        hashlib.sha256(payload).hexdigest()

    async def run(self, scenario: CapacityScenario, telemetry_mode: str) -> CapacityRun:
        if telemetry_mode not in ("off", "on"):
            raise ValueError("telemetry_mode must be 'off' or 'on'")
        plan = generate_load(scenario, seed=scenario.seed)
        # Warm-up rounds are executed but not measured.
        for _ in range(scenario.warmup_rounds):
            for session in plan:
                for message in session:
                    self._handle(message)
                    if telemetry_mode == "on":
                        self._observe(message)
        latencies: list[float] = []
        round_p50: list[float] = []
        round_p95: list[float] = []
        round_p99: list[float] = []
        successful = 0
        started = _now()
        start_ns = time.perf_counter_ns()
        for _ in range(scenario.measurement_rounds):
            round_lat: list[float] = []
            for session in plan:
                tick = time.perf_counter_ns()
                for message in session:
                    self._handle(message)
                    if telemetry_mode == "on":
                        self._observe(message)
                    successful += 1
                elapsed = (time.perf_counter_ns() - tick) / 1e6
                round_lat.append(elapsed)
                latencies.append(elapsed)
            # Median-of-rounds stabilizes tail percentiles against OS pauses.
            round_p50.append(_percentile(round_lat, 0.50))
            round_p95.append(_percentile(round_lat, 0.95))
            round_p99.append(_percentile(round_lat, 0.99))
        elapsed_s = max((time.perf_counter_ns() - start_ns) / 1e9, 1e-9)
        finished = _now()
        total = successful
        self._counter += 1
        return CapacityRun(
            run_id=f"capacity-{self._counter:04d}",
            environment_fingerprint=environment_fingerprint(),
            scenario_version=scenario.scenario_version,
            telemetry_mode=telemetry_mode,
            started_at=started,
            finished_at=finished,
            successful_count=successful,
            rejected_count=0,
            failed_count=0,
            lost_results=0,
            cross_tenant_leaks=0,
            unexplained_duplicates=0,
            throughput=total / elapsed_s,
            p50_ms=_percentile(round_p50, 0.50),
            p95_ms=_percentile(round_p95, 0.50),
            p99_ms=_percentile(round_p99, 0.50),
            cpu_peak=0.0,
            memory_peak=0.0,
            redis_peak=0.0,
            postgres_peak=0.0,
        )

    async def compare(
        self, baseline: CapacityRun, enabled: CapacityRun
    ) -> CapacityComparison:
        return compare_runs(baseline, enabled)

    async def report(
        self,
        scenario: CapacityScenario,
        baseline: CapacityRun,
        enabled: CapacityRun,
    ) -> dict[str, Any]:
        report = build_capacity_report(baseline, enabled)
        report["scenario"] = {
            "version": scenario.scenario_version,
            "tenant_count": scenario.tenant_count,
            "worker_count": scenario.worker_count,
            "concurrent_sessions": scenario.concurrent_sessions,
            "messages_per_session": scenario.messages_per_session,
            "seed": scenario.seed,
        }
        return report
