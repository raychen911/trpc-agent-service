"""T054 RED: hard/quality gate evaluation with latching semantics.

A HARD_STOP gate (cross-tenant leak, unauthorized side effect, data
consistency, configuration incompatibility) latches on its first occurrence
from a persistent enforcement point and blocks candidate traffic; a
QUALITY_PAUSE needs the observation window elapsed AND the minimum sample
reached before a breach pauses; INSUFFICIENT_SAMPLE forbids silent advance;
only PASS allows the CAS advance. Hard signals that only exist in droppable
telemetry never latch (FR-018, FR-019, DEC-004).
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


gates = _load("trpc_service.operations.gates")
models = _load("trpc_service.operations.models")

HARD_TYPES = (
    "cross_tenant_leak",
    "unauthorized_side_effect",
    "data_consistency",
    "configuration_incompatible",
)


def _evaluator():
    return gates.GateEvaluator(
        observation_window=300,
        minimum_sample=100,
        quality_gates=(
            {"gate_type": "error_rate", "threshold": 0.05, "direction": "max"},
        ),
    )


def _signal(
    gate_type: str,
    severity: str,
    *,
    evidence_digest: str | None = None,
    sample_count: int = 0,
    observed_value: float | None = None,
    observed_at: datetime = _NOW,
):
    return models.ReleaseGateSignal(
        signal_id=f"sig-{gate_type}-{severity}",
        tenant_id="tenant-alpha",
        release_id="rel-0001",
        signal_digest=f"{gate_type}:{severity}".ljust(64, ".")[:64],
        gate_type=gate_type,
        severity=severity,
        observation_window=300,
        sample_count=sample_count,
        observed_value=observed_value,
        evidence_digest=evidence_digest,
        observed_at=observed_at,
    )


def test_hard_gate_types_are_the_zero_tolerance_set() -> None:
    assert gates.HARD_GATE_TYPES == HARD_TYPES


def test_first_hard_signal_from_enforcement_latches() -> None:
    verdict = _evaluator().evaluate(
        hard_signals=[
            _signal("cross_tenant_leak", "hard", evidence_digest="e" * 64),
        ],
        quality_signals=[],
        window_elapsed=False,
        sample_count=1,
    )
    assert verdict == "hard_stop", "the first hard signal must latch immediately"


def test_hard_signal_without_enforcement_evidence_never_latches() -> None:
    verdict = _evaluator().evaluate(
        hard_signals=[_signal("data_consistency", "hard", evidence_digest=None)],
        quality_signals=[],
        window_elapsed=True,
        sample_count=500,
    )
    assert verdict != "hard_stop", (
        "hard signals only visible in droppable telemetry must not latch"
    )


def test_quality_pause_requires_window_and_minimum_sample() -> None:
    evaluator = _evaluator()
    breach = _signal("error_rate", "quality", sample_count=150, observed_value=0.2)
    early = evaluator.evaluate(
        hard_signals=[], quality_signals=[breach], window_elapsed=False, sample_count=150
    )
    assert early == "pending", "a breach before the window elapses stays pending"
    low_sample = evaluator.evaluate(
        hard_signals=[], quality_signals=[breach], window_elapsed=True, sample_count=50
    )
    assert low_sample == "insufficient_sample", (
        "a breach under the minimum sample is insufficient, not a pause"
    )
    paused = evaluator.evaluate(
        hard_signals=[], quality_signals=[breach], window_elapsed=True, sample_count=150
    )
    assert paused == "quality_pause", (
        "window + minimum sample + threshold breach pauses the rollout"
    )


def test_insufficient_sample_forbids_silent_advance() -> None:
    verdict = _evaluator().evaluate(
        hard_signals=[],
        quality_signals=[_signal("error_rate", "quality", sample_count=10, observed_value=0.01)],
        window_elapsed=True,
        sample_count=10,
    )
    assert verdict == "insufficient_sample"


def test_pass_requires_window_sample_and_clean_quality() -> None:
    verdict = _evaluator().evaluate(
        hard_signals=[],
        quality_signals=[_signal("error_rate", "quality", sample_count=200, observed_value=0.01)],
        window_elapsed=True,
        sample_count=200,
    )
    assert verdict == "pass", "only a clean, complete window passes"


def test_hard_stop_wins_over_quality_verdicts() -> None:
    verdict = _evaluator().evaluate(
        hard_signals=[_signal("unauthorized_side_effect", "hard", evidence_digest="e" * 64)],
        quality_signals=[_signal("error_rate", "quality", sample_count=150, observed_value=0.01)],
        window_elapsed=True,
        sample_count=150,
    )
    assert verdict == "hard_stop"
