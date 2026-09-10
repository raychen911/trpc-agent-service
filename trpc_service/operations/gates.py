"""Gate evaluation for canary releases (FR-018, FR-019, DEC-004).

Hard gates are zero tolerance: the first signal from a persistent
enforcement point (proven by its ``evidence_digest``) latches immediately
and wins over every other verdict. Quality gates need the observation
window elapsed AND the minimum sample reached before a breach can pause the
rollout; an unmet sample at window end forbids silent advance.
"""

from __future__ import annotations

from typing import Any, Iterable

from trpc_service.operations.models import ReleaseGateSignal

HARD_GATE_TYPES: tuple[str, ...] = (
    "cross_tenant_leak",
    "unauthorized_side_effect",
    "data_consistency",
    "configuration_incompatible",
)

# Verdicts: only ``pass`` allows the CAS advance.
VERDICT_PASS = "pass"
VERDICT_HARD_STOP = "hard_stop"
VERDICT_QUALITY_PAUSE = "quality_pause"
VERDICT_INSUFFICIENT_SAMPLE = "insufficient_sample"
VERDICT_PENDING = "pending"


class GateEvaluator:
    """Pure verdict function over recorded gate signals."""

    def __init__(
        self,
        *,
        observation_window: int,
        minimum_sample: int,
        quality_gates: Iterable[dict[str, Any]] = (),
    ) -> None:
        self.observation_window = observation_window
        self.minimum_sample = minimum_sample
        self.quality_gates = tuple(quality_gates)

    def evaluate(
        self,
        *,
        hard_signals: Iterable[ReleaseGateSignal],
        quality_signals: Iterable[ReleaseGateSignal],
        window_elapsed: bool,
        sample_count: int,
    ) -> str:
        # 1. Zero tolerance: first enforcement-bound hard signal latches.
        for signal in hard_signals:
            if signal.severity != "hard":
                continue
            if signal.gate_type in HARD_GATE_TYPES and signal.evidence_digest:
                return VERDICT_HARD_STOP
        # 2. The observation window must elapse before quality verdicts.
        if not window_elapsed:
            return VERDICT_PENDING
        # 3. Minimum sample: an unmet sample forbids silent advance.
        if sample_count < self.minimum_sample:
            return VERDICT_INSUFFICIENT_SAMPLE
        # 4. Quality breach with a complete window pauses for humans.
        for gate in self.quality_gates:
            gate_type = gate.get("gate_type")
            threshold = float(gate.get("threshold", 0.0))
            direction = gate.get("direction", "max")
            for signal in quality_signals:
                if signal.gate_type != gate_type:
                    continue
                if signal.observed_value is None:
                    continue
                breached = (
                    signal.observed_value > threshold
                    if direction == "max"
                    else signal.observed_value < threshold
                )
                if breached:
                    return VERDICT_QUALITY_PAUSE
        return VERDICT_PASS
