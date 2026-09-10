"""Outcome-aware tail sampling (FR-008, DEC-001).

Critical categories are always kept in full and no tenant override can lower
them. Ordinary success traces are decided once per trace by a stable hash
over (trace digest, scope digest, configuration version) so every span of
the same trace shares one decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

CRITICAL_OUTCOME_CATEGORIES: frozenset[str] = frozenset(
    {
        "error",
        "security_rejection",
        "recovery",
        "cross_tenant_attempt",
        "outcome_unknown",
    }
)

DEFAULT_SAMPLE_RATIO = 0.10
PLATFORM_CEILING = 0.25

_ACTIONS: tuple[str, ...] = ("keep_full", "drop")


@dataclass(frozen=True, slots=True)
class SamplingDecision:
    """Stable, explainable decision for one trace (or one span pre-aggregation)."""

    action: str
    reason: str

    def __post_init__(self) -> None:
        if self.action not in _ACTIONS:
            raise ValueError(f"unknown sampling action {self.action!r}")


def _stable_bucket(trace_digest: str, scope_digest: str, config_version: int) -> float:
    material = f"{trace_digest}|{scope_digest}|v{config_version}".encode("utf-8")
    return int(sha256(material).hexdigest()[:8], 16) / 0xFFFFFFFF


class OutcomeAwareSamplingPolicy:
    """Trace-wide outcome-aware sampling with tenant overrides inside the ceiling."""

    def __init__(
        self,
        *,
        default_ratio: float = DEFAULT_SAMPLE_RATIO,
        platform_ceiling: float = PLATFORM_CEILING,
        tenant_overrides: dict[str, float] | None = None,
        config_version: int = 1,
    ) -> None:
        if not 0 < default_ratio <= platform_ceiling:
            raise ValueError(
                f"default ratio {default_ratio} outside (0, {platform_ceiling}]"
            )
        if platform_ceiling > 1.0:
            raise ValueError("platform ceiling cannot exceed 1.0")
        resolved: dict[str, float] = {}
        for tenant_id, ratio in (tenant_overrides or {}).items():
            # 0.0 is a legitimate tenant opt-out of ORDINARY sampling; it can
            # never lower critical retention (critical keeps are absolute).
            if not 0 <= ratio <= platform_ceiling:
                raise ValueError(
                    f"tenant {tenant_id!r} override {ratio} outside "
                    f"[0, {platform_ceiling}]"
                )
            resolved[tenant_id] = float(ratio)
        self.default_ratio = float(default_ratio)
        self.platform_ceiling = float(platform_ceiling)
        self.tenant_overrides = resolved
        self.config_version = int(config_version)

    def ratio_for(self, tenant_id: str | None) -> float:
        if tenant_id is None:
            return self.default_ratio
        override = self.tenant_overrides.get(tenant_id)
        if override is None:
            return self.default_ratio
        # The override is already validated inside (0, ceiling] at construction.
        return override

    def decide(
        self,
        *,
        trace_digest: str,
        scope_digest: str,
        outcome_category: str,
        tenant_id: str | None = None,
    ) -> SamplingDecision:
        if outcome_category in CRITICAL_OUTCOME_CATEGORIES:
            # Critical retention is absolute: no ratio, no tenant override.
            return SamplingDecision(action="keep_full", reason=outcome_category)
        if outcome_category == "ordinary_success":
            ratio = self.ratio_for(tenant_id)
            bucket = _stable_bucket(trace_digest, scope_digest, self.config_version)
            if bucket < ratio:
                return SamplingDecision(
                    action="keep_full", reason="ordinary_sampled_in"
                )
            return SamplingDecision(action="drop", reason="ordinary_sampled_out")
        raise ValueError(f"unknown outcome category {outcome_category!r}")

    def decide_trace(
        self,
        *,
        trace_digest: str,
        scope_digest: str,
        outcome_categories: tuple[str, ...],
        tenant_id: str | None = None,
    ) -> SamplingDecision:
        """Root-close decision applied uniformly to every span of the trace."""

        critical = [
            category
            for category in outcome_categories
            if category in CRITICAL_OUTCOME_CATEGORIES
        ]
        if critical:
            # The first critical category explains the whole-trace keep.
            return SamplingDecision(action="keep_full", reason=critical[0])
        if not outcome_categories:
            raise ValueError("outcome_categories must not be empty")
        for category in outcome_categories:
            if category != "ordinary_success":
                raise ValueError(f"unknown outcome category {category!r}")
        return self.decide(
            trace_digest=trace_digest,
            scope_digest=scope_digest,
            outcome_category="ordinary_success",
            tenant_id=tenant_id,
        )
