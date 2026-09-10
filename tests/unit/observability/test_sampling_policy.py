"""T031 RED: outcome-aware tail sampling policy (FR-008, DEC-001).

Critical categories are always kept in full and cannot be lowered by any
tenant override. Ordinary success is decided by a stable hash over the trace
digest + scope digest + configuration version, bounded by the platform
ceiling.
"""

from __future__ import annotations

import importlib

from tests.observability_support import (
    obs_tenants,
    stable_scope_digest,
    stable_trace_digest,
)


def _load():
    try:
        return importlib.import_module("trpc_service.observability.sampling")
    except ModuleNotFoundError:
        return None


CRITICAL_CATEGORIES = (
    "error",
    "security_rejection",
    "recovery",
    "cross_tenant_attempt",
    "outcome_unknown",
)


def _digests(count: int, prefix: str) -> list[str]:
    return [stable_trace_digest(f"{prefix}-{index}") for index in range(count)]


def test_critical_categories_are_always_kept_in_full() -> None:
    sampling = _load()
    assert sampling is not None, "trpc_service.observability.sampling is not implemented"
    policy = sampling.OutcomeAwareSamplingPolicy()
    tenant = obs_tenants()[0]
    for category in CRITICAL_CATEGORIES:
        decision = policy.decide(
            trace_digest=stable_trace_digest("critical-trace"),
            scope_digest=stable_scope_digest(tenant.tenant_id),
            outcome_category=category,
        )
        assert decision.action == "keep_full", category
        assert decision.reason == category


def test_tenant_override_cannot_lower_critical_retention() -> None:
    sampling = _load()
    assert sampling is not None
    policy = sampling.OutcomeAwareSamplingPolicy(
        tenant_overrides={obs_tenants()[0].tenant_id: 0.0},
    )
    tenant = obs_tenants()[0]
    decision = policy.decide(
        trace_digest=stable_trace_digest("override-trace"),
        scope_digest=stable_scope_digest(tenant.tenant_id),
        outcome_category="error",
    )
    assert decision.action == "keep_full"


def test_ordinary_success_uses_stable_hash_decision() -> None:
    sampling = _load()
    assert sampling is not None
    policy = sampling.OutcomeAwareSamplingPolicy()
    tenant = obs_tenants()[0]
    scope_digest = stable_scope_digest(tenant.tenant_id)
    decisions = {
        digest: policy.decide(
            trace_digest=digest,
            scope_digest=scope_digest,
            outcome_category="ordinary_success",
        ).action
        for digest in _digests(200, "ordinary")
    }
    # Deterministic: same inputs always give the same decision.
    first = policy.decide(
        trace_digest=_digests(1, "ordinary")[0],
        scope_digest=scope_digest,
        outcome_category="ordinary_success",
    ).action
    assert decisions[_digests(1, "ordinary")[0]] == first
    kept = sum(1 for action in decisions.values() if action == "keep_full")
    # Default 10% with a stable hash: neither everything nor nothing.
    assert 0 < kept < 200


def test_default_ratio_is_ten_percent_with_platform_ceiling() -> None:
    sampling = _load()
    assert sampling is not None
    policy = sampling.OutcomeAwareSamplingPolicy()
    assert policy.default_ratio == 0.10
    assert policy.platform_ceiling == 0.25


def test_invalid_tenant_override_is_rejected() -> None:
    sampling = _load()
    assert sampling is not None
    tenant_id = obs_tenants()[0].tenant_id
    for bad in (-0.1, -1.0, 0.3, 1.5):
        try:
            sampling.OutcomeAwareSamplingPolicy(
                tenant_overrides={tenant_id: bad},
            )
        except ValueError:
            continue
        raise AssertionError(f"override {bad} must be rejected at construction")


def test_tenant_override_is_clamped_inside_platform_ceiling() -> None:
    sampling = _load()
    assert sampling is not None
    tenant = obs_tenants()[0]
    policy = sampling.OutcomeAwareSamplingPolicy(
        tenant_overrides={tenant.tenant_id: 0.20},
    )
    assert policy.ratio_for(tenant.tenant_id) == 0.20
    # And an unknown tenant falls back to the platform default.
    assert policy.ratio_for("tenant-unknown") == 0.10


def test_whole_trace_decision_is_uniform_and_outcome_aware() -> None:
    sampling = _load()
    assert sampling is not None
    policy = sampling.OutcomeAwareSamplingPolicy()
    tenant = obs_tenants()[0]
    digest = stable_trace_digest("consistency-trace")
    scope = stable_scope_digest(tenant.tenant_id)
    # Any critical span in the trace forces keep_full for the whole trace.
    mixed = policy.decide_trace(
        trace_digest=digest, scope_digest=scope,
        outcome_categories=("ordinary_success", "error"),
    )
    assert mixed.action == "keep_full"
    assert mixed.reason == "error"
    # A purely ordinary trace gets one uniform decision for all spans.
    ordinary = policy.decide_trace(
        trace_digest=digest, scope_digest=scope,
        outcome_categories=("ordinary_success", "ordinary_success"),
    )
    single = policy.decide(
        trace_digest=digest, scope_digest=scope, outcome_category="ordinary_success",
    )
    assert ordinary.action == single.action
    # Repeated evaluation of the same trace is stable.
    again = policy.decide_trace(
        trace_digest=digest, scope_digest=scope,
        outcome_categories=("ordinary_success",),
    )
    assert again.action == ordinary.action
