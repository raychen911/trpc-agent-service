"""T034 RED: tenant-isolated metrics, logs, traces and diagnostics (FR-006, FR-032).

Two tenants that deliberately share external identifiers (same user, same
session name, same message id, same external trace) must never observe each
other's telemetry. Identity values (raw or digested) are never valid metric
labels, and label domains are bounded enums.
"""

from __future__ import annotations

import asyncio
import importlib

from tests.observability_support import (
    obs_nodes,
    obs_tenants,
    stable_trace_digest,
)


def _metrics_module():
    try:
        return importlib.import_module("trpc_service.observability.metrics")
    except ModuleNotFoundError:
        return None


def _correlation_for(tenant_id: str):
    from trpc_service.observability.context import (
        build_correlation,
        bind_tenant_scope,
    )

    tenants = obs_tenants()
    tenant = next(item for item in tenants if item.tenant_id == tenant_id)
    # Both tenants reuse the SAME external identifiers.
    context = build_correlation(
        channel="local_http",
        external_message_digest="sha256:" + "a" * 16,
        trace_id=None,
        role="gateway",
        node_id=obs_nodes()[0].node_id,
    )
    return bind_tenant_scope(context, tenant.tenant_id)


def test_twin_tenants_share_external_ids_but_never_telemetry() -> None:
    from trpc_service.observability.service import (
        DiagnosticQueryService,
        TelemetryRecorder,
    )

    recorder = TelemetryRecorder()
    alpha = _correlation_for("tenant-alpha")
    beta = _correlation_for("tenant-beta")

    recorder.record_stage_now(alpha, "gateway.accept", "success")
    recorder.record_stage_now(alpha, "runner.invoke", "success")
    recorder.record_stage_now(beta, "gateway.accept", "success")
    recorder.record_stage_now(beta, "runner.invoke", "failed", error_type="agent_failed")

    diagnostics = DiagnosticQueryService(store=recorder)
    alpha_result = asyncio.run(diagnostics.query(alpha.tenant_scope, alpha.trace_digest))
    beta_result = asyncio.run(diagnostics.query(beta.tenant_scope, beta.trace_digest))

    def outcome_of(result: dict, stage: str) -> str | None:
        for span in result.get("stages", []):
            if span.get("stage") == stage:
                return span.get("outcome")
        return None

    assert outcome_of(alpha_result, "runner.invoke") == "success"
    assert outcome_of(beta_result, "runner.invoke") == "failed"
    # Identical external identities, distinct partition keys.
    assert alpha.tenant_scope != beta.tenant_scope


def test_cross_tenant_trace_query_returns_nothing() -> None:
    from trpc_service.observability.service import (
        DiagnosticQueryService,
        TelemetryRecorder,
    )

    recorder = TelemetryRecorder()
    alpha = _correlation_for("tenant-alpha")
    recorder.record_stage_now(alpha, "gateway.accept", "success")

    diagnostics = DiagnosticQueryService(store=recorder)
    beta_scope = _correlation_for("tenant-beta").tenant_scope
    result = asyncio.run(diagnostics.query(beta_scope, alpha.trace_digest))
    # Beta's partition holds no real evidence for this trace: every stage is
    # explicitly not_applicable, never a leaked outcome from tenant-alpha.
    assert result.get("stages"), "stage skeleton must still be explicit"
    assert all(
        span.get("outcome") == "not_applicable"
        for span in result.get("stages", [])
    )


def test_identity_values_are_never_valid_metric_labels() -> None:
    metrics = _metrics_module()
    assert metrics is not None, "trpc_service.observability.metrics is not implemented"
    registry = metrics.MetricRegistry.default()
    definition = registry.get("trpc.requests")
    label_key = definition.allowed_label_keys[0]
    identity_values = [
        "tenant-alpha",
        "sha256:0123456789abcdef",  # tenant digest
        "session-alpha",  # same-named session
        "user-external-42",
        "msg-external-42",
        stable_trace_digest("twin-trace"),
    ]
    for value in identity_values:
        try:
            registry.validate_labels("trpc.requests", {label_key: value})
        except ValueError:
            continue
        raise AssertionError(f"identity value {value!r} must not pass the label boundary")


def test_label_domains_are_bounded_enums() -> None:
    metrics = _metrics_module()
    assert metrics is not None
    registry = metrics.MetricRegistry.default()
    for name in (
        "trpc.requests", "trpc.stage.duration", "trpc.channel.delivery",
        "trpc.recovery", "trpc.telemetry.dropped", "trpc.release.transition",
    ):
        definition = registry.get(name)
        for label_key in definition.allowed_label_keys:
            domain = definition.label_domains.get(label_key)
            assert domain, f"{name}.{label_key} must have an explicit enum domain"
            assert len(domain) <= 32, f"{name}.{label_key} domain is unbounded"
