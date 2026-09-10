"""T030 RED: central, low-cardinality MetricRegistry (FR-005/FR-006, NFR-007).

The registry is a closed declaration set. Metric names, instruments, units and
label domains are defined centrally; tenants/adapters cannot invent metrics or
unbounded label values at runtime.
"""

from __future__ import annotations

import importlib


def _load():
    try:
        return importlib.import_module("trpc_service.observability.metrics")
    except ModuleNotFoundError:
        return None


CORE_METRIC_NAMES = (
    "trpc.requests",
    "trpc.stage.duration",
    "trpc.runner.duration",
    "trpc.tool.duration",
    "trpc.channel.delivery",
    "trpc.state.operation.duration",
    "trpc.recovery",
    "trpc.telemetry.dropped",
    "trpc.release.transition",
)


def test_all_core_metrics_are_registered() -> None:
    metrics = _load()
    assert metrics is not None, "trpc_service.observability.metrics is not implemented"
    registry = metrics.MetricRegistry.default()
    for name in CORE_METRIC_NAMES:
        definition = registry.get(name)
        assert definition.name == name
        assert definition.description
        assert definition.unit
        assert definition.instrument_type in ("counter", "gauge", "histogram")


def test_registry_is_closed_to_dynamic_names() -> None:
    metrics = _load()
    assert metrics is not None
    registry = metrics.MetricRegistry.default()
    try:
        registry.get("trpc.dynamic.tenant.loop")
    except Exception:
        return
    raise AssertionError("unknown metric name must not resolve")


def test_unregistered_label_keys_are_rejected() -> None:
    metrics = _load()
    assert metrics is not None
    registry = metrics.MetricRegistry.default()
    try:
        # "channel" is a valid label for other metrics but NOT for trpc.requests.
        registry.validate_labels("trpc.requests", {"channel": "local_http"})
    except ValueError:
        return
    raise AssertionError("label key outside the metric declaration must be rejected")


def test_label_values_outside_enum_domain_are_rejected() -> None:
    metrics = _load()
    assert metrics is not None
    registry = metrics.MetricRegistry.default()
    definition = registry.get("trpc.requests")
    label_key = definition.allowed_label_keys[0]
    domain = definition.label_domains.get(label_key, ())
    assert domain, "core outcome label must have a bounded enum domain"
    invalid_value = f"definitely-not-in-{label_key}"
    assert invalid_value not in domain
    try:
        registry.validate_labels("trpc.requests", {label_key: invalid_value})
    except Exception:
        return
    raise AssertionError("label value outside the enum domain must be rejected")


def test_declared_label_domains_accept_known_values() -> None:
    metrics = _load()
    assert metrics is not None
    registry = metrics.MetricRegistry.default()
    definition = registry.get("trpc.requests")
    label_key = definition.allowed_label_keys[0]
    known_value = definition.label_domains[label_key][0]
    registry.validate_labels("trpc.requests", {label_key: known_value})


def test_forbidden_identity_labels_are_never_declared() -> None:
    metrics = _load()
    assert metrics is not None
    registry = metrics.MetricRegistry.default()
    for name in CORE_METRIC_NAMES:
        definition = registry.get(name)
        for label_key in definition.allowed_label_keys:
            assert label_key not in {
                "tenant", "tenant_id", "tenant_digest",
                "user", "user_id", "session", "session_id",
                "message", "message_id", "trace", "trace_id", "trace_digest",
                "body", "url", "error_text",
            }


def test_deterministic_runner_marks_token_and_cost_not_applicable() -> None:
    metrics = _load()
    assert metrics is not None
    registry = metrics.MetricRegistry.default()
    usage = registry.usage_status()
    assert usage["token_metric_status"] == "not_applicable"
    assert usage["cost_metric_status"] == "not_applicable"
