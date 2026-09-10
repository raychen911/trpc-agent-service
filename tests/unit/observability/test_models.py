"""T008a RED: observability domain models and their invariants."""

from __future__ import annotations

import importlib

SENSITIVE_ATTRIBUTE_KEYS = (
    "input",
    "output",
    "request_body",
    "response_body",
    "url",
    "secret",
    "api_key",
    "user_id",
    "tenant_id",
    "message_text",
)


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


models = _load("trpc_service.observability.models")


def test_observability_models_module_exists() -> None:
    assert models is not None, "trpc_service.observability.models is not implemented yet"


def test_trusted_correlation_context_fields() -> None:
    assert models is not None
    context_type = getattr(models, "TrustedCorrelationContext", None)
    assert context_type is not None
    hints = getattr(context_type, "__dataclass_fields__", None) or getattr(
        context_type, "model_fields", {}
    )
    field_names = set(hints)
    for required in (
        "request_trace_id",
        "tenant_scope",
        "trace_digest",
        "first_claim_trace_id",
        "owner_trace_id",
        "execution_trace_id",
        "node_id",
        "role",
    ):
        assert required in field_names, f"TrustedCorrelationContext missing {required}"


def test_diagnostic_span_rejects_sensitive_attributes() -> None:
    assert models is not None
    span_type = getattr(models, "DiagnosticSpan", None)
    assert span_type is not None
    safe_kwargs = {
        "trace_id": "t",
        "span_id": "s",
        "parent_span_id": None,
        "trace_digest": "sha256:0123456789abcdef",
        "scope_digest": "0123456789abcdef",
        "component": "gateway",
        "stage": "gateway.accept",
        "start_ns": 1,
        "end_ns": 2,
        "outcome": "success",
        "error_type": None,
        "retryable": False,
        "role": "gateway",
        "node_digest": "0123456789abcdef",
        "attributes": {"url": "https://sentinel.invalid/x", "secret": "sentinel"},
    }
    try:
        span_type(**safe_kwargs)
    except (ValueError, TypeError):
        pass
    else:
        raise AssertionError("DiagnosticSpan must reject sensitive attribute keys")


def test_telemetry_envelope_priority_and_attempt_bounds() -> None:
    assert models is not None
    envelope_type = getattr(models, "TelemetryEnvelope", None)
    assert envelope_type is not None
    priorities = getattr(envelope_type, "PRIORITIES", None)
    assert priorities is not None and set(priorities) == {"critical", "normal"}
    signal_types = getattr(envelope_type, "SIGNAL_TYPES", None)
    assert signal_types is not None and "critical_summary" in set(signal_types)
    try:
        envelope_type(
            envelope_id="e", signal_type="trace", priority="critical",
            scope_digest="0123456789abcdef", payload={}, attempt_count=4,
        )
    except (ValueError, TypeError):
        pass
    else:
        raise AssertionError("attempt_count must be bounded to 0..3")


def test_alert_incident_state_machine_values() -> None:
    assert models is not None
    incident_type = getattr(models, "AlertIncident", None)
    assert incident_type is not None
    states = getattr(incident_type, "STATES", None)
    assert states is not None
    assert set(states) == {"pending", "firing", "recovering", "resolved"}


def test_critical_summary_has_fixed_fields_only() -> None:
    assert models is not None
    summary_type = getattr(models, "CriticalDiagnosticSummary", None)
    assert summary_type is not None
    hints = getattr(summary_type, "__dataclass_fields__", None) or getattr(
        summary_type, "model_fields", {}
    )
    allowed = {
        "trace_digest", "scope_digest", "component", "stage", "error_type",
        "retryable", "configuration_version", "occurred_at",
    }
    assert set(hints) <= allowed, "CriticalDiagnosticSummary must stay minimal and fixed-size"


def test_metric_definition_declares_label_allowlist() -> None:
    assert models is not None
    definition_type = getattr(models, "MetricDefinition", None)
    assert definition_type is not None
    hints = getattr(definition_type, "__dataclass_fields__", None) or getattr(
        definition_type, "model_fields", {}
    )
    for required in ("name", "unit", "instrument_type", "allowed_label_keys"):
        assert required in set(hints), f"MetricDefinition missing {required}"


def test_role_readiness_states_are_bounded() -> None:
    assert models is not None
    snapshot_type = getattr(models, "RoleReadinessSnapshot", None)
    assert snapshot_type is not None
    assert set(getattr(snapshot_type, "READINESS", ())) == {"ready", "unready"}
    assert set(getattr(snapshot_type, "SERVICE_STATES", ())) == {
        "ready", "degraded", "unready",
    }
