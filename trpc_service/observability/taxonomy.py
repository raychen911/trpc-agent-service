"""Central taxonomy for stages, outcomes, components, roles and dependencies.

Every adapter, gateway, worker and recovery component MUST resolve stage and
role names from this module instead of inventing parallel spellings (DEC-001).
The tuples are intentionally ordered: pipeline stages are emitted in execution
order and consumers rely on that ordering for stage-graph rendering.
"""

from __future__ import annotations

STAGES: tuple[str, ...] = (
    "adapter.receive",
    "binding.resolve",
    "gateway.accept",
    "idempotency.claim",
    "session.lock",
    "governance.evaluate",
    "worker.dispatch",
    "runner.invoke",
    "data.access",
    "reply.compose",
    "delivery.queue",
    "delivery.attempt",
    "delivery.result",
    "recovery.reconcile",
)

OUTCOMES: tuple[str, ...] = (
    "success",
    "rejected",
    "failed",
    "unknown",
    "recovered",
    "not_applicable",
)

ROLES: tuple[str, ...] = (
    "gateway",
    "worker",
    "feishu_adapter",
    "wecom_adapter",
    "recovery",
)

COMPONENTS: tuple[str, ...] = (
    "adapter",
    "binding",
    "gateway",
    "idempotency",
    "session",
    "governance",
    "worker",
    "runner",
    "data",
    "reply",
    "delivery",
    "recovery",
    "telemetry",
    "platform",
)

# Role that owns each stage.  ``runner.invoke`` executes inside worker
# processes; recovery owns the final reconcile stage.
STAGE_COMPONENTS: dict[str, str] = {
    "adapter.receive": "adapter",
    "binding.resolve": "binding",
    "gateway.accept": "gateway",
    "idempotency.claim": "idempotency",
    "session.lock": "session",
    "governance.evaluate": "governance",
    "worker.dispatch": "worker",
    "runner.invoke": "worker",
    "data.access": "data",
    "reply.compose": "reply",
    "delivery.queue": "delivery",
    "delivery.attempt": "delivery",
    "delivery.result": "delivery",
    "recovery.reconcile": "recovery",
}

# Platform-managed dependencies referenced by health probes (data-model 3.1).
DEPENDENCIES: tuple[str, ...] = (
    "postgres",
    "redis",
    "worker_path",
    "governance",
    "runner_initialisation",
    "feishu_connection",
    "wecom_connection",
    "channel_binding_identity",
    "telemetry",
)

_STAGE_SET = frozenset(STAGES)
_OUTCOME_SET = frozenset(OUTCOMES)
_ROLE_SET = frozenset(ROLES)
_COMPONENT_SET = frozenset(COMPONENTS)
_DEPENDENCY_SET = frozenset(DEPENDENCIES)

# Central, bounded error-type domain shared by operational logs, metric
# labels and stable error envelopes (NFR-007). Extending it is the ONLY
# sanctioned way to introduce a new error_type anywhere in the platform.
OPERATIONAL_ERROR_TYPES: tuple[str, ...] = (
    "none",
    # gateway / worker stage errors
    "audit_unavailable",
    "agent_unavailable",
    "agent_failed",
    "outcome_unknown",
    "binding_rejected",
    "idempotency_conflict",
    # channel delivery errors
    "provider_unavailable",
    "provider_rejected",
    "delivery_outcome_unknown",
    # observability errors (mirrors operations_errors codes)
    "telemetry_unavailable",
    "telemetry_dropped",
    "telemetry_adapter_incompatible",
    "diagnostic_access_denied",
    "health_state_unknown",
    # release / operations errors
    "release_not_found",
    "release_not_authorized",
    "release_conflict",
    "stale_release_fence",
    "snapshot_invalid",
    "snapshot_digest_mismatch",
    "configuration_incompatible",
    "quality_gate_paused",
    "hard_gate_triggered",
    "rollback_target_unavailable",
    "release_state_unavailable",
    "drain_timeout",
    # terminal delivery-state projections used as diagnostic markers
    "delivered",
    "retry_wait",
    "permanently_failed",
    "abandoned",
)

_OPERATIONAL_ERROR_TYPE_SET = frozenset(OPERATIONAL_ERROR_TYPES)


def validate_stage(stage: str) -> str:
    """Return ``stage`` unchanged, or raise for an unknown stage name."""
    if stage not in _STAGE_SET:
        raise ValueError(f"unknown stage {stage!r}")
    return stage


def validate_outcome(outcome: str) -> str:
    """Return ``outcome`` unchanged, or raise for an unknown outcome."""
    if outcome not in _OUTCOME_SET:
        raise ValueError(f"unknown outcome {outcome!r}")
    return outcome


def validate_role(role: str) -> str:
    """Return ``role`` unchanged, or raise for an unknown platform role."""
    if role not in _ROLE_SET:
        raise ValueError(f"unknown role {role!r}")
    return role


def validate_component(component: str) -> str:
    """Return ``component`` unchanged, or raise for an unknown component."""
    if component not in _COMPONENT_SET:
        raise ValueError(f"unknown component {component!r}")
    return component


def validate_dependency(dependency: str) -> str:
    """Return ``dependency`` unchanged, or raise for an unmanaged dependency."""
    if dependency not in _DEPENDENCY_SET:
        raise ValueError(f"unknown dependency {dependency!r}")
    return dependency


def stage_component(stage: str) -> str:
    """Centralised mapping from a stage to its owning component."""
    try:
        return STAGE_COMPONENTS[stage]
    except KeyError:
        raise ValueError(f"unknown stage {stage!r}") from None


def validate_error_type(error_type: str) -> str:
    """Return ``error_type`` unchanged, or raise for an unbounded value."""
    if error_type not in _OPERATIONAL_ERROR_TYPE_SET:
        raise ValueError(f"unknown error_type {error_type!r}")
    return error_type
