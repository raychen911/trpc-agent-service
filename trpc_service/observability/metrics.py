"""Central, closed metric registry (FR-005/FR-006/FR-007, NFR-007, DEC-001).

One place declares every platform metric: name, description, unit,
instrument and bounded enum label domains. Adapters and tenants cannot
invent metric names, label keys or unbounded label values at runtime —
identity values (raw or digested) are structurally excluded from labels.
"""

from __future__ import annotations

from trpc_service.observability.models import MetricDefinition
from trpc_service.observability import taxonomy

_STAGE_DOMAIN: tuple[str, ...] = tuple(taxonomy.STAGES)
_OUTCOME_DOMAIN: tuple[str, ...] = tuple(taxonomy.OUTCOMES)
_COMPONENT_DOMAIN: tuple[str, ...] = tuple(taxonomy.COMPONENTS)
_CHANNEL_DOMAIN: tuple[str, ...] = ("local_http", "feishu", "wecom")
_ROLE_DOMAIN: tuple[str, ...] = tuple(taxonomy.ROLES)
_ERROR_DOMAIN: tuple[str, ...] = (
    "none",
    "agent_unavailable",
    "agent_failed",
    "outcome_unknown",
    "audit_unavailable",
    "binding_rejected",
    "idempotency_conflict",
    "provider_unavailable",
    "provider_rejected",
    "delivery_outcome_unknown",
    "telemetry_unavailable",
    "telemetry_adapter_incompatible",
    "configuration_invalid",
    "release_gate_blocked",
    "capacity_gate_blocked",
    "drain_timeout",
)
_RECOVERY_DOMAIN: tuple[str, ...] = (
    "reconciled",
    "pending_review",
    "skipped_stale",
    "error",
)
_DROP_CATEGORY_DOMAIN: tuple[str, ...] = (
    "normal_evicted",
    "critical_dropped",
    "retry_exhausted",
    "ttl_expired",
    "record_failed",
)
_RELEASE_TRANSITION_DOMAIN: tuple[str, ...] = (
    "created",
    "activated",
    "rolled_back",
    "completed",
    "rejected",
)
_STATE_OPERATION_DOMAIN: tuple[str, ...] = (
    "session_read",
    "session_write",
    "memory_read",
    "memory_write",
    "data_query",
    "data_write",
    "lease_acquire",
    "lease_release",
    "lock_acquire",
    "lock_release",
)


def _build_registry() -> dict[str, MetricDefinition]:
    definitions: dict[str, MetricDefinition] = {}

    def add(definition: MetricDefinition) -> None:
        if definition.name in definitions:
            raise ValueError(f"duplicate metric {definition.name}")
        definitions[definition.name] = definition

    add(
        MetricDefinition(
            name="trpc.requests",
            description="Inbound platform requests by stage-level outcome.",
            unit="1",
            instrument_type="counter",
            allowed_label_keys=("stage", "outcome"),
            label_domains={"stage": _STAGE_DOMAIN, "outcome": _OUTCOME_DOMAIN},
        )
    )
    add(
        MetricDefinition(
            name="trpc.stage.duration",
            description="Latency of one platform stage execution.",
            unit="ms",
            instrument_type="histogram",
            allowed_label_keys=("stage", "outcome"),
            label_domains={"stage": _STAGE_DOMAIN, "outcome": _OUTCOME_DOMAIN},
        )
    )
    add(
        MetricDefinition(
            name="trpc.runner.duration",
            description="Runner (agent) invocation latency.",
            unit="ms",
            instrument_type="histogram",
            allowed_label_keys=("outcome",),
            label_domains={"outcome": _OUTCOME_DOMAIN},
        )
    )
    add(
        MetricDefinition(
            name="trpc.tool.duration",
            description="Tool invocation latency inside the Runner.",
            unit="ms",
            instrument_type="histogram",
            allowed_label_keys=("outcome",),
            label_domains={"outcome": _OUTCOME_DOMAIN},
        )
    )
    add(
        MetricDefinition(
            name="trpc.channel.delivery",
            description="IM delivery attempts and terminal outcomes.",
            unit="1",
            instrument_type="counter",
            allowed_label_keys=("channel", "outcome", "attempt_no"),
            label_domains={
                "channel": _CHANNEL_DOMAIN,
                "outcome": _OUTCOME_DOMAIN,
                "attempt_no": ("1", "2", "3", "4"),
            },
        )
    )
    add(
        MetricDefinition(
            name="trpc.state.operation.duration",
            description="Storage/state backend operation latency.",
            unit="ms",
            instrument_type="histogram",
            allowed_label_keys=("operation", "outcome"),
            label_domains={
                "operation": _STATE_OPERATION_DOMAIN,
                "outcome": _OUTCOME_DOMAIN,
            },
        )
    )
    add(
        MetricDefinition(
            name="trpc.recovery",
            description="Recovery reconciliation outcomes.",
            unit="1",
            instrument_type="counter",
            allowed_label_keys=("outcome",),
            label_domains={"outcome": _RECOVERY_DOMAIN},
        )
    )
    add(
        MetricDefinition(
            name="trpc.telemetry.dropped",
            description="Telemetry envelopes dropped by category.",
            unit="1",
            instrument_type="counter",
            allowed_label_keys=("category",),
            label_domains={"category": _DROP_CATEGORY_DOMAIN},
        )
    )
    add(
        MetricDefinition(
            name="trpc.release.transition",
            description="Configuration release transitions.",
            unit="1",
            instrument_type="counter",
            allowed_label_keys=("transition",),
            label_domains={"transition": _RELEASE_TRANSITION_DOMAIN},
        )
    )
    return definitions


class MetricRegistry:
    """Closed registry: lookup and label-boundary validation only."""

    def __init__(self, definitions: dict[str, MetricDefinition]) -> None:
        self._definitions = dict(definitions)

    @classmethod
    def default(cls) -> "MetricRegistry":
        return cls(_build_registry())

    def get(self, name: str) -> MetricDefinition:
        definition = self._definitions.get(name)
        if definition is None:
            raise KeyError(f"metric {name!r} is not registered")
        return definition

    def names(self) -> tuple[str, ...]:
        return tuple(self._definitions)

    def validate_labels(self, metric_name: str, labels: dict[str, str]) -> None:
        """Reject unregistered keys and any value outside the enum domain."""

        definition = self.get(metric_name)
        allowed = set(definition.allowed_label_keys)
        for key, value in labels.items():
            if key not in allowed:
                raise ValueError(
                    f"metric {metric_name!r} does not declare label key {key!r}"
                )
            domain = definition.label_domains.get(key, ())
            if str(value) not in domain:
                raise ValueError(
                    f"label {key}={value!r} outside the bounded enum domain "
                    f"({len(domain)} values)"
                )

    def usage_status(self) -> dict[str, str]:
        """Token/cost observability under the deterministic local Runner.

        The bundled Runner is deterministic and offline: no real model calls
        happen, so token and cost metrics are explicitly not applicable
        rather than reported as zero.
        """

        return {"token_metric_status": "not_applicable", "cost_metric_status": "not_applicable"}
