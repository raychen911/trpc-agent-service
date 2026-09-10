"""Bounded, pseudonymous shared-profile metric dimensions."""

from __future__ import annotations

from hashlib import sha256

from trpc_service.audit.models import PreAuthScope, TenantScope
from trpc_service.channels.contracts import Channel
from trpc_service.metrics.inmemory import InMemoryMetricsRecorder
from trpc_service.metrics.models import ChannelMetricEvent, DataMetricEvent


def _anonymous(value: str) -> str:
    return "sha256:" + sha256(value.encode("utf-8")).hexdigest()[:16]


class SharedMetricsRecorder(InMemoryMetricsRecorder):
    def __init__(self, *, node_id: str) -> None:
        super().__init__()
        self.node_id = node_id
        self.events: list[dict[str, object]] = []

    def observe_lease(self, scope: TenantScope, *, backend: str, session_id: str,
                      outcome: str, wait_ms: float) -> None:
        self.events.append({
            "node_id": self.node_id, "tenant": _anonymous(scope.tenant_id),
            "backend": backend, "session": _anonymous(session_id),
            "outcome": outcome, "wait_ms": wait_ms,
        })

    def observe_trace(self, scope: TenantScope, *, backend: str, outcome: str,
                      first_trace: str | None, owner_trace: str | None,
                      execution_trace: str | None, generation: int | None) -> None:
        self.events.append({
            "node_id": self.node_id, "tenant": _anonymous(scope.tenant_id),
            "backend": backend, "outcome": outcome,
            "first_trace": first_trace, "owner_trace": owner_trace,
            "execution_trace": execution_trace, "generation": generation,
        })

    def observe_channel(
        self,
        scope: TenantScope | PreAuthScope,
        *,
        channel: Channel,
        stage: str,
        outcome: str,
        duration_ms: float,
        attempt_no: int | None = None,
        generation: int | None = None,
    ) -> None:
        event = ChannelMetricEvent(
            node_id=self.node_id,
            tenant=_anonymous(
                scope.tenant_id if isinstance(scope, TenantScope) else "__preauth__"
            ),
            channel=channel,
            stage=stage,
            outcome=outcome,
            duration_ms=duration_ms,
            attempt_no=attempt_no,
            generation=generation,
        )
        self.events.append(event.model_dump(mode="json"))

    def observe_data(self, *, resource_type: str, backend_type: str, operation: str, outcome: str, duration_ms: float) -> None:
        event = DataMetricEvent(resource_type=resource_type, backend_type=backend_type, operation=operation, outcome=outcome, duration_ms=duration_ms)
        self.events.append(event.model_dump(mode="json"))
