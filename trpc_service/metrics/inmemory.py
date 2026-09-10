"""Thread-safe-enough process-local metrics for deterministic validation."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from uuid import UUID

from trpc_service.audit.models import PreAuthScope, TenantScope
from trpc_service.storage.contracts import AccessDenied, InvalidRequest
from trpc_service.metrics.contracts import MetricsUnavailable
from trpc_service.metrics.models import MetricSnapshot


class InMemoryMetricsRecorder:
    def __init__(self) -> None:
        self._values: dict[str, dict[str, object]] = {}
        self.fail_record = False
        self.operational_events: list[dict[str, str]] = []

    @staticmethod
    def _key(scope: TenantScope | PreAuthScope) -> str:
        if isinstance(scope, TenantScope):
            return scope.tenant_id
        if isinstance(scope, PreAuthScope):
            return "__preauth__"
        raise AccessDenied("Metrics scope is invalid.")

    def record(
        self,
        scope: TenantScope | PreAuthScope,
        *,
        trace_id: UUID,
        stage: str,
        outcome: str,
        duration_ms: float,
        values: dict[str, float] | None = None,
    ) -> None:
        supplied = values or {}
        if set(supplied) - {"token_count", "tenant_cost"}:
            raise InvalidRequest("Metrics values contain unsupported fields.")
        if self.fail_record:
            self.operational_events.append({"event": "metrics_incomplete", "trace_id": str(trace_id)})
            raise MetricsUnavailable("Metrics recording is unavailable.")
        key = self._key(scope)
        item = self._values.setdefault(
            key,
            {"request": 0, "error": 0, "delivery": 0, "agent": 0.0, "state": 0.0, "token": 0, "cost": Decimal("0"), "latencies": defaultdict(float)},
        )
        if stage == "request":
            item["request"] = int(item["request"]) + 1
        if stage == "request" and outcome == "error":
            item["error"] = int(item["error"]) + 1
        if stage == "delivery":
            item["delivery"] = int(item["delivery"]) + 1
        if stage == "agent":
            item["agent"] = float(item["agent"]) + duration_ms
        if stage == "state_backend":
            item["state"] = float(item["state"]) + duration_ms
        item["latencies"][stage] += duration_ms  # type: ignore[index]
        item["token"] = int(item["token"]) + int(supplied.get("token_count", 0))
        item["cost"] = Decimal(item["cost"]) + Decimal(str(supplied.get("tenant_cost", 0)))

    def snapshot(self, scope: TenantScope | PreAuthScope) -> MetricSnapshot:
        item = self._values.get(self._key(scope))
        if item is None:
            return MetricSnapshot(scope=scope)
        return MetricSnapshot(
            scope=scope,
            request_count=item["request"],
            error_count=item["error"],
            stage_latency_ms=dict(item["latencies"]),
            agent_latency_ms=item["agent"],
            state_backend_latency_ms=item["state"],
            channel_delivery_count=item["delivery"],
            token_count=item["token"],
            tenant_cost=item["cost"],
        )

    def reset(self) -> None:
        self._values.clear()
        self.operational_events.clear()
