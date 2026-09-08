# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Low-cardinality enterprise metrics with an OTel and local snapshot sink."""

from __future__ import annotations

import threading
from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass
from typing import Any
from typing import Optional

DURATION_BUCKETS_MS = (10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0)
PART_COUNT_BUCKETS = (1.0, 2.0, 3.0, 5.0, 10.0)


@dataclass(frozen=True)
class MetricDefinition:
    """Stable metadata for one platform metric."""

    kind: str
    description: str
    unit: str = "1"
    boundaries: tuple[float, ...] = ()


METRIC_DEFINITIONS: dict[str, MetricDefinition] = {
    "agent_callback_total":
    MetricDefinition("counter", "Number of IM callbacks handled."),
    "agent_callback_duration_ms":
    MetricDefinition("histogram", "End-to-end Gateway callback duration.", "ms", DURATION_BUCKETS_MS),
    "agent_callback_enqueue_total":
    MetricDefinition("counter", "Number of callback enqueue attempts."),
    "agent_callback_rate_limited_total":
    MetricDefinition("counter", "Number of callbacks rejected by tenant rate limits."),
    "agent_queue_operation_total":
    MetricDefinition("counter", "Number of queue backend operations."),
    "agent_queue_operation_duration_ms":
    MetricDefinition("histogram", "Queue backend operation duration.", "ms", DURATION_BUCKETS_MS),
    "agent_worker_task_total":
    MetricDefinition("counter", "Number of stream task delivery attempts."),
    "agent_worker_task_duration_ms":
    MetricDefinition("histogram", "Stream task processing duration.", "ms", DURATION_BUCKETS_MS),
    "agent_worker_retry_total":
    MetricDefinition("counter", "Number of stream task retries."),
    "agent_worker_reconnect_total":
    MetricDefinition("counter", "Number of Worker queue reconnect attempts."),
    "agent_worker_unavailable_total":
    MetricDefinition("counter", "Number of callbacks rejected because no Worker was active."),
    "agent_worker_available":
    MetricDefinition("gauge", "Whether at least one queue Worker is currently active."),
    "agent_queue_dlq_total":
    MetricDefinition("counter", "Number of tasks moved to the dead-letter stream."),
    "agent_result_cache_total":
    MetricDefinition("counter", "Number of task-result cache lookups."),
    "agent_session_lock_duration_ms":
    MetricDefinition("histogram", "Time waiting for and holding a session lock.", "ms", DURATION_BUCKETS_MS),
    "agent_storage_operation_total":
    MetricDefinition("counter", "Number of storage adapter operations."),
    "agent_storage_operation_duration_ms":
    MetricDefinition("histogram", "Storage adapter operation duration.", "ms", DURATION_BUCKETS_MS),
    "agent_session_backend_latency_ms":
    MetricDefinition("histogram", "Session get-or-create duration.", "ms", DURATION_BUCKETS_MS),
    "agent_requests_total":
    MetricDefinition("counter", "Number of Agent turn attempts."),
    "agent_runner_latency_ms":
    MetricDefinition("histogram", "Agent Runner duration.", "ms", DURATION_BUCKETS_MS),
    "agent_tool_call_total":
    MetricDefinition("counter", "Number of governed Tool call attempts."),
    "agent_tool_call_duration_ms":
    MetricDefinition("histogram", "Governed Tool call duration.", "ms", DURATION_BUCKETS_MS),
    "agent_llm_input_tokens_total":
    MetricDefinition("counter", "Number of prompt tokens consumed.", "{token}"),
    "agent_llm_output_tokens_total":
    MetricDefinition("counter", "Number of completion tokens consumed.", "{token}"),
    "agent_llm_cost_total":
    MetricDefinition("counter", "Estimated model cost.", "USD"),
    "agent_budget_rejection_total":
    MetricDefinition("counter", "Number of model calls rejected by tenant budget."),
    "agent_budget_daily_token_limit":
    MetricDefinition("gauge", "Configured daily token budget for a tenant.", "{token}"),
    "agent_budget_tokens_used":
    MetricDefinition("gauge", "Tokens committed against today's tenant budget.", "{token}"),
    "agent_budget_tokens_reserved":
    MetricDefinition("gauge", "Tokens currently reserved by in-flight model calls.", "{token}"),
    "agent_budget_daily_cost_limit":
    MetricDefinition("gauge", "Configured daily cost budget for a tenant.", "USD"),
    "agent_budget_cost_used":
    MetricDefinition("gauge", "Estimated cost committed against today's tenant budget.", "USD"),
    "agent_budget_cost_reserved":
    MetricDefinition("gauge", "Estimated cost reserved by in-flight model calls.", "USD"),
    "agent_im_delivery_total":
    MetricDefinition("counter", "Number of IM reply delivery attempts."),
    "agent_im_delivery_duration_ms":
    MetricDefinition("histogram", "IM reply delivery duration.", "ms", DURATION_BUCKETS_MS),
    "agent_im_delivery_parts":
    MetricDefinition("histogram", "Logical parts in an IM reply.", "{part}", PART_COUNT_BUCKETS),
}

FORBIDDEN_METRIC_ATTRIBUTES = frozenset({
    "user_id",
    "session_id",
    "message_id",
    "trace_id",
    "request_id",
    "url",
})


class EnterpriseMetrics:
    """Record required tenant metrics without user/session high-cardinality labels."""

    def __init__(self, meter: Optional[Any] = None) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = defaultdict(float)
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._histograms: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]] = {}
        if meter is False:
            meter = None
        elif meter is None:
            try:
                from opentelemetry import metrics

                meter = metrics.get_meter("trpc.python.agent.enterprise")
            except ImportError:  # pragma: no cover - OTel optional
                meter = None
        self._meter = meter
        self._otel_counters: dict[str, Any] = {}
        self._otel_gauges: dict[str, Any] = {}
        self._otel_histograms: dict[str, Any] = {}

    @staticmethod
    def _labels(attributes: dict[str, Any]) -> tuple[tuple[str, str], ...]:
        forbidden = FORBIDDEN_METRIC_ATTRIBUTES.intersection(attributes)
        if forbidden:
            names = ", ".join(sorted(forbidden))
            raise ValueError(f"high-cardinality metric attributes are forbidden: {names}")
        return tuple(sorted((key, str(value)) for key, value in attributes.items() if value is not None))

    @staticmethod
    def _otel_attributes(labels: tuple[tuple[str, str], ...]) -> dict[str, str]:
        attributes = dict(labels)
        tenant_id = attributes.pop("tenant_id", None)
        if tenant_id is not None:
            attributes["tenant.id"] = tenant_id
        return attributes

    @staticmethod
    def _definition(name: str, kind: str) -> MetricDefinition:
        definition = METRIC_DEFINITIONS.get(name)
        if definition is not None and definition.kind != kind:
            raise ValueError(f"metric '{name}' is defined as {definition.kind}, not {kind}")
        return definition or MetricDefinition(kind, name)

    def increment(self, name: str, value: float = 1, **attributes: Any) -> None:
        definition = self._definition(name, "counter")
        labels = self._labels(attributes)
        with self._lock:
            self._counters[(name, labels)] += value
        if self._meter is not None:
            instrument = self._otel_counters.get(name)
            if instrument is None:
                instrument = self._meter.create_counter(
                    name,
                    description=definition.description,
                    unit=definition.unit,
                )
                self._otel_counters[name] = instrument
            instrument.add(value, self._otel_attributes(labels))

    def set_gauge(self, name: str, value: float, **attributes: Any) -> None:
        """Set the current value of a non-monotonic metric."""
        definition = self._definition(name, "gauge")
        labels = self._labels(attributes)
        with self._lock:
            self._gauges[(name, labels)] = value
        if self._meter is not None:
            instrument = self._otel_gauges.get(name)
            if instrument is None:
                instrument = self._meter.create_gauge(
                    name,
                    description=definition.description,
                    unit=definition.unit,
                )
                self._otel_gauges[name] = instrument
            instrument.set(value, self._otel_attributes(labels))

    def observe(self, name: str, value: float, **attributes: Any) -> None:
        definition = self._definition(name, "histogram")
        labels = self._labels(attributes)
        with self._lock:
            aggregate = self._histograms.setdefault((name, labels), {
                "count": 0.0,
                "sum": 0.0,
                "max": 0.0,
                "bucket_counts": [0] * (len(definition.boundaries) + 1),
            })
            aggregate["count"] += 1
            aggregate["sum"] += value
            aggregate["max"] = max(aggregate["max"], value)
            aggregate["bucket_counts"][bisect_left(definition.boundaries, value)] += 1
        if self._meter is not None:
            instrument = self._otel_histograms.get(name)
            if instrument is None:
                instrument = self._meter.create_histogram(
                    name,
                    description=definition.description,
                    unit=definition.unit,
                )
                self._otel_histograms[name] = instrument
            instrument.record(value, self._otel_attributes(labels))

    def snapshot(self, tenant_id: Optional[str] = None) -> dict[str, list[dict[str, Any]]]:
        result: dict[str, list[dict[str, Any]]] = {"counters": [], "gauges": [], "histograms": []}
        with self._lock:
            counters = list(self._counters.items())
            gauges = list(self._gauges.items())
            histograms = [(key, {
                **value, "bucket_counts": list(value["bucket_counts"])
            }) for key, value in self._histograms.items()]
        for (name, labels), value in counters:
            attributes = dict(labels)
            if tenant_id is None or attributes.get("tenant_id") == tenant_id:
                definition = self._definition(name, "counter")
                result["counters"].append({
                    "name": name,
                    "description": definition.description,
                    "unit": definition.unit,
                    "attributes": attributes,
                    "value": value,
                })
        for (name, labels), value in gauges:
            attributes = dict(labels)
            if tenant_id is None or attributes.get("tenant_id") == tenant_id:
                definition = self._definition(name, "gauge")
                result["gauges"].append({
                    "name": name,
                    "description": definition.description,
                    "unit": definition.unit,
                    "attributes": attributes,
                    "value": value,
                })
        for (name, labels), value in histograms:
            attributes = dict(labels)
            if tenant_id is None or attributes.get("tenant_id") == tenant_id:
                definition = self._definition(name, "histogram")
                lower = None
                buckets = []
                for index, count in enumerate(value.pop("bucket_counts")):
                    upper = definition.boundaries[index] if index < len(definition.boundaries) else None
                    buckets.append({"lower": lower, "upper": upper, "count": count})
                    lower = upper
                result["histograms"].append({
                    "name": name,
                    "description": definition.description,
                    "unit": definition.unit,
                    "attributes": attributes,
                    "buckets": buckets,
                    **value,
                })
        return result

    def reset(self) -> None:
        """Clear only the process-local diagnostic snapshot."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()


_DEFAULT_METRICS = EnterpriseMetrics()


def get_enterprise_metrics() -> EnterpriseMetrics:
    return _DEFAULT_METRICS
