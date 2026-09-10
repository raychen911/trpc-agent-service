"""Application-owned Prometheus metrics registry."""

from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)


class ServiceMetrics:
    content_type = CONTENT_TYPE_LATEST

    def __init__(self) -> None:
        self._registry = CollectorRegistry()
        self.requests = Counter(
            "trpc_service_requests_total",
            "Requests accepted by gateway and channel.",
            ("transport", "status"),
            registry=self._registry,
        )
        self.agent_latency = Histogram(
            "trpc_service_agent_seconds",
            "Agent execution latency.",
            ("channel",),
            registry=self._registry,
        )
        self.agent_executions = Counter(
            "trpc_service_agent_executions_total",
            "Agent executions by channel and outcome.",
            ("channel", "status"),
            registry=self._registry,
        )
        self.tool_calls = Counter(
            "trpc_service_tool_calls_total",
            "Tool calls observed in agent events.",
            ("tool", "status"),
            registry=self._registry,
        )
        self.storage_operations = Counter(
            "trpc_service_storage_operations_total",
            "Tenant storage operations by logical store and outcome.",
            ("store", "status"),
            registry=self._registry,
        )
        self.storage_latency = Histogram(
            "trpc_service_storage_seconds",
            "Tenant storage operation latency.",
            ("store",),
            registry=self._registry,
        )
        self.channel_sends = Counter(
            "trpc_service_channel_sends_total",
            "Outbound channel send attempts.",
            ("channel", "status"),
            registry=self._registry,
        )

    def render(self) -> bytes:
        return generate_latest(self._registry)


__all__ = ["ServiceMetrics"]
