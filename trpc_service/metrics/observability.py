from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

if TYPE_CHECKING:
    from trpc_service.config import Settings

tracer = trace.get_tracer("trpc_service")
_configure_guard = threading.Lock()
_configured = False


def configure_tracing(settings: Settings) -> None:
    global _configured
    if _configured:
        return
    with _configure_guard:
        if _configured:
            return
        provider = TracerProvider(
            resource=Resource.create({"service.name": settings.otel_service_name})
        )
        if settings.otel_enabled and settings.otel_exporter_otlp_endpoint:
            exporter = OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint)
            provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _configured = True


class PlatformMetrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        self.http_requests = Counter(
            "trpc_http_requests_total",
            "HTTP requests handled",
            ("method", "route", "status"),
            registry=self.registry,
        )
        self.http_latency = Histogram(
            "trpc_http_request_duration_seconds",
            "HTTP request latency",
            ("method", "route"),
            registry=self.registry,
        )
        self.gateway_messages = Counter(
            "trpc_gateway_messages_total",
            "Messages routed by the gateway",
            ("channel", "status"),
            registry=self.registry,
        )
        self.inbound_messages = Counter(
            "trpc_inbound_messages_total",
            "Durable inbound queue processing results",
            ("channel", "status"),
            registry=self.registry,
        )
        self.agent_latency = Histogram(
            "trpc_agent_duration_seconds",
            "Agent execution latency",
            ("tenant_id", "agent_app_id", "status"),
            registry=self.registry,
        )
        self.model_calls = Counter(
            "trpc_model_calls_total",
            "Model calls by tenant and result",
            ("tenant_id", "agent_app_id", "status"),
            registry=self.registry,
        )
        self.model_tokens = Counter(
            "trpc_model_tokens_total",
            "Model token consumption by tenant",
            ("tenant_id", "agent_app_id", "token_type"),
            registry=self.registry,
        )
        self.tenant_cost = Counter(
            "trpc_tenant_cost_total",
            "Accumulated model and tool cost by tenant",
            ("tenant_id", "agent_app_id"),
            registry=self.registry,
        )
        self.tool_decisions = Counter(
            "trpc_tool_governance_decisions_total",
            "Tool governance decisions",
            ("tenant_id", "tool_name", "decision"),
            registry=self.registry,
        )
        self.tool_latency = Histogram(
            "trpc_tool_duration_seconds",
            "Tool execution latency",
            ("tenant_id", "tool_name", "status"),
            registry=self.registry,
        )
        self.storage_latency = Histogram(
            "trpc_storage_operation_duration_seconds",
            "Storage operation latency",
            ("backend", "operation", "status"),
            registry=self.registry,
        )
        self.outbox_messages = Counter(
            "trpc_outbox_messages_total",
            "Outbox processing results",
            ("topic", "status"),
            registry=self.registry,
        )
        self.im_deliveries = Counter(
            "trpc_im_deliveries_total",
            "IM delivery attempts by channel and result",
            ("channel", "status"),
            registry=self.registry,
        )
        self.im_delivery_latency = Histogram(
            "trpc_im_delivery_duration_seconds",
            "IM delivery latency",
            ("channel", "status"),
            registry=self.registry,
        )
        self.healthy_nodes = Gauge(
            "trpc_gateway_healthy_nodes",
            "Healthy gateway nodes",
            registry=self.registry,
        )
        self.active_sessions = Gauge(
            "trpc_active_session_executions",
            "Agent session executions currently running on this node",
            registry=self.registry,
        )
