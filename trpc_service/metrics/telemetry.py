"""
Telemetry, Metrics, and Distributed Tracing instrumentation.
"""

import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict, Optional
import logging
from prometheus_client import Counter, Histogram, Gauge, generate_latest

logger = logging.getLogger(__name__)

# Prometheus Metrics Definitions
AGENT_REQUESTS_TOTAL = Counter(
    "trpc_agent_requests_total",
    "Total incoming agent requests",
    ["tenant_id", "channel", "status"],
)

AGENT_LATENCY_SECONDS = Histogram(
    "trpc_agent_latency_seconds",
    "Latency of Agent execution in seconds",
    ["tenant_id", "channel"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
)

AGENT_TOKENS_TOTAL = Counter(
    "trpc_agent_tokens_total",
    "Total LLM tokens consumed",
    ["tenant_id", "model", "type"],  # type: prompt / completion
)

IM_DELIVERY_TOTAL = Counter(
    "trpc_im_delivery_total",
    "IM outbound message deliveries",
    ["tenant_id", "channel", "status"],
)

TOOL_EXECUTIONS_TOTAL = Counter(
    "trpc_tool_executions_total",
    "Total tool invocations",
    ["tenant_id", "tool_name", "status"],
)

ACTIVE_SESSIONS = Gauge(
    "trpc_active_sessions",
    "Currently active concurrent sessions",
    ["tenant_id"],
)


class TraceSpan:
    """Lightweight OpenTelemetry-compatible distributed trace span."""

    def __init__(self, name: str, trace_id: str, tenant_id: str, parent_id: Optional[str] = None):
        self.name = name
        self.trace_id = trace_id
        self.tenant_id = tenant_id
        self.span_id = uuid.uuid4().hex[:16]
        self.parent_id = parent_id
        self.start_time = 0.0
        self.end_time = 0.0
        self.tags: Dict[str, str] = {}
        self.events = []

    def set_tag(self, key: str, value: Any):
        self.tags[key] = str(value)

    def log_event(self, name: str, payload: Optional[Dict] = None):
        self.events.append({"name": name, "timestamp": time.time(), "payload": payload or {}})


@asynccontextmanager
async def trace_span(name: str, trace_id: str, tenant_id: str, parent_id: Optional[str] = None) -> AsyncGenerator[TraceSpan, None]:
    """Async context manager to trace operations across Gateway, Runner, and Tools."""
    span = TraceSpan(name, trace_id, tenant_id, parent_id)
    span.start_time = time.time()
    try:
        yield span
    finally:
        span.end_time = time.time()
        duration_ms = (span.end_time - span.start_time) * 1000
        logger.debug("[TraceSpan] %s | trace=%s span=%s took=%.2fms tags=%s",
                     span.name, span.trace_id, span.span_id, duration_ms, span.tags)


def get_prometheus_metrics() -> bytes:
    """Export raw prometheus metrics for /metrics scraping endpoint."""
    return generate_latest()
