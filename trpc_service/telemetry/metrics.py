"""Prometheus metrics with a no-op fallback when optional dependencies are absent."""

from __future__ import annotations

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        CollectorRegistry,
        Counter,
        Histogram,
    )
    from prometheus_client import (
        generate_latest as prometheus_generate_latest,
    )
except ImportError:
    Counter = Histogram = None
    CONTENT_TYPE_LATEST = "text/plain"

    def generate_latest():
        return b""

else:
    import os

    _multiproc_dir = os.getenv("PROMETHEUS_MULTIPROC_DIR", "").strip()
    if _multiproc_dir:
        os.makedirs(_multiproc_dir, exist_ok=True)

    def generate_latest():
        """Use the shared multiprocess registry when configured."""
        import os

        directory = os.getenv("PROMETHEUS_MULTIPROC_DIR", "").strip()
        if not directory or not os.path.isdir(directory):
            return prometheus_generate_latest()
        from prometheus_client import multiprocess

        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return prometheus_generate_latest(registry)


REQUESTS = (
    Counter("trpc_agent_requests_total", "Gateway requests", ["tenant", "channel", "status"]) if Counter else None
)
TOKENS = Counter("trpc_agent_tokens_total", "Estimated model tokens", ["tenant"]) if Counter else None
MODEL_LATENCY = Histogram("trpc_agent_model_latency_seconds", "Model latency", ["tenant"]) if Histogram else None
TOOL_CALLS = Counter("trpc_agent_tool_calls_total", "Tool calls", ["tenant", "tool", "status"]) if Counter else None
TOOL_LATENCY = Histogram("trpc_agent_tool_latency_seconds", "Tool latency", ["tenant", "tool"]) if Histogram else None
IM_DELIVERIES = (
    Counter("trpc_agent_im_deliveries_total", "IM deliveries", ["tenant", "channel", "status"]) if Counter else None
)
COST = Counter("trpc_agent_cost_total", "Estimated model cost", ["tenant"]) if Counter else None
SESSION_BACKEND_LATENCY = (
    Histogram(
        "trpc_agent_session_backend_latency_seconds",
        "Session and memory backend latency",
        ["tenant", "operation", "status"],
    )
    if Histogram
    else None
)
ERRORS = Counter("trpc_agent_errors_total", "Agent errors", ["tenant", "channel", "error_type"]) if Counter else None


def observe_request(tenant: str, channel: str, status: str) -> None:
    if REQUESTS:
        REQUESTS.labels(tenant, channel, status).inc()


def observe_tokens(tenant: str, count: int) -> None:
    if TOKENS:
        TOKENS.labels(tenant).inc(count)


def observe_model_latency(tenant: str, seconds: float) -> None:
    if MODEL_LATENCY:
        MODEL_LATENCY.labels(tenant).observe(seconds)


def observe_tool(tenant: str, tool: str, status: str) -> None:
    if TOOL_CALLS:
        TOOL_CALLS.labels(tenant, tool, status).inc()


def observe_tool_latency(tenant: str, tool: str, seconds: float) -> None:
    if TOOL_LATENCY:
        TOOL_LATENCY.labels(tenant, tool).observe(seconds)


def observe_delivery(channel: str, status: str, tenant: str = "unknown") -> None:
    if IM_DELIVERIES:
        IM_DELIVERIES.labels(tenant, channel, status).inc()


def observe_cost(tenant: str, amount: float) -> None:
    if COST and amount > 0:
        COST.labels(tenant).inc(amount)


def observe_session_backend_latency(tenant: str, operation: str, seconds: float, status: str = "ok") -> None:
    if SESSION_BACKEND_LATENCY:
        SESSION_BACKEND_LATENCY.labels(tenant, operation, status).observe(seconds)


def observe_error(tenant: str, channel: str, error_type: str) -> None:
    if ERRORS:
        ERRORS.labels(tenant, channel, error_type).inc()
