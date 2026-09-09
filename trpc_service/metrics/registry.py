"""Prometheus metric definitions.

External user and session identifiers are deliberately absent from labels. Tenant and
app labels are bounded by control-plane inventory and support required cost isolation.
"""

from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import Counter, Gauge, Histogram


@dataclass(frozen=True, slots=True)
class PlatformMetrics:
    """All platform metrics grouped to avoid ad-hoc high-cardinality labels."""

    inbound_total: Counter
    agent_duration_seconds: Histogram
    model_duration_seconds: Histogram
    tool_duration_seconds: Histogram
    storage_duration_seconds: Histogram
    delivery_total: Counter
    token_total: Counter
    cost_micros_total: Counter
    session_leases: Gauge


METRICS = PlatformMetrics(
    inbound_total=Counter(
        "agent_platform_inbound_total",
        "Accepted or rejected inbound channel deliveries.",
        ("tenant", "channel", "outcome"),
    ),
    agent_duration_seconds=Histogram(
        "agent_platform_agent_duration_seconds",
        "End-to-end Agent turn latency.",
        ("tenant", "app", "outcome"),
    ),
    model_duration_seconds=Histogram(
        "agent_platform_model_duration_seconds",
        "Model call latency.",
        ("tenant", "provider", "model", "outcome"),
    ),
    tool_duration_seconds=Histogram(
        "agent_platform_tool_duration_seconds",
        "Tool execution latency.",
        ("tenant", "tool", "outcome"),
    ),
    storage_duration_seconds=Histogram(
        "agent_platform_storage_duration_seconds",
        "Storage operation latency.",
        ("backend", "operation", "outcome"),
    ),
    delivery_total=Counter(
        "agent_platform_delivery_total",
        "Outbound IM delivery results.",
        ("tenant", "channel", "outcome"),
    ),
    token_total=Counter(
        "agent_platform_model_tokens_total",
        "Model tokens by tenant, app, and direction.",
        ("tenant", "app", "direction"),
    ),
    cost_micros_total=Counter(
        "agent_platform_cost_micros_total",
        "Estimated model and tool cost in integer micro-units.",
        ("tenant", "app", "category"),
    ),
    session_leases=Gauge(
        "agent_platform_session_leases",
        "Sessions currently leased by a worker.",
        ("worker",),
    ),
)
