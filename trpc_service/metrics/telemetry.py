"""Small helpers shared by the service's custom telemetry hooks."""

from __future__ import annotations

from hashlib import blake2b

from opentelemetry import trace
from opentelemetry.trace import Span, Status, StatusCode, Tracer


def get_tracer() -> Tracer:
    """Return the service tracer lazily so tests and runtime setup can replace it."""

    return trace.get_tracer("trpc_service")


def mark_span_error(span: Span, error_type: str) -> None:
    """Mark a span without recording exception text or other sensitive values."""

    span.set_status(Status(StatusCode.ERROR))
    span.set_attribute("error.type", error_type)


def stable_tenant_label(tenant_id: str) -> str:
    """Return a stable opaque label for tenant-level cost attribution."""

    return blake2b(tenant_id.encode("utf-8"), digest_size=12, person=b"trpc-tenant").hexdigest()


def queue_label(event_type: str) -> str:
    """Map event types to a bounded queue label."""

    prefix = event_type.partition(".")[0]
    if prefix in {"inbound", "outbound", "post_turn"}:
        return prefix
    return "other"


__all__ = ["get_tracer", "mark_span_error", "queue_label", "stable_tenant_label"]
