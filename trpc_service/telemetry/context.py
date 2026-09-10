"""Helpers for carrying trace context through non-HTTP message queues."""

from __future__ import annotations

from dataclasses import replace

from trpc_service.tenant.models import TenantContext


def with_traceparent(context: TenantContext, traceparent: str | None) -> TenantContext:
    return replace(context, traceparent=traceparent)
