"""Tenant-scoped execution context."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TenantContext:
    tenant_id: str
    app_id: str
    trace_id: str


_current_tenant: ContextVar[TenantContext | None] = ContextVar("current_tenant", default=None)


@contextmanager
def tenant_scope(context: TenantContext) -> Iterator[TenantContext]:
    token: Token[TenantContext | None] = _current_tenant.set(context)
    try:
        yield context
    finally:
        _current_tenant.reset(token)


def current_tenant() -> TenantContext:
    context = _current_tenant.get()
    if context is None:
        raise RuntimeError("tenant context is not available")
    return context


__all__ = ["TenantContext", "current_tenant", "tenant_scope"]
