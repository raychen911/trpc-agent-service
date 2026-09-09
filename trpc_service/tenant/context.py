# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Immutable tenant context propagated through one service request."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Iterator


@dataclass(frozen=True, slots=True)
class TenantContext:
    """Trusted tenant identity resolved at the service boundary."""

    tenant_id: str
    app_id: str
    config_version: int
    request_id: str
    channel: str = "web"
    binding_id: str = ""

    @property
    def sdk_app_name(self) -> str:
        """Return the tenant-scoped SDK application name."""
        return f"tenant:{self.tenant_id}:app:{self.app_id}"


_current_tenant: ContextVar[TenantContext | None] = ContextVar("trpc_service_tenant", default=None)


def get_current_tenant() -> TenantContext:
    """Return the current trusted tenant context or fail closed."""
    context = _current_tenant.get()
    if context is None:
        raise RuntimeError("tenant context is not available")
    return context


@contextmanager
def tenant_scope(context: TenantContext) -> Iterator[TenantContext]:
    """Bind a tenant context to the current asynchronous execution context."""
    token = _current_tenant.set(context)
    try:
        yield context
    finally:
        _current_tenant.reset(token)
