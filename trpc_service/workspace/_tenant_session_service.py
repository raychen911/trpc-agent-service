# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant-scoped session service.

``TenantSessionService`` wraps any :class:`SessionServiceABC` backend and
injects the tenant id into ``app_name`` before delegating. Because the
framework already prefixes every storage key with ``app_name``, this provides
hard isolation at the storage-key level: two tenants can use identical
``user_id`` / ``session_id`` values without ever colliding.
"""

from __future__ import annotations

import time
from typing import Any
from typing import Optional

from trpc_agent_sdk.abc import ListSessionsResponse
from trpc_agent_sdk.abc import ResponseABC
from trpc_agent_sdk.abc import SessionABC
from trpc_agent_sdk.abc import SessionServiceABC
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.context import InvocationContext

from trpc_service._utils import scope_key
from trpc_service.metrics._observability import storage_span
from trpc_service.metrics import EnterpriseMetrics
from trpc_service.metrics import get_enterprise_metrics


class TenantSessionService(SessionServiceABC):
    """Session service that isolates a tenant by scoping ``app_name``."""

    def __init__(self,
                 backend: SessionServiceABC,
                 tenant_id: str,
                 metrics: Optional[EnterpriseMetrics] = None,
                 backend_name: Optional[str] = None) -> None:
        self._backend = backend
        self._tenant_id = tenant_id
        self._metrics = metrics or get_enterprise_metrics()
        self._backend_name = backend_name or type(backend).__name__

    @property
    def tenant_id(self) -> str:
        return self._tenant_id

    @property
    def backend(self) -> SessionServiceABC:
        return self._backend

    def _scope(self, app_name: str) -> str:
        return scope_key(self._tenant_id, app_name)

    async def _execute(self, data_type: str, operation: str, awaitable: Any) -> Any:
        started = time.perf_counter()
        outcome = "error"
        error_type = None
        try:
            with storage_span(self._tenant_id, data_type, operation):
                result = await awaitable
            outcome = "success"
            return result
        except Exception as exc:
            error_type = type(exc).__name__
            raise
        finally:
            attributes = {
                "tenant_id": self._tenant_id,
                "backend": self._backend_name,
                "data_type": data_type,
                "operation": operation,
                "outcome": outcome,
                "error_type": error_type,
            }
            self._metrics.increment("agent_storage_operation_total", **attributes)
            self._metrics.observe(
                "agent_storage_operation_duration_ms",
                (time.perf_counter() - started) * 1000,
                **attributes,
            )

    async def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: Optional[dict[str, Any]] = None,
        session_id: Optional[str] = None,
        agent_context: Optional[AgentContext] = None,
    ) -> SessionABC:
        return await self._execute(
            "session",
            "create",
            self._backend.create_session(
                app_name=self._scope(app_name),
                user_id=user_id,
                state=state,
                session_id=session_id,
                agent_context=agent_context,
            ),
        )

    async def get_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        agent_context: Optional[AgentContext] = None,
    ) -> Optional[SessionABC]:
        return await self._execute(
            "session",
            "get",
            self._backend.get_session(
                app_name=self._scope(app_name),
                user_id=user_id,
                session_id=session_id,
                agent_context=agent_context,
            ),
        )

    async def list_sessions(
        self,
        *,
        app_name: str,
        user_id: Optional[str] = None,
    ) -> ListSessionsResponse:
        return await self._execute(
            "session",
            "list",
            self._backend.list_sessions(app_name=self._scope(app_name), user_id=user_id),
        )

    async def delete_session(self, *, app_name: str, user_id: str, session_id: str) -> None:
        return await self._execute(
            "session",
            "delete",
            self._backend.delete_session(
                app_name=self._scope(app_name),
                user_id=user_id,
                session_id=session_id,
            ),
        )

    async def append_event(self, session: SessionABC, event: ResponseABC) -> ResponseABC:
        return await self._execute("session", "append_event", self._backend.append_event(session, event))

    async def update_session(self, session: SessionABC) -> None:
        return await self._execute("session", "update", self._backend.update_session(session))

    async def create_session_summary(self, session: SessionABC, ctx: "InvocationContext" = None) -> None:
        return await self._execute("summary", "create", self._backend.create_session_summary(session, ctx=ctx))

    async def get_session_summary(self, session: SessionABC) -> Optional[str]:
        return await self._execute("summary", "get", self._backend.get_session_summary(session))

    async def close(self) -> None:
        return await self._backend.close()
