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


class TenantSessionService(SessionServiceABC):
    """Session service that isolates a tenant by scoping ``app_name``."""

    def __init__(self, backend: SessionServiceABC, tenant_id: str) -> None:
        self._backend = backend
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> str:
        return self._tenant_id

    @property
    def backend(self) -> SessionServiceABC:
        return self._backend

    def _scope(self, app_name: str) -> str:
        return scope_key(self._tenant_id, app_name)

    async def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: Optional[dict[str, Any]] = None,
        session_id: Optional[str] = None,
        agent_context: Optional[AgentContext] = None,
    ) -> SessionABC:
        with storage_span(self._tenant_id, "session", "create"):
            return await self._backend.create_session(
                app_name=self._scope(app_name),
                user_id=user_id,
                state=state,
                session_id=session_id,
                agent_context=agent_context,
            )

    async def get_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        agent_context: Optional[AgentContext] = None,
    ) -> Optional[SessionABC]:
        with storage_span(self._tenant_id, "session", "get"):
            return await self._backend.get_session(
                app_name=self._scope(app_name),
                user_id=user_id,
                session_id=session_id,
                agent_context=agent_context,
            )

    async def list_sessions(
        self,
        *,
        app_name: str,
        user_id: Optional[str] = None,
    ) -> ListSessionsResponse:
        with storage_span(self._tenant_id, "session", "list"):
            return await self._backend.list_sessions(app_name=self._scope(app_name), user_id=user_id)

    async def delete_session(self, *, app_name: str, user_id: str, session_id: str) -> None:
        with storage_span(self._tenant_id, "session", "delete"):
            return await self._backend.delete_session(app_name=self._scope(app_name),
                                                      user_id=user_id,
                                                      session_id=session_id)

    async def append_event(self, session: SessionABC, event: ResponseABC) -> ResponseABC:
        with storage_span(self._tenant_id, "session", "append_event"):
            return await self._backend.append_event(session, event)

    async def update_session(self, session: SessionABC) -> None:
        with storage_span(self._tenant_id, "session", "update"):
            return await self._backend.update_session(session)

    async def create_session_summary(self, session: SessionABC, ctx: "InvocationContext" = None) -> None:
        with storage_span(self._tenant_id, "summary", "create"):
            return await self._backend.create_session_summary(session, ctx=ctx)

    async def get_session_summary(self, session: SessionABC) -> Optional[str]:
        with storage_span(self._tenant_id, "summary", "get"):
            return await self._backend.get_session_summary(session)

    async def close(self) -> None:
        return await self._backend.close()
