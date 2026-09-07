# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant-scoped memory service."""

from __future__ import annotations

from typing import Optional

from trpc_agent_sdk.abc import MemoryServiceABC
from trpc_agent_sdk.abc import SessionABC
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.types import SearchMemoryResponse

from trpc_service._utils import scope_key
from trpc_service.metrics._observability import storage_span


class TenantMemoryService(MemoryServiceABC):
    """Memory service that isolates a tenant by scoping the memory key.

    ``store_session`` needs no key rewriting: the session it receives already
    carries a tenant-scoped ``app_name`` (from :class:`TenantSessionService`),
    so its ``save_key`` is naturally scoped. ``search_memory`` scopes the
    caller-supplied key idempotently.
    """

    def __init__(self, backend: MemoryServiceABC, tenant_id: str) -> None:
        super().__init__()
        self._backend = backend
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> str:
        return self._tenant_id

    @property
    def backend(self) -> MemoryServiceABC:
        return self._backend

    @property
    def enabled(self) -> bool:
        return self._backend.enabled

    async def store_session(self, session: SessionABC, agent_context: Optional[AgentContext] = None) -> None:
        with storage_span(self._tenant_id, "memory", "store"):
            return await self._backend.store_session(session, agent_context=agent_context)

    async def search_memory(
        self,
        key: str,
        query: str,
        limit: int = 10,
        agent_context: Optional[AgentContext] = None,
    ) -> SearchMemoryResponse:
        with storage_span(self._tenant_id, "memory", "search"):
            return await self._backend.search_memory(
                scope_key(self._tenant_id, key),
                query,
                limit=limit,
                agent_context=agent_context,
            )

    async def close(self) -> None:
        return await self._backend.close()
