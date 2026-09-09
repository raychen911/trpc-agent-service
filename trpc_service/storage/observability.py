# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Observable SDK storage adapters that preserve the original service API."""

from __future__ import annotations

from contextlib import contextmanager
import time
from typing import Any

from trpc_agent_sdk.abc import MemoryServiceABC

from trpc_service.metrics import platform_span


class TracingMemoryService(MemoryServiceABC):
    """Add low-cardinality spans around SDK Memory operations."""

    def __init__(self, delegate: Any, backend: str = "unknown", metrics: Any = None) -> None:
        super().__init__(enabled=bool(delegate.enabled))
        self._delegate = delegate
        self._backend = backend
        self._metrics = metrics

    @property
    def enabled(self) -> bool:
        return bool(self._delegate.enabled)

    @contextmanager
    def _span(self, operation: str):
        started = time.perf_counter()
        result = "succeeded"
        try:
            with platform_span(f"storage.memory.{operation}",
                               attributes={
                                   "trpc_service.backend": self._backend,
                                   "trpc_service.operation": operation
                               }):
                yield
        except BaseException:
            result = "failed"
            raise
        finally:
            if self._metrics is not None:
                self._metrics.observe("trpc_service_storage_duration_seconds",
                                      max(0.0,
                                          time.perf_counter() - started),
                                      store="memory",
                                      backend=self._backend,
                                      operation=operation,
                                      result=result)

    async def store_session(self, session, agent_context=None) -> None:
        with self._span("store_session"):
            await self._delegate.store_session(session, agent_context=agent_context)

    async def search_memory(self, key: str, query: str, limit: int = 10, agent_context=None):
        with self._span("search"):
            return await self._delegate.search_memory(key, query, limit=limit, agent_context=agent_context)

    async def close(self) -> None:
        await self._delegate.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)
