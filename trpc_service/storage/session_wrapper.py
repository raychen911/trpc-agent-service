"""Platform metadata wrapper around an SDK SessionService."""

from __future__ import annotations

from contextlib import contextmanager
import time
from typing import Any

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.abc import SessionServiceABC

from trpc_service.tenant.context import get_current_tenant
from trpc_service.storage.guard import verify_current_lease
from trpc_service.metrics import platform_span


class RequestTaggingSessionService(SessionServiceABC):
    """Attach the trusted platform request_id before a non-partial Event persists."""

    def __init__(self, delegate: Any, backend: str = "unknown", metrics: Any = None) -> None:
        self._delegate = delegate
        self._backend = backend
        self._metrics = metrics

    @contextmanager
    def _span(self, operation: str):
        started = time.perf_counter()
        result = "succeeded"
        try:
            with platform_span(f"storage.session.{operation}",
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
                                      store="session",
                                      backend=self._backend,
                                      operation=operation,
                                      result=result)

    async def create_session(self, **kwargs):
        with self._span("create"):
            await verify_current_lease()
            return await self._delegate.create_session(**kwargs)

    async def get_session(self, **kwargs):
        with self._span("get"):
            return await self._delegate.get_session(**kwargs)

    async def list_sessions(self, **kwargs):
        with self._span("list"):
            return await self._delegate.list_sessions(**kwargs)

    async def delete_session(self, **kwargs):
        with self._span("delete"):
            await verify_current_lease()
            return await self._delegate.delete_session(**kwargs)

    async def update_session(self, session):
        with self._span("update"):
            await verify_current_lease()
            return await self._delegate.update_session(session)

    async def create_session_summary(self, session, ctx=None):
        with self._span("summarize"):
            await verify_current_lease()
            return await self._delegate.create_session_summary(session, ctx=ctx)

    async def get_session_summary(self, session):
        with self._span("get_summary"):
            return await self._delegate.get_session_summary(session)

    async def close(self):
        await self._delegate.close()

    async def append_event(self, session: Any, event: Event) -> Event:
        with self._span("append_event"):
            await verify_current_lease()
            try:
                event.request_id = get_current_tenant().request_id
            except RuntimeError:
                # SDK-only use outside a platform request remains supported.
                pass
            result = await self._delegate.append_event(session, event)
            await verify_current_lease()
            return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)
