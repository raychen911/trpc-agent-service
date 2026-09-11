"""Tenant guard for the public tRPC SessionService interface."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import asyncio
from trpc_agent_sdk.abc import SessionServiceABC

from .errors import TenantBoundaryError
from trpc_service.telemetry.runtime import measured


@dataclass(frozen=True)
class StorageExecutionContext:
    tenant_id: str
    execution_id: str
    fencing_token: int


_EXECUTION_CONTEXT: ContextVar[StorageExecutionContext | None] = ContextVar("storage_execution_context", default=None)


@contextmanager
def execution_scope(tenant_id: str, execution_id: str, fencing_token: int):
    current = _EXECUTION_CONTEXT.get()
    if current is not None and current.tenant_id != tenant_id:
        raise TenantBoundaryError("nested storage execution cannot change tenant")
    token = _EXECUTION_CONTEXT.set(StorageExecutionContext(tenant_id, execution_id, fencing_token))
    try:
        yield _EXECUTION_CONTEXT.get()
    finally:
        _EXECUTION_CONTEXT.reset(token)


class GuardedSessionService(SessionServiceABC):
    """Validate scope and delegate without depending on tRPC private modules."""

    def __init__(self, service: SessionServiceABC, tenant_id: str, agent_app_id: str, *, threaded=False) -> None:
        self._service = service
        self.tenant_id = tenant_id
        self.agent_app_id = agent_app_id
        self.app_name = f"{tenant_id}:{agent_app_id}"
        self.threaded = threaded
        self._thread_lock = asyncio.Lock()

    async def _call(self, method, **kwargs):
        operation = getattr(self._service, method)
        if self.threaded:
            # Public SDK sync SQL mode; serialize initialization and keep blocking
            # I/O off the heartbeat loop. Cancellation waits for the write to settle.
            async with self._thread_lock:
                task = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(operation(**kwargs))))
                try:
                    return await asyncio.shield(task)
                except asyncio.CancelledError:
                    await task
                    raise
        return await operation(**kwargs)

    @property
    def raw_service(self) -> SessionServiceABC:
        return self._service

    @property
    def summarizer_manager(self):
        return getattr(self._service, "summarizer_manager", None)

    @property
    def session_config(self):
        return getattr(self._service, "session_config", None)

    def _validate(self, app_name: str, agent_context=None) -> None:
        if app_name != self.app_name:
            raise TenantBoundaryError("session app_name is outside the tenant application scope")
        if agent_context is not None:
            context_tenant = agent_context.metadata.get("tenant_id")
            if context_tenant is not None and context_tenant != self.tenant_id:
                raise TenantBoundaryError("AgentContext tenant does not match session scope")
        execution = _EXECUTION_CONTEXT.get()
        if execution is not None and execution.tenant_id != self.tenant_id:
            raise TenantBoundaryError("execution tenant does not match session scope")

    @measured("session.create")
    async def create_session(self, *, app_name, user_id, state=None, session_id=None, agent_context=None):
        self._validate(app_name, agent_context)
        return await self._call(
            "create_session",
            app_name=app_name,
            user_id=user_id,
            state=state,
            session_id=session_id,
            agent_context=agent_context,
        )

    @measured("session.get")
    async def get_session(self, *, app_name, user_id, session_id, agent_context=None):
        self._validate(app_name, agent_context)
        return await self._call(
            "get_session",
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
            agent_context=agent_context,
        )

    async def list_sessions(self, *, app_name, user_id):
        self._validate(app_name)
        return await self._call("list_sessions", app_name=app_name, user_id=user_id)

    async def delete_session(self, *, app_name, user_id, session_id):
        self._validate(app_name)
        return await self._call("delete_session", app_name=app_name, user_id=user_id, session_id=session_id)

    @measured("session.append")
    async def append_event(self, session, event):
        self._validate(session.app_name)
        execution = _EXECUTION_CONTEXT.get()
        if execution is not None:
            event.custom_metadata = {**(event.custom_metadata or {}),
                                     "platform_execution_id": execution.execution_id,
                                     "platform_fencing_token": execution.fencing_token}
        return await self._call("append_event", session=session, event=event)

    @measured("session.update")
    async def update_session(self, session):
        self._validate(session.app_name)
        return await self._call("update_session", session=session)

    async def create_session_summary(self, session, ctx=None):
        self._validate(session.app_name, getattr(ctx, "agent_context", None))
        return await self._service.create_session_summary(session, ctx=ctx)

    async def get_session_summary(self, session):
        self._validate(session.app_name)
        return await self._service.get_session_summary(session)

    async def close(self):
        return None
