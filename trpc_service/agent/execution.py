"""Tenant-scoped Agent execution independent of HTTP and IM transports."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any
from uuid import uuid4

from sqlalchemy.exc import IntegrityError
from trpc_agent_sdk.types import Content, Part

from trpc_service.agent.runtime import RunnerProvider
from trpc_service.config.models import (
    AuditDecision,
    AuditLogRecord,
    ChannelType,
    EventType,
    MemoryRecord,
    SessionEventRecord,
    SessionRecord,
    SummaryRecord,
    TenantRecord,
    TenantStatus,
)
from trpc_service.metrics import ServiceMetrics
from trpc_service.storage.database import Database
from trpc_service.storage.lock import RedisSessionLockManager
from trpc_service.storage.repositories import (
    AgentAppRepository,
    AuditLogRepository,
    SessionEventRepository,
    SessionRepository,
    SummaryRepository,
    TenantRepository,
)
from trpc_service.storage.router import TenantStorageRouter
from trpc_service.telemetry import tracer
from trpc_service.tenant.context import TenantContext, tenant_scope


class TenantUnavailableError(LookupError):
    pass


class AgentAppUnavailableError(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class RunAgentCommand:
    tenant_id: str
    app_id: str
    user_id: str
    session_id: str
    message: str
    channel: ChannelType = ChannelType.HTTP
    trace_id: str = field(default_factory=lambda: uuid4().hex)


@dataclass(frozen=True, slots=True)
class ToolEvent:
    type: str
    name: str
    data: Any


@dataclass(frozen=True, slots=True)
class AgentReply:
    tenant_id: str
    app_id: str
    user_id: str
    session_id: str
    trace_id: str
    text: str
    tool_events: tuple[ToolEvent, ...]


class AgentExecutionService:
    def __init__(
        self,
        database: Database,
        runners: RunnerProvider,
        storage_router: TenantStorageRouter | None = None,
        metrics: ServiceMetrics | None = None,
        timeout_seconds: float = 120.0,
        distributed_lock: RedisSessionLockManager | None = None,
    ) -> None:
        self._database = database
        self._runners = runners
        self._storage_router = storage_router or TenantStorageRouter(database)
        self._metrics = metrics or ServiceMetrics()
        self._timeout_seconds = timeout_seconds
        self._distributed_lock = distributed_lock
        self._session_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def execute(self, command: RunAgentCommand) -> AgentReply:
        with tracer.start_as_current_span(
            "agent.execute",
            attributes={
                "tenant.id": command.tenant_id,
                "agent.app_id": command.app_id,
                "messaging.system": command.channel.value,
                "trpc.trace_id": command.trace_id,
            },
        ):
            lock = await self._get_session_lock(command.tenant_id, command.session_id)
            async with lock:
                if self._distributed_lock is None:
                    return await self._execute_locked(command)
                async with self._distributed_lock.lock(command.tenant_id, command.session_id):
                    return await self._execute_locked(command)

    async def _execute_locked(self, command: RunAgentCommand) -> AgentReply:
        started = perf_counter()
        tenant = await TenantRepository(self._database).get(command.tenant_id)
        if tenant is None or tenant.status != TenantStatus.ACTIVE:
            raise TenantUnavailableError("tenant is not active or does not exist")

        app = await AgentAppRepository(self._database).get(command.tenant_id, command.app_id)
        if app is None or not app.is_active:
            raise AgentAppUnavailableError("agent app is not active or does not exist")

        session_repository = SessionRepository(self._database)
        session = await session_repository.get(command.tenant_id, command.session_id)
        if session is None:
            try:
                session = await session_repository.create(
                    SessionRecord(
                        tenant_id=command.tenant_id,
                        session_id=command.session_id,
                        app_id=command.app_id,
                        principal_id=command.user_id,
                        channel_type=command.channel,
                    )
                )
            except IntegrityError:
                session = await session_repository.get(command.tenant_id, command.session_id)
                if session is None:
                    raise
        elif session.app_id != command.app_id or session.principal_id != command.user_id:
            raise AgentAppUnavailableError("session does not belong to this app and user")

        event_repository = SessionEventRepository(self._database)
        sequence = session.last_event_sequence
        sequence += 1
        await event_repository.append(
            SessionEventRecord(
                event_id=uuid4().hex,
                tenant_id=command.tenant_id,
                session_id=command.session_id,
                sequence=sequence,
                event_type=EventType.USER_MESSAGE,
                payload={"text": command.message},
                trace_id=command.trace_id,
            )
        )

        partial_reply_parts: list[str] = []
        final_reply_text: str | None = None
        tool_events: list[ToolEvent] = []
        decision = AuditDecision.ALLOWED
        error_type: str | None = None
        progress_updated = False
        try:
            with tenant_scope(TenantContext(command.tenant_id, command.app_id, command.trace_id)):
                runner = await self._runners.get_runner(app)
                content = Content(parts=[Part.from_text(text=command.message)])
                with tracer.start_as_current_span("runner.run"):
                    async with asyncio.timeout(self._timeout_seconds):
                        async for event in runner.run_async(
                            user_id=f"{command.tenant_id}:{command.user_id}",
                            session_id=command.session_id,
                            new_message=content,
                        ):
                            if event.is_error():
                                raise RuntimeError(
                                    event.error_message or event.error_code or "agent error"
                                )
                            if not event.content or not event.content.parts:
                                continue
                            event_text_parts: list[str] = []
                            for part in event.content.parts:
                                if part.thought:
                                    continue
                                if part.text:
                                    event_text_parts.append(part.text)
                                elif part.function_call:
                                    tool_events.append(
                                        ToolEvent(
                                            type="tool_call",
                                            name=part.function_call.name,
                                            data=dict(part.function_call.args or {}),
                                        )
                                    )
                                elif part.function_response:
                                    tool_events.append(
                                        ToolEvent(
                                            type="tool_result",
                                            name=part.function_response.name,
                                            data=part.function_response.response,
                                        )
                                    )

                            event_text = "".join(event_text_parts)
                            if event_text and event.is_final_response():
                                final_reply_text = event_text
                            elif event_text and event.partial:
                                partial_reply_parts.append(event_text)

            for tool_event in tool_events:
                self._metrics.tool_calls.labels(
                    tool_event.name,
                    "called" if tool_event.type == "tool_call" else "completed",
                ).inc()
                sequence += 1
                event_type = (
                    EventType.TOOL_CALL if tool_event.type == "tool_call" else EventType.TOOL_RESULT
                )
                await event_repository.append(
                    SessionEventRecord(
                        event_id=uuid4().hex,
                        tenant_id=command.tenant_id,
                        session_id=command.session_id,
                        sequence=sequence,
                        event_type=event_type,
                        payload={"name": tool_event.name, "data": tool_event.data},
                        trace_id=command.trace_id,
                    )
                )

            sequence += 1
            reply_text = (
                final_reply_text if final_reply_text is not None else "".join(partial_reply_parts)
            ).strip()
            agent_event = await event_repository.append(
                SessionEventRecord(
                    event_id=uuid4().hex,
                    tenant_id=command.tenant_id,
                    session_id=command.session_id,
                    sequence=sequence,
                    event_type=EventType.AGENT_MESSAGE,
                    payload={"text": reply_text},
                    trace_id=command.trace_id,
                )
            )
            updated = await session_repository.update_progress(
                command.tenant_id,
                command.session_id,
                expected_version=session.version,
                last_event_sequence=sequence,
                state={"last_trace_id": command.trace_id},
            )
            if not updated:
                raise RuntimeError("session was concurrently modified")
            progress_updated = True
            await self._project_turn(
                tenant=tenant,
                command=command,
                reply_text=reply_text,
                agent_event_id=agent_event.event_id,
                source_end_sequence=sequence,
            )
            self._metrics.agent_executions.labels(command.channel.value, "completed").inc()
            return AgentReply(
                tenant_id=command.tenant_id,
                app_id=command.app_id,
                user_id=command.user_id,
                session_id=command.session_id,
                trace_id=command.trace_id,
                text=reply_text,
                tool_events=tuple(tool_events),
            )
        except Exception as exc:
            decision = AuditDecision.ERROR
            error_type = type(exc).__name__
            self._metrics.agent_executions.labels(command.channel.value, "failed").inc()
            if not progress_updated:
                sequence += 1
                await event_repository.append(
                    SessionEventRecord(
                        event_id=uuid4().hex,
                        tenant_id=command.tenant_id,
                        session_id=command.session_id,
                        sequence=sequence,
                        event_type=EventType.SYSTEM,
                        payload={"status": "error", "error_type": error_type},
                        trace_id=command.trace_id,
                    )
                )
                await session_repository.update_progress(
                    command.tenant_id,
                    command.session_id,
                    expected_version=session.version,
                    last_event_sequence=sequence,
                    state={"last_trace_id": command.trace_id, "last_status": "error"},
                )
            raise
        finally:
            await AuditLogRepository(self._database).create(
                AuditLogRecord(
                    log_id=uuid4().hex,
                    trace_id=command.trace_id,
                    tenant_id=command.tenant_id,
                    channel=command.channel,
                    user_id=command.user_id,
                    session_id=command.session_id,
                    agent_name=app.app_id,
                    tool_name=next((item.name for item in tool_events), None),
                    decision=decision,
                    latency_ms=max(0, round((perf_counter() - started) * 1000)),
                    error_type=error_type,
                )
            )

    async def _project_turn(
        self,
        *,
        tenant: TenantRecord,
        command: RunAgentCommand,
        reply_text: str,
        agent_event_id: str,
        source_end_sequence: int,
    ) -> None:
        """Persist a deterministic turn projection after event/state commit."""

        started = perf_counter()
        try:
            with tracer.start_as_current_span("memory.write"):
                await self._storage_router.memory_store_for(tenant).create(
                    MemoryRecord(
                        memory_id=uuid4().hex,
                        tenant_id=command.tenant_id,
                        principal_id=command.user_id,
                        content=f"User: {command.message}\nAssistant: {reply_text}",
                        source_event_id=agent_event_id,
                        metadata_data={
                            "app_id": command.app_id,
                            "session_id": command.session_id,
                            "visibility": "principal",
                            "kind": "conversation_turn",
                        },
                    )
                )
            self._metrics.storage_operations.labels("memory", "completed").inc()
        except Exception:
            self._metrics.storage_operations.labels("memory", "failed").inc()
            raise
        finally:
            self._metrics.storage_latency.labels("memory").observe(perf_counter() - started)

        previous = await SummaryRepository(self._database).latest(
            command.tenant_id, command.session_id
        )
        summary_started = perf_counter()
        try:
            with tracer.start_as_current_span("summary.write"):
                await SummaryRepository(self._database).create(
                    SummaryRecord(
                        summary_id=uuid4().hex,
                        tenant_id=command.tenant_id,
                        session_id=command.session_id,
                        content=f"User: {command.message}\nAssistant: {reply_text}",
                        source_end_sequence=source_end_sequence,
                        version=1 if previous is None else previous.version + 1,
                    )
                )
            self._metrics.storage_operations.labels("summary", "completed").inc()
        except Exception:
            self._metrics.storage_operations.labels("summary", "failed").inc()
            raise
        finally:
            self._metrics.storage_latency.labels("summary").observe(
                perf_counter() - summary_started
            )

    async def _get_session_lock(self, tenant_id: str, session_id: str) -> asyncio.Lock:
        key = (tenant_id, session_id)
        async with self._locks_guard:
            return self._session_locks.setdefault(key, asyncio.Lock())


__all__ = [
    "AgentAppUnavailableError",
    "AgentExecutionService",
    "AgentReply",
    "RunAgentCommand",
    "TenantUnavailableError",
    "ToolEvent",
]
