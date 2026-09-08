# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Stateless tenant worker.

The worker turns a normalized inbound message into an agent run for the right
tenant and session. It holds no per-request state: the session service is a
shared backend wrapped with :class:`TenantSessionService`, so any worker can
serve any tenant/session (no sticky sessions required).
"""

from __future__ import annotations

import inspect
import time
from typing import Any
from typing import Callable
from typing import Optional

from trpc_agent_sdk.abc import MemoryServiceABC
from trpc_agent_sdk.abc import SessionABC
from trpc_agent_sdk.abc import SessionServiceABC
from trpc_agent_sdk.agents import BaseAgent
from trpc_agent_sdk.context import new_agent_context
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part

from trpc_service.log import AuditLogEntry
from trpc_service.log import AuditLogger
from trpc_service.channels import InboundMessage
from trpc_service.channels import generate_session_id
from trpc_service.tool import ConfirmationManager
from trpc_service.tool import SensitiveDataRedactor
from trpc_service.tool import apply_tenant_governance
from trpc_service.tool import parse_confirmation_token
from trpc_service.metrics._observability import attach_tenant_to_span
from trpc_service.metrics._observability import current_trace_id
from trpc_service.metrics import EnterpriseMetrics
from trpc_service.metrics import get_enterprise_metrics
from trpc_service.workspace import TenantMemoryService
from trpc_service.workspace import TenantSessionService
from trpc_service.channels import CHAT_GROUP
from trpc_service.tenant import AppInfo
from trpc_service.tenant import Tenant
from trpc_service.tenant import TenantConfigManager
from trpc_service.tenant import TenantStatus
from ._locks import LocalSessionLockManager

AgentFactory = Callable[[Tenant], BaseAgent]
SessionServiceFactory = Callable[[Tenant], SessionServiceABC]
MemoryServiceFactory = Callable[[Tenant], MemoryServiceABC]

CONFIRMED_TOOLS_KEY = "confirmed_tools"
"""Session-state key holding tool names approved via HITL confirmation."""


async def collect_final_text(events) -> str:
    """Collect the final assistant text from a stream of agent events.

    Mirrors the framework's own aggregation: partial deltas and tool-call
    drafts are skipped to avoid duplicated or half-formed answers.
    """
    final_text = ""
    async for event in events:
        if not event.content or not event.content.parts:
            continue
        if event.partial:
            continue
        if any(part.function_call for part in event.content.parts):
            continue
        event_text = "".join(part.text for part in event.content.parts
                             if part.text and not getattr(part, "thought", False))
        if event_text:
            final_text = event_text if not final_text or event_text.startswith(final_text) else final_text + event_text
    return final_text


class TenantWorker:
    """Runs an agent turn for a tenant and returns the final reply text."""

    def __init__(
        self,
        *,
        manager: TenantConfigManager,
        agent_factory: AgentFactory,
        session_service_factory: SessionServiceFactory,
        memory_service_factory: Optional[MemoryServiceFactory] = None,
        app_name: str = "default",
        audit_logger: Optional[AuditLogger] = None,
        confirmation_manager: Optional[ConfirmationManager] = None,
        session_lock_manager: Optional[Any] = None,
        metrics: Optional[EnterpriseMetrics] = None,
    ) -> None:
        self._manager = manager
        self._agent_factory = agent_factory
        self._session_factory = session_service_factory
        self._memory_factory = memory_service_factory
        self._app_name = app_name
        self._audit_logger = audit_logger
        self._confirmation_manager = confirmation_manager
        self._session_locks = session_lock_manager or LocalSessionLockManager()
        self._metrics = metrics or get_enterprise_metrics()

    @property
    def metrics(self) -> EnterpriseMetrics:
        return self._metrics

    def resolve_tenant(self, tenant_id: str, config_revision: Optional[int] = None) -> Optional[Tenant]:
        tenant = (self._manager.get_version(tenant_id, config_revision)
                  if config_revision is not None else self._manager.get(tenant_id))
        if tenant is None or tenant.status != TenantStatus.ACTIVE:
            return None
        return tenant

    async def _get_or_create_session(self, session_service: SessionServiceABC, app_name: str, user_id: str,
                                     session_id: str) -> SessionABC:
        session = await session_service.get_session(app_name=app_name, user_id=user_id, session_id=session_id)
        if session is None:
            session = await session_service.create_session(app_name=app_name, user_id=user_id, session_id=session_id)
        return session

    def _resolve_app(self, tenant: Tenant, requested_app_id: Optional[str]) -> tuple[str, Tenant, Optional[AppInfo]]:
        """Resolve one tenant Agent app while preserving legacy single-app configs."""
        apps = {item.app_id: item for item in tenant.app_config.app_list}
        selected_id = requested_app_id or tenant.app_config.default_app_id
        if selected_id is None and len(apps) == 1:
            selected_id = next(iter(apps))
        if selected_id is None and len(apps) > 1:
            raise ValueError(f"tenant '{tenant.tenant_id}' requires an explicit agent_app_id")
        if selected_id is not None and selected_id not in apps:
            raise ValueError(f"agent app '{selected_id}' is not configured for tenant '{tenant.tenant_id}'")

        effective = tenant.model_copy(deep=True)
        selected = apps.get(selected_id) if selected_id is not None else None
        app_name = selected_id or self._app_name
        if selected is not None:
            effective.app_config.default_app_id = selected.app_id
            if selected.instruction is not None:
                effective.app_config.default_instruction = selected.instruction
        return app_name, effective, selected

    @staticmethod
    def _session_user_id(inbound: InboundMessage) -> str:
        """Use one storage owner per group while retaining the real sender in context."""
        if inbound.chat_type == CHAT_GROUP:
            return f"group:{inbound.chat_id}"
        return inbound.sender_id

    @staticmethod
    def _load_confirmed_tools(session: SessionABC, sender_id: str) -> list[str]:
        """Load one sender's one-shot grants from a possibly shared group session."""
        tools = session.state.get(CONFIRMED_TOOLS_KEY, {}) if session.state else {}
        if isinstance(tools, dict):
            sender_tools = tools.get(sender_id, [])
            return list(sender_tools) if isinstance(sender_tools, list) else []
        # Read legacy list state as belonging to the current sender. It is
        # removed before the next run, so old grants cannot remain reusable.
        return list(tools) if isinstance(tools, list) else []

    @staticmethod
    async def _consume_confirmed_tools(
        session: SessionABC,
        session_service: SessionServiceABC,
        sender_id: str,
        confirmed_tools: list[str],
    ) -> None:
        """Remove grants before execution so approval is identity-bound and one-shot."""
        if not confirmed_tools:
            return
        stored = session.state.get(CONFIRMED_TOOLS_KEY, {}) if session.state else {}
        if isinstance(stored, dict):
            stored = dict(stored)
            stored.pop(sender_id, None)
            session.state[CONFIRMED_TOOLS_KEY] = stored
        else:
            session.state.pop(CONFIRMED_TOOLS_KEY, None)
        await session_service.update_session(session)

    async def handle(self, tenant_id: str, channel: str, inbound: InboundMessage) -> str:
        """Execute a turn and return the final assistant text (empty on failure)."""
        started = time.perf_counter()
        config_revision = inbound.metadata.get("config_revision")
        tenant = self.resolve_tenant(tenant_id, config_revision)
        if tenant is None:
            self._metrics.increment(
                "agent_requests_total",
                tenant_id=tenant_id,
                channel=channel,
                outcome="tenant_unavailable",
            )
            return ""

        attach_tenant_to_span(tenant_id)
        session_id = generate_session_id(tenant_id, channel, inbound.chat_type, inbound.sender_id, inbound.chat_id)
        outcome = "error"
        error_type = None
        lock_acquired = False
        lock_wait_started = time.perf_counter()
        try:
            requested_app_id = inbound.metadata.get("agent_app_id")
            app_name, effective_tenant, _selected_app = self._resolve_app(tenant, requested_app_id)
            session_user_id = self._session_user_id(inbound)
            lock_key = f"{tenant_id}:{app_name}:{session_user_id}:{session_id}"
            async with self._session_locks.acquire(lock_key) as lease:
                lock_acquired = True
                self._metrics.observe(
                    "agent_session_lock_duration_ms",
                    (time.perf_counter() - lock_wait_started) * 1000,
                    tenant_id=tenant_id,
                    phase="wait",
                    outcome="success",
                )
                lock_held_started = time.perf_counter()
                try:
                    result = await self._handle_locked(
                        effective_tenant,
                        tenant_id,
                        channel,
                        inbound,
                        session_id,
                        session_user_id,
                        app_name,
                        getattr(lease, "fencing_token", 0),
                        started,
                    )
                    if hasattr(lease, "assert_valid"):
                        lease.assert_valid()
                    outcome = "success"
                    return result
                finally:
                    self._metrics.observe(
                        "agent_session_lock_duration_ms",
                        (time.perf_counter() - lock_held_started) * 1000,
                        tenant_id=tenant_id,
                        phase="hold",
                        outcome=outcome,
                    )
        except Exception as exc:
            error_type = type(exc).__name__
            await self._audit_turn(
                tenant,
                channel,
                inbound,
                session_id,
                "",
                decision="error",
                error_type=type(exc).__name__,
                latency_ms=int((time.perf_counter() - started) * 1000),
            )
            raise
        finally:
            if not lock_acquired:
                self._metrics.observe(
                    "agent_session_lock_duration_ms",
                    (time.perf_counter() - lock_wait_started) * 1000,
                    tenant_id=tenant_id,
                    phase="wait",
                    outcome="error",
                    error_type=error_type,
                )
            self._metrics.increment(
                "agent_requests_total",
                tenant_id=tenant_id,
                channel=channel,
                outcome=outcome,
                error_type=error_type,
            )

    async def _handle_locked(
        self,
        tenant: Tenant,
        tenant_id: str,
        channel: str,
        inbound: InboundMessage,
        session_id: str,
        session_user_id: str,
        app_name: str,
        fencing_token: int,
        started: float,
    ) -> str:
        """Execute a turn while holding the session-scoped writer lock."""

        backend: SessionServiceABC = self._session_factory(tenant)
        session_service = TenantSessionService(
            backend,
            tenant_id,
            metrics=self._metrics,
            backend_name=tenant.storage_config.session_backend,
        )
        memory_service = None
        if self._memory_factory is not None:
            memory_service = TenantMemoryService(
                self._memory_factory(tenant),
                tenant_id,
                metrics=self._metrics,
                backend_name=tenant.storage_config.memory_backend,
            )
        storage_started = time.perf_counter()
        storage_outcome = "error"
        storage_error_type = None
        try:
            session = await self._get_or_create_session(session_service, app_name, session_user_id, session_id)
            storage_outcome = "success"
        except Exception as exc:
            storage_error_type = type(exc).__name__
            raise
        finally:
            self._metrics.observe(
                "agent_session_backend_latency_ms",
                (time.perf_counter() - storage_started) * 1000,
                tenant_id=tenant_id,
                operation="get_or_create",
                backend=tenant.storage_config.session_backend,
                outcome=storage_outcome,
                error_type=storage_error_type,
            )
        confirmed_tools = self._load_confirmed_tools(session, inbound.sender_id)

        # HITL: a confirmation reply ("确认 <token>") resolves the pending request
        # and records the approved tool so a subsequent run may execute it.
        token = parse_confirmation_token(inbound.text)
        if token is not None and self._confirmation_manager is not None:
            pending = self._confirmation_manager.resolve(token, approve=True)
            if inspect.isawaitable(pending):
                pending = await pending
            identity_matches = pending is not None and (pending.user_id is None or pending.user_id == inbound.sender_id)
            session_matches = pending is not None and (pending.session_id is None or pending.session_id == session_id)
            if pending is not None and pending.tenant_id == tenant_id and identity_matches and session_matches:
                if pending.tool_name not in confirmed_tools:
                    confirmed_tools.append(pending.tool_name)
                stored = session.state.get(CONFIRMED_TOOLS_KEY, {}) if session.state else {}
                grants = dict(stored) if isinstance(stored, dict) else {}
                grants[inbound.sender_id] = confirmed_tools
                session.state[CONFIRMED_TOOLS_KEY] = grants
                await session_service.update_session(session)
                return f"已确认执行工具「{pending.tool_name}」，请重新发起该操作。"
            return "确认码无效或已过期。"

        await self._consume_confirmed_tools(
            session,
            session_service,
            inbound.sender_id,
            confirmed_tools,
        )

        agent = apply_tenant_governance(
            self._agent_factory(tenant),
            tenant,
            confirmation_manager=self._confirmation_manager,
            audit_logger=self._audit_logger,
        )
        runner = Runner(
            app_name=app_name,
            agent=agent,
            session_service=session_service,
            memory_service=memory_service,
            close_session_service_on_close=False,
            close_memory_service_on_close=False,
        )

        agent_context = new_agent_context(
            metadata={
                "tenant_id": tenant_id,
                "channel": channel,
                "channel_user_id": inbound.sender_id,
                "channel_chat_id": inbound.chat_id,
                "channel_user_verified": inbound.metadata.get("user_verified", False),
                "confirmed_tools": confirmed_tools,
                "session_fencing_token": fencing_token,
                "config_revision": inbound.metadata.get("config_revision"),
                "turn_id": inbound.metadata.get("turn_id"),
            })
        new_message = Content(parts=[Part.from_text(text=inbound.text or "")])

        try:
            run_started = time.perf_counter()
            run_outcome = "error"
            run_error_type = None
            events = runner.run_async(
                user_id=session_user_id,
                session_id=session_id,
                new_message=new_message,
                agent_context=agent_context,
            )
            final_text = await collect_final_text(events)
            final_text = SensitiveDataRedactor().redact(final_text, tenant.audit_policy.desensitize_rules)
            run_outcome = "success"
        except Exception as exc:
            run_error_type = type(exc).__name__
            raise
        finally:
            self._metrics.observe(
                "agent_runner_latency_ms",
                (time.perf_counter() - run_started) * 1000,
                tenant_id=tenant_id,
                channel=channel,
                outcome=run_outcome,
                error_type=run_error_type,
            )
            await runner.close()

        await self._audit_turn(
            tenant,
            channel,
            inbound,
            session_id,
            final_text,
            agent_name=agent.name,
            latency_ms=int((time.perf_counter() - started) * 1000),
        )
        return final_text

    async def _audit_turn(
        self,
        tenant: Tenant,
        channel: str,
        inbound: InboundMessage,
        session_id: str,
        final_text: str,
        *,
        agent_name: Optional[str] = None,
        decision: str = "allow",
        latency_ms: Optional[int] = None,
        error_type: Optional[str] = None,
    ) -> None:
        if self._audit_logger is None or not tenant.audit_policy.enabled:
            return
        await self._audit_logger.log(
            AuditLogEntry(
                tenant_id=tenant.tenant_id,
                channel=channel,
                user_id=inbound.sender_id,
                session_id=session_id,
                message_id=inbound.message_id,
                turn_id=inbound.metadata.get("turn_id"),
                config_revision=inbound.metadata.get("config_revision"),
                agent_name=agent_name,
                decision=decision,
                latency_ms=latency_ms,
                error_type=error_type,
                trace_id=current_trace_id(),
                detail={
                    "reply_length": len(final_text),
                },
            ))
