# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for the stateless tenant worker (real Runner path)."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from trpc_agent_sdk.agents import BaseAgent
from trpc_service import EnterpriseMetrics
from trpc_service import InboundMessage
from trpc_service import TenantConfigManager
from trpc_service import TenantWorker
from trpc_service import generate_session_id
from trpc_service.channels import CHAT_PRIVATE
from trpc_service.channels import CHAT_GROUP
from trpc_service.tool import ConfirmationManager
from trpc_service.tool import parse_confirmation_token
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import AppConfig
from trpc_service.tenant import AppInfo
from trpc_service.tenant import Tenant
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part


class EchoAgent(BaseAgent):
    """Minimal agent that echoes the user message."""

    def __init__(self, name: str) -> None:
        super().__init__(name=name)

    async def _run_async_impl(self, ctx):
        user_text = "".join(p.text or "" for p in (ctx.user_content.parts if ctx.user_content else []))
        yield Event(author=self.name, content=Content(parts=[Part.from_text(text=f"echo:{user_text}")]), partial=False)


class FailingAgent(BaseAgent):

    async def _run_async_impl(self, ctx):
        if False:  # pragma: no cover - makes this an async generator
            yield
        raise RuntimeError("runner failed")


class ConfirmationCaptureAgent(BaseAgent):

    def __init__(self, name: str, captured: list[list[str]]) -> None:
        super().__init__(name=name)
        self._captured = captured

    async def _run_async_impl(self, ctx):
        self._captured.append(ctx.agent_context.get_metadata("confirmed_tools") or [])
        yield Event(author=self.name, content=Content(parts=[Part.from_text(text="ok")]), partial=False)


def _has_metric(metrics, kind, name, **expected):
    expected = {key: str(value) for key, value in expected.items()}
    return any(item["name"] == name and all(item["attributes"].get(key) == value for key, value in expected.items())
               for item in metrics.snapshot()[kind])


def _worker() -> TenantWorker:
    manager = TenantConfigManager()
    manager.register(Tenant(tenant_id="t_a", name="A", model=ModelEndpoint(model_name="m")))
    manager.register(Tenant(tenant_id="t_b", name="B", model=ModelEndpoint(model_name="m")))
    shared = InMemorySessionService()
    metrics = EnterpriseMetrics(meter=False)
    return manager, TenantWorker(
        manager=manager,
        agent_factory=lambda t: EchoAgent(name=t.tenant_id),
        session_service_factory=lambda t: shared,
        metrics=metrics,
    )


async def test_worker_returns_agent_reply():
    _, worker = _worker()
    inbound = InboundMessage(channel="wecom",
                             chat_id="u1",
                             chat_type=CHAT_PRIVATE,
                             sender_id="u1",
                             message_id="m1",
                             text="hi")
    reply = await worker.handle("t_a", "wecom", inbound)
    assert "echo:hi" in reply
    assert _has_metric(worker.metrics, "counters", "agent_requests_total", outcome="success")
    assert _has_metric(worker.metrics, "histograms", "agent_runner_latency_ms", outcome="success")
    assert _has_metric(worker.metrics,
                       "histograms",
                       "agent_session_backend_latency_ms",
                       operation="get_or_create",
                       outcome="success")
    assert _has_metric(worker.metrics, "histograms", "agent_session_lock_duration_ms", phase="wait")
    assert _has_metric(worker.metrics, "histograms", "agent_session_lock_duration_ms", phase="hold")


async def test_worker_redacts_sensitive_data_from_final_reply():
    _, worker = _worker()
    inbound = InboundMessage(
        channel="wecom",
        chat_id="u1",
        chat_type=CHAT_PRIVATE,
        sender_id="u1",
        message_id="m-redact",
        text="call 13812345678 with key sk-abcdefghijklmnop",
    )
    reply = await worker.handle("t_a", "wecom", inbound)
    assert "13812345678" not in reply
    assert "sk-abcdefghijklmnop" not in reply
    assert "***" in reply


async def test_worker_isolates_tenants_on_shared_backend():
    _, worker = _worker()
    inbound = InboundMessage(channel="wecom",
                             chat_id="u1",
                             chat_type=CHAT_PRIVATE,
                             sender_id="u1",
                             message_id="m1",
                             text="hi")
    await worker.handle("t_a", "wecom", inbound)
    await worker.handle("t_b", "wecom", inbound)
    # Same user/session on a shared backend must still produce distinct sessions.
    sid_a = generate_session_id("t_a", "wecom", CHAT_PRIVATE, "u1", "u1")
    sid_b = generate_session_id("t_b", "wecom", CHAT_PRIVATE, "u1", "u1")
    assert sid_a != sid_b


async def test_group_members_share_session_and_binding_selects_agent_app():
    manager = TenantConfigManager()
    manager.register(
        Tenant(
            tenant_id="t_a",
            name="A",
            model=ModelEndpoint(model_name="m"),
            app_config=AppConfig(
                app_list=[
                    AppInfo(app_id="sales", instruction="sales prompt"),
                    AppInfo(app_id="support", instruction="support prompt"),
                ],
                default_app_id="sales",
            ),
        ))
    shared = InMemorySessionService()
    selected = []

    def agent_factory(tenant):
        selected.append((tenant.app_config.default_app_id, tenant.app_config.default_instruction))
        return EchoAgent(name=tenant.app_config.default_app_id or tenant.tenant_id)

    worker = TenantWorker(
        manager=manager,
        agent_factory=agent_factory,
        session_service_factory=lambda tenant: shared,
    )
    first = InboundMessage(
        channel="wecom",
        chat_id="group-1",
        chat_type=CHAT_GROUP,
        sender_id="alice",
        message_id="group-1-a",
        text="one",
        metadata={"agent_app_id": "support"},
    )
    second = first.model_copy(update={"sender_id": "bob", "message_id": "group-1-b", "text": "two"})

    await worker.handle("t_a", "support_wecom", first)
    await worker.handle("t_a", "support_wecom", second)

    session_id = generate_session_id("t_a", "support_wecom", CHAT_GROUP, "alice", "group-1")
    shared_session = await shared.get_session(
        app_name="t_a:support",
        user_id="group:group-1",
        session_id=session_id,
    )
    assert shared_session is not None
    assert await shared.get_session(app_name="t_a:support", user_id="alice", session_id=session_id) is None
    assert await shared.get_session(app_name="t_a:support", user_id="bob", session_id=session_id) is None
    assert selected == [("support", "support prompt"), ("support", "support prompt")]


async def test_worker_rejects_ambiguous_or_unknown_agent_app():
    manager = TenantConfigManager()
    manager.register(
        Tenant(
            tenant_id="t_a",
            name="A",
            model=ModelEndpoint(model_name="m"),
            app_config=AppConfig(app_list=[AppInfo(app_id="one"), AppInfo(app_id="two")]),
        ))
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda tenant: EchoAgent(name=tenant.tenant_id),
        session_service_factory=lambda tenant: InMemorySessionService(),
    )
    inbound = InboundMessage(channel="wecom", chat_id="u1", sender_id="u1", message_id="app", text="hi")

    with pytest.raises(ValueError, match="explicit agent_app_id"):
        await worker.handle("t_a", "wecom", inbound)
    with pytest.raises(ValueError, match="not configured"):
        await worker.handle(
            "t_a",
            "wecom",
            inbound.model_copy(update={"metadata": {
                "agent_app_id": "missing"
            }}),
        )
    assert _has_metric(worker.metrics, "counters", "agent_requests_total", outcome="error", error_type="ValueError")


async def test_worker_rejects_unknown_tenant():
    _, worker = _worker()
    inbound = InboundMessage(channel="wecom",
                             chat_id="u1",
                             chat_type=CHAT_PRIVATE,
                             sender_id="u1",
                             message_id="m1",
                             text="hi")
    assert await worker.handle("nope", "wecom", inbound) == ""
    assert _has_metric(worker.metrics, "counters", "agent_requests_total", outcome="tenant_unavailable")


async def test_worker_records_runner_failure():
    manager = TenantConfigManager()
    manager.register(Tenant(tenant_id="t_a", name="A", model=ModelEndpoint(model_name="m")))
    metrics = EnterpriseMetrics(meter=False)
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda tenant: FailingAgent(name=tenant.tenant_id),
        session_service_factory=lambda tenant: InMemorySessionService(),
        metrics=metrics,
    )
    inbound = InboundMessage(channel="wecom",
                             chat_id="u1",
                             chat_type=CHAT_PRIVATE,
                             sender_id="u1",
                             message_id="failed",
                             text="fail")

    with pytest.raises(AttributeError, match="has_content"):
        await worker.handle("t_a", "wecom", inbound)

    assert _has_metric(metrics, "histograms", "agent_runner_latency_ms", outcome="error", error_type="AttributeError")
    assert _has_metric(metrics, "counters", "agent_requests_total", outcome="error", error_type="AttributeError")


async def test_worker_records_session_lock_failure():

    class FailingLocks:

        @asynccontextmanager
        async def acquire(self, key):
            raise TimeoutError("lock unavailable")
            yield  # pragma: no cover

    manager = TenantConfigManager()
    manager.register(Tenant(tenant_id="t_a", name="A", model=ModelEndpoint(model_name="m")))
    metrics = EnterpriseMetrics(meter=False)
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda tenant: EchoAgent(name=tenant.tenant_id),
        session_service_factory=lambda tenant: InMemorySessionService(),
        session_lock_manager=FailingLocks(),
        metrics=metrics,
    )
    inbound = InboundMessage(channel="wecom", chat_id="u1", sender_id="u1", message_id="lock", text="hi")

    with pytest.raises(TimeoutError, match="lock unavailable"):
        await worker.handle("t_a", "wecom", inbound)

    assert _has_metric(metrics,
                       "histograms",
                       "agent_session_lock_duration_ms",
                       phase="wait",
                       outcome="error",
                       error_type="TimeoutError")


def test_parse_confirmation_token():
    assert parse_confirmation_token("确认 abcdefghijklmnop") == "abcdefghijklmnop"
    assert parse_confirmation_token("confirm ABCDEFGHIJKL") == "ABCDEFGHIJKL"
    assert parse_confirmation_token("你好") is None
    assert parse_confirmation_token(None) is None


async def test_worker_hitl_confirmation_flow():
    manager = TenantConfigManager()
    manager.register(Tenant(tenant_id="t_a", name="A", model=ModelEndpoint(model_name="m")))
    shared = InMemorySessionService()
    cm = ConfirmationManager()
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda t: EchoAgent(name=t.tenant_id),
        session_service_factory=lambda t: shared,
        confirmation_manager=cm,
    )

    pending = cm.request("t_a", "cancel_order", {"order_id": 1})
    inbound = InboundMessage(channel="wecom",
                             chat_id="u1",
                             chat_type=CHAT_PRIVATE,
                             sender_id="u1",
                             message_id="m2",
                             text=f"确认 {pending.token}")
    reply = await worker.handle("t_a", "wecom", inbound)
    assert "cancel_order" in reply

    session_id = generate_session_id("t_a", "wecom", CHAT_PRIVATE, "u1", "u1")
    session = await shared.get_session(app_name="t_a:default", user_id="u1", session_id=session_id)
    assert session is not None
    assert "cancel_order" in session.state.get("confirmed_tools", {}).get("u1", [])


async def test_group_hitl_grant_is_sender_bound_and_one_shot():
    manager = TenantConfigManager()
    manager.register(Tenant(tenant_id="t_a", name="A", model=ModelEndpoint(model_name="m")))
    shared = InMemorySessionService()
    confirmations = ConfirmationManager()
    captured: list[list[str]] = []
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda tenant: ConfirmationCaptureAgent(tenant.tenant_id, captured),
        session_service_factory=lambda tenant: shared,
        confirmation_manager=confirmations,
    )
    session_id = generate_session_id("t_a", "wecom", CHAT_GROUP, "u1", "group-1")
    pending = confirmations.request(
        "t_a",
        "cancel_order",
        user_id="u1",
        session_id=session_id,
    )

    def inbound(sender_id: str, message_id: str, text: str) -> InboundMessage:
        return InboundMessage(
            channel="wecom",
            chat_id="group-1",
            chat_type=CHAT_GROUP,
            sender_id=sender_id,
            message_id=message_id,
            text=text,
        )

    await worker.handle("t_a", "wecom", inbound("u1", "confirm", f"确认 {pending.token}"))
    await worker.handle("t_a", "wecom", inbound("u2", "other", "do it"))
    assert captured[-1] == []

    session = await shared.get_session(
        app_name="t_a:default",
        user_id="group:group-1",
        session_id=session_id,
    )
    assert session.state["confirmed_tools"]["u1"] == ["cancel_order"]

    await worker.handle("t_a", "wecom", inbound("u1", "owner", "do it"))
    assert captured[-1] == ["cancel_order"]
    session = await shared.get_session(
        app_name="t_a:default",
        user_id="group:group-1",
        session_id=session_id,
    )
    assert "u1" not in session.state["confirmed_tools"]

    await worker.handle("t_a", "wecom", inbound("u1", "again", "do it again"))
    assert captured[-1] == []


async def test_worker_hitl_invalid_token():
    manager = TenantConfigManager()
    manager.register(Tenant(tenant_id="t_a", name="A", model=ModelEndpoint(model_name="m")))
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda t: EchoAgent(name=t.tenant_id),
        session_service_factory=lambda t: InMemorySessionService(),
        confirmation_manager=ConfirmationManager(),
    )
    inbound = InboundMessage(channel="wecom",
                             chat_id="u1",
                             chat_type=CHAT_PRIVATE,
                             sender_id="u1",
                             message_id="m3",
                             text="确认 invalidtoken123")
    reply = await worker.handle("t_a", "wecom", inbound)
    assert "无效" in reply or "过期" in reply
