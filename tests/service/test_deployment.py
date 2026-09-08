# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tests for deployment wiring, especially the shared session backend.

These guard the regression where ``create_session_service`` returned a fresh
backend per call, breaking session continuity across turns.
"""

from __future__ import annotations

import importlib
from trpc_agent_sdk.agents import BaseAgent
from trpc_service import InboundMessage
from trpc_service import TenantConfigManager
from trpc_service import TenantWorker
from trpc_service import generate_session_id
from trpc_service.channels import CHAT_PRIVATE
from trpc_service.web.app import create_session_service
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import Tenant
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_service.workspace import TenantStorageRouter
import trpc_service.workspace._router as router_module

deployment_app = importlib.import_module("trpc_service.web.app")


def _fake_redis_storage(monkeypatch):
    shared = InMemorySessionService()
    monkeypatch.setenv("TRPC_SERVICE_REDIS_URL", "redis://test/0")
    monkeypatch.setattr(router_module, "_redis_session_builder", lambda _url: shared)
    monkeypatch.setattr(deployment_app, "_STORAGE_ROUTER", TenantStorageRouter(redis_url="redis://test/0"))
    return shared


class EchoAgent(BaseAgent):

    def __init__(self, name: str) -> None:
        super().__init__(name=name)

    async def _run_async_impl(self, ctx):
        user_text = "".join(p.text or "" for p in (ctx.user_content.parts if ctx.user_content else []))
        yield Event(author=self.name, content=Content(parts=[Part.from_text(text=f"echo:{user_text}")]), partial=False)


def test_create_session_service_is_shared_singleton(monkeypatch):
    _fake_redis_storage(monkeypatch)
    assert create_session_service(None) is create_session_service(None)


async def test_multiturn_continuity_uses_shared_backend(monkeypatch):
    backend = _fake_redis_storage(monkeypatch)

    manager = TenantConfigManager()
    manager.register(Tenant(tenant_id="t_a", name="A", model=ModelEndpoint(model_name="m")))
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda t: EchoAgent(name=t.tenant_id),
        session_service_factory=create_session_service,
    )

    def inbound(msg_id: str, text: str) -> InboundMessage:
        return InboundMessage(channel="wecom",
                              chat_id="u1",
                              chat_type=CHAT_PRIVATE,
                              sender_id="u1",
                              message_id=msg_id,
                              text=text)

    assert create_session_service(None) is backend
    session_id = generate_session_id("t_a", "wecom", CHAT_PRIVATE, "u1", "u1")

    await worker.handle("t_a", "wecom", inbound("m1", "one"))
    first = await backend.get_session(app_name="t_a:default", user_id="u1", session_id=session_id)
    assert first is not None

    await worker.handle("t_a", "wecom", inbound("m2", "two"))
    second = await backend.get_session(app_name="t_a:default", user_id="u1", session_id=session_id)
    assert second is not None
    # The second turn must see the history of the first turn accumulated.
    assert len(second.events) > len(first.events)
