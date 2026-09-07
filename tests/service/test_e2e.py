# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""End-to-end HTTP test: gateway + real worker + agent via a fake channel."""

from __future__ import annotations

import json

from fastapi.testclient import TestClient

from trpc_agent_sdk.agents import BaseAgent
from trpc_service import ChannelRegistry
from trpc_service import InboundMessage
from trpc_service import TenantConfigManager
from trpc_service import TenantWorker
from trpc_service import create_gateway_app
from trpc_service.channels import ChannelAdapter
from trpc_service.channels import SendResult
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import Tenant
from trpc_service.tenant import WeComChannelConfig
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part


class EchoAgent(BaseAgent):

    def __init__(self, name: str) -> None:
        super().__init__(name=name)

    async def _run_async_impl(self, ctx):
        user_text = "".join(p.text or "" for p in (ctx.user_content.parts if ctx.user_content else []))
        yield Event(author=self.name, content=Content(parts=[Part.from_text(text=f"echo:{user_text}")]), partial=False)


class FakeAdapter(ChannelAdapter):
    channel = "fake"

    def __init__(self, cfg=None):
        self.replies: list[str] = []

    async def verify_signature(self, payload, headers, query):
        return True

    async def parse_message(self, payload):
        msg = payload if isinstance(payload, dict) else json.loads(payload)
        return InboundMessage(channel=self.channel,
                              chat_id="c1",
                              chat_type="private",
                              sender_id="u1",
                              message_id=msg.get("message_id", "m1"),
                              text=msg.get("text", ""))

    async def send_message(self, outbound):
        return SendResult(ok=True)

    async def send_stream(self, chat_id, stream):
        return SendResult(ok=True)

    async def reply_text(self, inbound, text):
        self.replies.append(text)
        return SendResult(ok=True)


def test_end_to_end_gateway_worker_agent():
    manager = TenantConfigManager()
    tenant = Tenant(tenant_id="t_a", name="A", model=ModelEndpoint(model_name="m"))
    tenant.channel_configs["fake"] = WeComChannelConfig(
        token="token",
        aes_key="aes",
        corp_id="corp",
        agent_id="1",
    )
    manager.register(tenant)

    shared = InMemorySessionService()
    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda t: EchoAgent(name="t_a"),
        session_service_factory=lambda t: shared,
    )

    adapter = FakeAdapter()
    registry = ChannelRegistry(factories={"fake": lambda cfg: adapter})
    app = create_gateway_app(manager=manager, worker=worker, registry=registry, async_dispatch=False)
    client = TestClient(app)

    response = client.post("/webhook/t_a/fake", json={"message_id": "m1", "text": "hi"})
    assert response.status_code == 200
    # The real worker ran the agent and the adapter received the echo reply.
    assert adapter.replies == ["echo:hi"]
