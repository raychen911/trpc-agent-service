from __future__ import annotations

import asyncio
import os
import unittest
from unittest.mock import patch

from trpc_service.agent import AgentService
from trpc_service.channels import ChannelDispatcher, WeComAdapter
from trpc_service.config import ServiceSettings
from trpc_service.tenant import AgentApp, ChannelBinding, TenantConfig, TenantRegistry
from trpc_service.web.app import ServiceRuntime


class FakeBotClient:
    def __init__(self) -> None:
        self.replies = []

    async def reply_stream(self, frame, stream_id, text, finish):
        self.replies.append((frame, stream_id, text, finish))
        return {"errcode": 0}


class WeComLongConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_long_connection_message_enters_agent_and_replies(self) -> None:
        adapter = WeComAdapter(dry_run=False)
        binding = ChannelBinding("wecom", "bot-123", secret_ref="env://WECOM_BOT_SECRET")
        config = TenantConfig("acme", "Acme", {"default": AgentApp("default", "assistant")}, {"wecom": binding})
        registry = TenantRegistry([config])
        service = AgentService(registry, ChannelDispatcher({"wecom": adapter}))
        runtime = ServiceRuntime(ServiceSettings(), registry, service)
        client = FakeBotClient()
        connection = type("Connection", (), {"client": client})()
        frame = {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-1",
                "aibotid": "bot-123",
                "chattype": "single",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hello"},
            },
        }
        with patch.dict(os.environ, {"WECOM_BOT_SECRET": "secret"}):
            accepted = await runtime.ingest_wecom_long_connection("acme", frame, connection)
        self.assertTrue(accepted)
        stop = asyncio.Event()
        worker = asyncio.create_task(service.worker_loop(stop))
        for _ in range(50):
            if client.replies:
                break
            await asyncio.sleep(0.01)
        stop.set()
        await worker
        self.assertEqual("已收到：hello", client.replies[0][2])
        self.assertEqual("req-1", client.replies[0][0]["headers"]["req_id"])
