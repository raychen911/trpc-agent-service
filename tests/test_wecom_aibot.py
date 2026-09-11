from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import Any

from trpc_service.channels import (
    CallbackRequest,
    ChannelBinding,
    ChannelError,
    WeComAIBotConnection,
)
from trpc_service.config import AppSettings
from trpc_service.web.app import ServiceContainer
from trpc_service.web.schemas import AgentCreate, ChannelCreate, ReleaseCreate, TenantCreate


class FakeAIBotSocket:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[str | bytes | BaseException] = asyncio.Queue()
        self.sent: list[dict[str, Any]] = []
        self.closed = False

    async def send(self, message: str) -> None:
        self.sent.append(json.loads(message))

    async def recv(self) -> str | bytes:
        next_value = await self.incoming.get()
        if isinstance(next_value, BaseException):
            raise next_value
        return next_value

    async def close(self) -> None:
        self.closed = True
        self.incoming.put_nowait(ConnectionError("socket closed"))


async def _until(predicate, *, timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition was not reached before timeout")


def _bootstrap(services: ServiceContainer) -> ChannelBinding:
    tenant_id = "tenant-aibot"
    services.create_tenant(TenantCreate(tenant_id=tenant_id, display_name="Smart Bot tenant"))
    services.create_agent(tenant_id, AgentCreate(agent_id="support", name="Support"))
    services.create_release(
        tenant_id,
        "support",
        ReleaseCreate(version=1, model_config={"mode": "mock"}),
    )
    services.activate_release(tenant_id, "support", 1)
    created = services.create_binding(
        tenant_id,
        ChannelCreate(
            binding_id="wecom-smart-bot",
            agent_id="support",
            provider="wecom_aibot",
            external_account_id="bot-123",
            secret_ref="env://TRPC_LIVE_WECOM_AIBOT_SECRET",
        ),
    )
    return services.adapter_binding(services.control.binding(tenant_id, created["binding_id"]))


def _message_frame(request_id: str) -> dict[str, Any]:
    return {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": request_id},
        "body": {
            "msgid": "message-001",
            "aibotid": "bot-123",
            "chattype": "single",
            "from": {"userid": "alice"},
            "msgtype": "text",
            "text": {"content": "hello Smart Bot"},
            "create_time": 1_700_000_000,
        },
    }


def test_wecom_aibot_long_connection_uses_durable_pipeline_and_same_socket_reply() -> None:
    async def scenario() -> None:
        services = ServiceContainer(AppSettings(admin_api_key=None))
        binding = _bootstrap(services)
        services.secrets.put(
            "env://TRPC_LIVE_WECOM_AIBOT_SECRET",
            '{"bot_id":"bot-123","secret":"test-only-secret"}',
        )
        socket = FakeAIBotSocket()

        async def connector(_: str) -> FakeAIBotSocket:
            return socket

        connection = WeComAIBotConnection(
            binding=binding,
            secrets_provider=services.secrets,
            registry=services.aibot_registry,
            on_inbound=services.accept_aibot_frame,
            connector=connector,
            heartbeat_seconds=60,
            reply_ack_seconds=0.25,
        )
        run = asyncio.create_task(connection.run())
        await _until(lambda: len(socket.sent) == 1)
        auth_request_id = socket.sent[0]["headers"]["req_id"]
        assert socket.sent[0]["cmd"] == "aibot_subscribe"
        assert socket.sent[0]["body"] == {"bot_id": "bot-123", "secret": "test-only-secret"}
        await socket.incoming.put(json.dumps({"headers": {"req_id": auth_request_id}, "errcode": 0}))
        await _until(lambda: connection.authenticated)

        await socket.incoming.put(json.dumps(_message_frame("callback-request-1")))
        # The duplicate has a new transport request ID but the same provider
        # message ID. Inbox dedupe keeps the first request ID for the reply.
        await socket.incoming.put(json.dumps(_message_frame("callback-request-duplicate")))
        await _until(
            lambda: len(services.runtime.snapshot(_context())["inboxes"]) == 1
        )
        inbox = services.runtime.snapshot(_context())["inboxes"][0]
        assert inbox["payload"]["channel_context"]["wecom_aibot_request_id"] == "callback-request-1"

        services.dispatch("tenant-aibot")
        await services.process_published("tenant-aibot")
        services.dispatch("tenant-aibot")
        skipped = await services.deliver_replies("tenant-aibot")
        assert skipped[0]["status"] == "aibot_gateway_required"
        delivery = asyncio.create_task(
            services.deliver_replies("tenant-aibot", provider_filter="wecom_aibot")
        )
        await _until(lambda: any(frame["cmd"] == "aibot_respond_msg" for frame in socket.sent))
        reply = next(frame for frame in socket.sent if frame["cmd"] == "aibot_respond_msg")
        assert reply["headers"]["req_id"] == "callback-request-1"
        assert reply["body"]["msgtype"] == "stream"
        assert reply["body"]["stream"]["finish"] is True
        await socket.incoming.put(
            json.dumps(
                {
                    "headers": {"req_id": "callback-request-1"},
                    "errcode": 0,
                    "body": {"msgid": "reply-001"},
                }
            )
        )
        outcomes = await delivery
        assert outcomes[0]["status"] == "accepted"
        outbox = services.runtime.snapshot(_context())["outbox"]
        assert outbox[-1]["status"] == "delivered"

        await connection.stop()
        await asyncio.wait_for(run, timeout=1)

    def _context():
        from trpc_service.runtime import TenantContext

        return TenantContext("tenant-aibot", actor_id="test")

    asyncio.run(scenario())


def test_wecom_aibot_rejects_a_message_for_a_different_bot() -> None:
    async def scenario() -> None:
        services = ServiceContainer(AppSettings(admin_api_key=None))
        binding = _bootstrap(services)
        adapter = services.adapters["wecom_aibot"]
        frame: Mapping[str, Any] = _message_frame("callback-request")
        altered = {**frame, "body": {**frame["body"], "aibotid": "different-bot"}}
        try:
            await adapter.validate_and_normalize(
                binding,
                CallbackRequest(body=altered, headers={"x-wecom-aibot-request-id": "callback-request"}),
            )
        except ChannelError as exc:
            assert exc.code == "aibot_binding_mismatch"
        else:
            raise AssertionError("mismatched Smart Bot ID was accepted")

    asyncio.run(scenario())
