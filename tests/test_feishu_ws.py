from __future__ import annotations

import asyncio
import json
import os
import unittest
from collections.abc import Mapping
from typing import Any
from unittest.mock import patch

from fastapi.testclient import TestClient

from trpc_service.agent import AgentService
from trpc_service.channels import ChannelDispatcher, FeishuAdapter, FeishuLongConnection, make_session_id
from trpc_service.channels.feishu import HTTPResponse
from trpc_service.config import ServiceSettings
from trpc_service.tenant import AgentApp, ChannelBinding, TenantConfig, TenantRegistry
from trpc_service.web.app import ServiceRuntime, build_demo_runtime, create_app

APP_ID = "cli_ws_test"
APP_SECRET = "ws-app-secret"


def _event() -> dict[str, Any]:
    return {
        "schema": "2.0",
        "header": {
            "event_id": "ws-event-1",
            "event_type": "im.message.receive_v1",
            "app_id": APP_ID,
        },
        "event": {
            "sender": {"sender_id": {"open_id": "ou_ws_user"}, "sender_type": "user"},
            "message": {
                "message_id": "om_ws_message",
                "chat_id": "oc_ws_chat",
                "chat_type": "p2p",
                "message_type": "text",
                "content": json.dumps({"text": "长连接消息"}, ensure_ascii=False),
            },
        },
    }


class FakeHTTPClient:
    async def post(self, url: str, *, headers: Mapping[str, str], payload: Mapping[str, Any]) -> HTTPResponse:
        if url.endswith("/open-apis/auth/v3/tenant_access_token/internal"):
            body = {"code": 0, "tenant_access_token": "tenant-token", "expire": 7200}
        else:
            body = {"code": 0, "data": {"message_id": "reply-message"}}
        return HTTPResponse(200, {}, json.dumps(body).encode())


def _runtime() -> ServiceRuntime:
    binding = ChannelBinding(
        channel="feishu",
        account_id=APP_ID,
        secret_ref=f"literal://{APP_SECRET}",
        api_base_url="https://feishu.test",
    )
    config = TenantConfig(
        tenant_id="acme",
        name="Acme",
        apps={"default": AgentApp("default", "assistant")},
        channels={"feishu": binding},
    )
    registry = TenantRegistry([config])
    adapter = FeishuAdapter(FakeHTTPClient())
    dispatcher = ChannelDispatcher(
        {"feishu": adapter},
        binding_resolver=lambda tenant, channel, account: registry.resolve_binding(tenant, channel, account)[1],
    )
    return ServiceRuntime(ServiceSettings(), registry, AgentService(registry, dispatcher))


class FeishuLongConnectionTest(unittest.IsolatedAsyncioTestCase):
    async def test_event_uses_existing_agent_pipeline_without_webhook_secrets(self) -> None:
        runtime = _runtime()

        self.assertTrue(await runtime.ingest_feishu_long_connection("acme", _event()))
        self.assertFalse(await runtime.ingest_feishu_long_connection("acme", _event()))
        await runtime.service.process_one()

        session_id = make_session_id("acme", "feishu", "oc_ws_chat", "direct", runtime.settings.session_hmac_key)
        self.assertEqual(2, len(await runtime.service.sessions.events("acme", session_id)))
        self.assertEqual(1, len(runtime.service.dispatcher.deliveries))

    async def test_worker_does_not_cancel_a_slow_agent_turn(self) -> None:
        completed = asyncio.Event()

        class SlowExecutor:
            async def reply(self, message: Any, memory: list[str], instruction: str) -> str:
                await asyncio.sleep(0.35)
                completed.set()
                return "慢请求已完成"

        runtime = _runtime()
        runtime.service.executor = SlowExecutor()
        await runtime.ingest_feishu_long_connection("acme", _event())
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.service.worker_loop(stop))
        try:
            await asyncio.wait_for(completed.wait(), timeout=1)
            await asyncio.wait_for(runtime.service.queue.join(), timeout=1)
        finally:
            stop.set()
            await worker

        self.assertEqual("慢请求已完成", runtime.service.dispatcher.deliveries[0].text)
        failed_turns = sum(
            value for (name, _), value in runtime.service.metrics.counters.items() if name == "agent_turn_failed"
        )
        self.assertEqual(0, failed_turns)

    async def test_sdk_callback_bridges_from_sdk_thread_to_asyncio(self) -> None:
        received: list[tuple[str, Mapping[str, Any]]] = []
        processed = asyncio.Event()

        async def receive(tenant_id: str, payload: Mapping[str, Any]) -> bool:
            received.append((tenant_id, payload))
            processed.set()
            return True

        connection = FeishuLongConnection(APP_ID, APP_SECRET, "acme", receive)
        connection._asyncio_loop = asyncio.get_running_loop()
        connection._start_event_consumer(connection._asyncio_loop)
        with patch("lark_oapi.JSON.marshal", return_value=json.dumps(_event(), ensure_ascii=False)):
            await asyncio.to_thread(connection._handle_sdk_event, object())
        await asyncio.wait_for(processed.wait(), timeout=1)
        await connection._stop_event_consumer()

        self.assertEqual("acme", received[0][0])
        self.assertEqual("om_ws_message", received[0][1]["event"]["message"]["message_id"])

    async def test_official_sdk_deserializes_and_dispatches_message_event(self) -> None:
        received: list[Mapping[str, Any]] = []
        processed = asyncio.Event()

        async def receive(_: str, payload: Mapping[str, Any]) -> bool:
            received.append(payload)
            processed.set()
            return True

        connection = FeishuLongConnection(APP_ID, APP_SECRET, "acme", receive)
        connection._asyncio_loop = asyncio.get_running_loop()
        connection._start_event_consumer(connection._asyncio_loop)
        client = connection._build_client()
        body = json.dumps(_event(), ensure_ascii=False).encode()

        await asyncio.to_thread(client._event_handler._do_without_validation, body)
        await asyncio.wait_for(processed.wait(), timeout=1)
        await connection._stop_event_consumer()

        self.assertEqual("im.message.receive_v1", received[0]["header"]["event_type"])
        self.assertEqual("长连接消息", json.loads(received[0]["event"]["message"]["content"])["text"])

    async def test_sdk_callback_acks_without_waiting_for_async_consumer(self) -> None:
        release = asyncio.Event()
        processing_started = asyncio.Event()

        async def slow_receive(_: str, __: Mapping[str, Any]) -> bool:
            processing_started.set()
            await release.wait()
            return True

        connection = FeishuLongConnection(APP_ID, APP_SECRET, "acme", slow_receive)
        connection._asyncio_loop = asyncio.get_running_loop()
        connection._start_event_consumer(connection._asyncio_loop)
        with patch("lark_oapi.JSON.marshal", return_value=json.dumps(_event(), ensure_ascii=False)):
            await asyncio.wait_for(asyncio.to_thread(connection._handle_sdk_event, object()), timeout=0.2)

        await asyncio.wait_for(processing_started.wait(), timeout=1)
        release.set()
        await connection._stop_event_consumer()

    async def test_lifespan_starts_and_stops_connections(self) -> None:
        class FakeConnection:
            def __init__(self) -> None:
                self.started = False
                self.stopped = False

            @property
            def status(self) -> dict[str, str]:
                return {"state": "connected"}

            def start(self, _: asyncio.AbstractEventLoop) -> None:
                self.started = True

            def stop(self) -> None:
                self.stopped = True

        runtime = _runtime()
        connection = FakeConnection()
        runtime.feishu_connections.append(connection)  # type: ignore[arg-type]
        with TestClient(create_app(runtime)) as client:
            self.assertTrue(connection.started)
            response = client.get("/health/ready")
            self.assertEqual("connected", response.json()["feishu_connections"][0]["state"])
        self.assertTrue(connection.stopped)

    async def test_websocket_mode_does_not_require_verification_token(self) -> None:
        environment = {
            "TRPC_SERVICE_FEISHU_CONNECTION_MODE": "websocket",
            "TRPC_SERVICE_FEISHU_TENANT_ID": "acme",
            "TRPC_SERVICE_FEISHU_APP_ID": APP_ID,
            "TRPC_SERVICE_FEISHU_APP_SECRET_REF": f"literal://{APP_SECRET}",
        }
        with patch.dict(os.environ, environment, clear=True):
            runtime = build_demo_runtime(ServiceSettings())

        self.assertEqual(1, len(runtime.feishu_connections))
        self.assertIsNone(runtime.registry.get("acme").channels["feishu"].verify_token)

    async def test_websocket_mode_requires_app_id(self) -> None:
        with (
            patch.dict(os.environ, {"TRPC_SERVICE_FEISHU_CONNECTION_MODE": "websocket"}, clear=True),
            self.assertRaisesRegex(ValueError, "APP_ID"),
        ):
            build_demo_runtime(ServiceSettings())


if __name__ == "__main__":
    unittest.main()
