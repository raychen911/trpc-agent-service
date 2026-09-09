"""Protocol and visual IM demo checks; all transports and the model are local fakes."""

import json
import os
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from trpc_service.channels.base import ChannelAuthenticationError, UnsupportedMessageError
from trpc_service.channels.simulator import (ImFaultRequest, ImSimulationAttachment, ImSimulationRequest,
                                             build_im_demo_container)
from trpc_service.channels.telegram import TelegramChannelAdapter
from trpc_service.channels.wecom import WeComChannelAdapter
from trpc_service.config import ChannelType, ServiceSettings, load_tenant_configs
from trpc_service.gateway.models import OutboundMessage
from trpc_service.gateway.models import Attachment
from trpc_service.resources import InMemoryArtifactStore
from trpc_service.web import build_container, create_app
from trpc_service import _cli as cli_module

pytestmark = pytest.mark.component
ROOT = Path(__file__).parents[1]


def test_unified_im_live_cli_loads_env_and_keeps_channel_order(tmp_path, monkeypatch):
    monkeypatch.delenv("TRPC_TEST_IM_LIVE_ENV", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("TRPC_TEST_IM_LIVE_ENV=loaded-from-file\n", encoding="utf-8")
    calls = []

    async def fake_run_demo(scenario):
        assert os.environ["TRPC_TEST_IM_LIVE_ENV"] == "loaded-from-file"
        calls.append(scenario)
        if scenario == "wecom-live":
            return {"scenario": scenario, "authenticated": True}
        return {"scenario": scenario, "delivered": True}

    monkeypatch.setattr(cli_module, "run_demo", fake_run_demo)
    runner = CliRunner()
    refused = runner.invoke(cli_module.cli, ["demo", "im-live", "--channels", "wecom"])
    assert refused.exit_code != 0 and calls == []
    completed = runner.invoke(cli_module.cli, [
        "demo",
        "im-live",
        "--env-file",
        str(env_file),
        "--channels",
        "telegram,wecom",
        "--confirm",
        "--json",
    ])
    assert completed.exit_code == 0, completed.output
    result = json.loads(completed.output)
    assert calls == ["wecom-live", "telegram-live"]
    assert result["passed"] == 2 and result["failed"] == 0
    assert [item["channel"] for item in result["results"]] == ["wecom", "telegram"]


def wecom_frame(msgtype="text", content=None):
    return {
        "cmd": "aibot_msg_callback",
        "headers": {
            "req_id": "req-1",
            "future_field": "ignored"
        },
        "body": {
            "msgid": "msg-1",
            "aibotid": "bot-1",
            "chattype": "group",
            "chatid": "chat-1",
            "from": {
                "userid": "user-1"
            },
            "msgtype": msgtype,
            msgtype: content or {
                "content": "hello"
            },
            "future_field": {
                "ignored": True
            },
        },
    }


@pytest.mark.asyncio
async def test_wecom_real_frame_shapes_and_binding_authentication():
    adapter = WeComChannelAdapter(expected_bot_id="bot-1")
    text = await adapter.normalize("binding", wecom_frame(), {})
    assert (text.external_user_id, text.external_conversation_id, text.is_group) == ("user-1", "chat-1", True)
    voice = await adapter.normalize("binding", wecom_frame("voice", {"content": "voice text"}), {})
    assert voice.text == "voice text"
    mixed = await adapter.normalize(
        "binding",
        wecom_frame(
            "mixed", {
                "msg_item": [{
                    "msgtype": "text",
                    "text": {
                        "content": "A"
                    }
                }, {
                    "msgtype": "image",
                    "image": {
                        "url": "https://example.invalid/a",
                        "aeskey": "k"
                    }
                }]
            }), {})
    assert mixed.text == "A" and len(mixed.attachments) == 1
    wrong = wecom_frame()
    wrong["body"]["aibotid"] = "another-bot"
    with pytest.raises(ChannelAuthenticationError):
        await adapter.normalize("binding", wrong, {})
    missing = wecom_frame()
    missing["headers"].pop("req_id")
    with pytest.raises(UnsupportedMessageError):
        await adapter.normalize("binding", missing, {})
    invalid_media = wecom_frame("file", {"url": "https://example.invalid/file"})
    with pytest.raises(UnsupportedMessageError):
        await adapter.normalize("binding", invalid_media, {})


@pytest.mark.asyncio
async def test_telegram_http_200_ok_false_is_not_delivered():

    async def handler(request):
        return httpx.Response(200, json={"ok": False, "error_code": 400, "description": "Bad Request"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = TelegramChannelAdapter("token", "secret", client)
    result = await adapter.deliver(
        OutboundMessage(outbound_id="o",
                        request_id="r",
                        tenant_id="t",
                        binding_id="b",
                        channel="telegram",
                        external_conversation_id="1",
                        text="hello"))
    assert not result.delivered and not result.retryable and result.error_code == "telegram_api_400"
    await client.aclose()


@pytest.mark.asyncio
async def test_telegram_outbound_image_uses_send_photo():
    paths = []

    async def handler(request):
        paths.append(request.url.path)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 9}})

    store = InMemoryArtifactStore()
    metadata = await store.put("tenant", "app", "photo.png", "image/png", b"png")
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = TelegramChannelAdapter("token", "secret", client, artifacts=store)
    result = await adapter.deliver(
        OutboundMessage(outbound_id="o",
                        request_id="r",
                        tenant_id="tenant",
                        binding_id="b",
                        channel="telegram",
                        external_conversation_id="1",
                        text="",
                        attachments=[
                            Attachment(attachment_id=metadata.artifact_id,
                                       kind="image",
                                       name="photo.png",
                                       mime_type="image/png")
                        ]))
    assert result.delivered and paths[-1].endswith("sendPhoto")
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", [ChannelType.WECOM, ChannelType.WECOM_KF, ChannelType.TELEGRAM])
async def test_each_visual_channel_uses_adapter_queue_runner_outbox_and_fake_delivery(channel):
    settings = ServiceSettings(environment="development")
    configs = load_tenant_configs(ROOT / "examples/config/im-demo.yaml")
    container = await build_im_demo_container(settings, configs)
    try:
        submitted = await container.im_simulator.send(
            ImSimulationRequest(channel=channel,
                                external_user_id="10001",
                                external_conversation_id="20001",
                                duplicate_count=2,
                                text="hello"))
        assert submitted["duplicate_reused"] is True
        assert await container.task_processor.process_one(timeout_seconds=.01)
        assert await container.delivery_worker.deliver_due(limit=10) == 1
        status = await container.im_simulator.status(submitted["request_id"])
        assert status["request"]["state"] == "succeeded"
        assert status["outbox"]["state"] == "delivered"
        assert status["delivered"] is True
        assert status["request"]["request"]["trace"]["traceparent"].startswith("00-")
    finally:
        await container.close()


def test_visual_routes_exist_only_on_explicit_im_demo():
    configs = load_tenant_configs(ROOT / "examples/config/im-demo.yaml")
    normal_configs = load_tenant_configs(ROOT / "examples/config/tenants.yaml")
    normal = build_container(ServiceSettings(environment="development"), normal_configs)
    with TestClient(create_app(normal)) as client:
        assert client.get("/im").status_code == 404

    async def build():
        return await build_im_demo_container(ServiceSettings(environment="development"), configs)

    import asyncio
    demo = asyncio.run(build())
    with TestClient(create_app(demo)) as client:
        page = client.get("/im")
        assert page.status_code == 200
        assert "IM 接入本地验证台" in page.text
        assert 'type="file" multiple' in page.text
        assert "取消全部附件" in page.text
        assert [item["value"] for item in client.get("/api/v1/dev/im/bootstrap").json()["channels"]
                ] == ["wecom", "wecom_kf", "telegram"]
        assert list(demo.channel_adapters) == [
            "demo-wecom-configured", "demo-wecom-offline", "demo-wecom-kf-configured", "demo-wecom-kf-offline",
            "demo-telegram-configured", "demo-telegram-offline"
        ]
        accepted = client.post("/api/v1/dev/im/messages",
                               json={
                                   "channel": "wecom",
                                   "external_user_id": "10001",
                                   "external_conversation_id": "20001",
                                   "text": "browser flow"
                               })
        assert accepted.status_code == 202
        assert accepted.json()["duplicate_reused"] is False
        import time
        for _ in range(50):
            completed = client.get(f"/api/v1/dev/im/messages/{accepted.json()['request_id']}").json()
            if completed["delivered"]:
                break
            time.sleep(.05)
        assert completed["request"]["state"] == "succeeded" and completed["delivered"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", [ChannelType.WECOM, ChannelType.WECOM_KF, ChannelType.TELEGRAM])
async def test_visual_channel_attachment_is_downloaded_before_queue(channel):
    import base64
    configs = load_tenant_configs(ROOT / "examples/config/im-demo.yaml")
    container = await build_im_demo_container(ServiceSettings(environment="development"), configs)
    try:
        png = base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")
        submitted = await container.im_simulator.send(
            ImSimulationRequest(channel=channel,
                                external_user_id="10001",
                                external_conversation_id="20001",
                                text="attachment",
                                duplicate_count=2,
                                attachments=[
                                    ImSimulationAttachment(name="note.txt",
                                                           mime_type="text/plain",
                                                           content_base64=base64.b64encode(b"note").decode()),
                                    ImSimulationAttachment(name="sample.png",
                                                           mime_type="image/png",
                                                           content_base64=base64.b64encode(png).decode()),
                                ]))
        status = await container.im_simulator.status(submitted["request_id"])
        assert submitted["duplicate_reused"] is True
        assert submitted["batch_size"] == 3
        attachments = status["request"]["request"]["attachments"]
        assert len(attachments) == 2
        assert all(attachment["object_uri"].startswith("memory://") for attachment in attachments)
        assert all(attachment["source_url"] == "" for attachment in attachments)
        if channel == ChannelType.WECOM:
            raw = status["protocol"]["raw"]
            assert raw[0]["body"]["text"]["content"] == "attachment"
            assert raw[2]["body"]["image"]["aeskey"] == "***"
            binding = container.im_simulator.bindings[(channel, "fake")]
            assert container.im_simulator.clients[binding.binding_id].downloads[0][1] == "simulated-aes-key"
        assert await container.task_processor.process_one(timeout_seconds=.01)
        assert await container.delivery_worker.deliver_due(limit=1) == 1
        status = await container.im_simulator.status(submitted["request_id"])
        assert status["request"]["state"] == "succeeded"
        assert status["outbox"]["state"] == "delivered"
    finally:
        await container.close()


@pytest.mark.asyncio
async def test_visual_faults_reach_outbox_state_machine():
    configs = load_tenant_configs(ROOT / "examples/config/im-demo.yaml")
    container = await build_im_demo_container(ServiceSettings(environment="development"), configs)
    try:
        container.im_simulator.set_fault(ImFaultRequest(channel="telegram", fault="delivery_timeout"))
        submitted = await container.im_simulator.send(ImSimulationRequest(channel="telegram", text="hello"))
        await container.task_processor.process_one(timeout_seconds=.01)
        assert await container.delivery_worker.deliver_due(limit=1) == 0
        status = await container.im_simulator.status(submitted["request_id"])
        assert status["outbox"]["state"] == "unknown"

        container.im_simulator.set_fault(ImFaultRequest(channel="wecom_kf", fault="human_handoff"))
        submitted = await container.im_simulator.send(ImSimulationRequest(channel="wecom_kf", text="hello"))
        await container.task_processor.process_one(timeout_seconds=.01)
        assert await container.delivery_worker.deliver_due(limit=1) == 0
        status = await container.im_simulator.status(submitted["request_id"])
        assert status["outbox"]["state"] == "dead"
        assert status["outbox"]["last_error"] == "kf_human_or_closed"
    finally:
        await container.close()


@pytest.mark.asyncio
async def test_visual_demo_enables_its_local_worker_and_delivery_roles():
    from trpc_service.config import ServiceRole, ServiceSettings
    from trpc_service.channels.simulator import build_im_demo_container

    configs = load_tenant_configs(ROOT / "examples/config/im-demo.yaml")
    settings = ServiceSettings(environment="development", roles={ServiceRole.GATEWAY})
    container = await build_im_demo_container(settings, configs)
    try:
        assert {ServiceRole.GATEWAY, ServiceRole.WORKER, ServiceRole.DELIVERY} <= settings.roles
        assert container.task_processor is not None
        assert container.delivery_worker is not None
    finally:
        await container.close()
