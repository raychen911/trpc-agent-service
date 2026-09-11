"""Feishu public event schema, HTTP protocol and durable ingress/egress contracts."""

import asyncio
import copy
import json
import logging
import multiprocessing
import threading
from datetime import timedelta

from cryptography.fernet import Fernet
import httpx
import pytest
from sqlalchemy import select

from trpc_service.channels.acceptance import AcceptanceRuntime, test_tenant as build_tenant
from trpc_service.channels.base import CallbackVerificationError
from trpc_service.channels.feishu import (
    FeishuAdapter, LifecycleLogHandler, commit_callback, retry_delay, unsupported_media_reason,
)
from trpc_service.channels.models import AttachmentRef
from trpc_service.persistence.models import InboundMessageRow
from trpc_service.tenant import ChannelBindingRegistry, MessageRouter, SessionIdentityFactory, TenantConfig
from tests.integration.test_tool_acceptance import tool_response


def frame(text="hello", *, group=False, tenant="enterprise", user="ou_user", chat="oc_chat", **message):
    return {
        "schema": "2.0",
        "header": {
            "app_id": "cli_test",
            "tenant_key": tenant,
            "event_id": "event1",
            "event_type": "im.message.receive_v1"
        },
        "event": {
            "sender": {
                "sender_type": "user",
                "tenant_key": tenant,
                "sender_id": {
                    "open_id": user
                }
            },
            "message": {
                "message_id": "om_message",
                "chat_id": chat,
                "chat_type": "group" if group else "p2p",
                "message_type": "text",
                "content": json.dumps({"text": text}),
                **message
            }
        }
    }


class Protocol:

    def __init__(self, result=None):
        self.result, self.sent, self.auth_calls = result, [], 0

    def __call__(self, request):
        assert request.url.host == "open.feishu.cn"
        if request.url.path.endswith("/tenant_access_token/internal"):
            self.auth_calls += 1
            return httpx.Response(200, json={"code": 0, "tenant_access_token": "synthetic-token", "expire": 7200})
        assert request.headers["Authorization"] == "Bearer synthetic-token"
        if request.url.path.endswith("/bot/v3/info"):
            return httpx.Response(200, json={"code": 0, "bot": {"app_id": "cli_test", "open_id": "ou_bot"}})
        if "/resources/" in request.url.path:
            return self.result or httpx.Response(200, content=b"synthetic-image",
                                                 headers={"Content-Type": "image/png"})
        self.sent.append((request.url, json.loads(request.content)))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result or httpx.Response(200, json={"code": 0, "data": {"message_id": "om_reply"}})


@pytest.mark.asyncio
async def test_public_event_schema_identity_mentions_threads_and_controls():
    from lark_oapi.api.im.v1.model.p2_im_message_receive_v1 import P2ImMessageReceiveV1
    import lark_oapi as lark
    adapter = FeishuAdapter("cli_test", "synthetic", transport=httpx.MockTransport(Protocol()))
    try:
        await adapter.initialize()
        binding = build_tenant("feishu", "cli_test").channel_bindings[0]
        wire = json.loads(lark.JSON.marshal(P2ImMessageReceiveV1(frame())))
        event = adapter.normalize(wire, binding)
        assert event.message.text == "hello" and event.external_actor_id == "enterprise:ou_user"
        assert event.message.conversation_type.value == "direct"
        assert adapter.normalize(frame(group=True), binding).reason == "bot_not_mentioned"
        mention = [{"key": "@_user_1", "id": {"open_id": "ou_bot"}}]
        group = adapter.normalize(frame("@_user_1 hello", group=True, mentions=mention, thread_id="omt_topic"), binding)
        assert group.message.text == "hello" and group.message.thread_id == "omt_topic"
        assert group.message.conversation_type.value == "thread"
        assert group.reply_context["reply_in_thread"] is True
        reply = adapter.normalize(frame("@_user_1 hello", group=True, mentions=mention, root_id="om_root"), binding)
        assert reply.message.thread_id == "om_root" and reply.message.conversation_type.value == "thread"
        assert reply.reply_context["reply_in_thread"] is False
        action = adapter.normalize(frame("/action synthetic-nonce"), binding)
        assert action.kind == "action" and action.message is None
        media = adapter.normalize(frame(message_type="image",
                                        content=json.dumps({"image_key": "img_synthetic"})), binding)
        assert media.kind == "chat" and media.pending_media[0].kind == "image"
        group_media = adapter.normalize(frame(group=True, message_type="image",
                                              content=json.dumps({"image_key": "img_synthetic"})), binding)
        assert group_media.kind == "chat" and group_media.message.conversation_type.value == "group"
        payload, filename, mime_type = await adapter.download_media(
            media.pending_media[0], media.reply_context["message_id"])
        assert payload == b"synthetic-image" and filename == "image.bin" and mime_type == "image/png"
        staged = AttachmentRef(artifact_id="artifact://synthetic", filename=filename, mime_type=mime_type,
                               size_bytes=len(payload), sha256="0" * 64)
        normalized = adapter.normalize(frame(message_type="image",
                                             content=json.dumps({"image_key": "img_synthetic"})),
                                       binding, attachments=(staged,))
        assert normalized.message.message_type.value == "image" and normalized.message.attachments == (staged,)
        recall = frame()
        recall["header"].update(event_id="recall_event", event_type="im.message.recalled_v1")
        recall["event"] = {"message_id": "om_message", "chat_id": "oc_chat"}
        recalled = adapter.normalize(recall, binding)
        assert recalled.kind == "recall" and recalled.recalled_message_id == "om_message"
        wrong = frame()
        wrong["header"]["app_id"] = "cli_other"
        with pytest.raises(CallbackVerificationError):
            adapter.normalize(wrong, binding)
        wrong = frame()
        wrong["event"]["sender"]["tenant_key"] = "other"
        with pytest.raises(CallbackVerificationError):
            adapter.normalize(wrong, binding)
    finally:
        await adapter.close()


def test_retry_after_supports_seconds_and_http_dates():
    from datetime import datetime, timezone

    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert retry_delay("7.5", now=now) == 7.5
    assert retry_delay("Thu, 01 Jan 2026 00:00:09 GMT", now=now) == 9
    assert retry_delay("invalid", now=now) == 2


def test_unsupported_media_diagnostic_only_exposes_safe_type_tokens():
    assert unsupported_media_reason("post") == "unsupported_media_post"
    assert unsupported_media_reason("interactive_card") == "unsupported_media_interactive_card"
    assert unsupported_media_reason("unsafe/type") == "unsupported_media"
    assert unsupported_media_reason(None) == "unsupported_media"


@pytest.mark.asyncio
async def test_scopes_isolate_enterprises_groups_users_and_channels():
    adapter = FeishuAdapter("cli_test", "synthetic")
    try:
        config = build_tenant("feishu", "cli_test").model_dump(mode="json")
        feishu = config["channel_bindings"][0]
        config["channel_bindings"].append({
            **feishu, "binding_id": "wecom_binding",
            "channel": "wecom",
            "transport": "wecom_ws",
            "webhook_public_id": "wecom_scope_test"
        })
        tenant = TenantConfig.model_validate(config)
        router = MessageRouter(ChannelBindingRegistry([tenant]), SessionIdentityFactory(lambda _: b"k" * 32))
        binding = tenant.channel_bindings[0]
        scopes = []
        for enterprise, user, chat, group in [("enterprise", "ou_user", "oc_chat", False),
                                              ("other", "ou_user", "oc_chat", False),
                                              ("enterprise", "ou_other", "oc_chat", False),
                                              ("enterprise", "ou_user", "oc_group1", True),
                                              ("enterprise", "ou_user", "oc_group2", True)]:
            adapter.bot_open_id = "ou_bot"
            event = adapter.normalize(
                frame("@_user_1 hello",
                      tenant=enterprise,
                      user=user,
                      chat=chat,
                      group=group,
                      mentions=[{
                          "key": "@_user_1",
                          "id": {
                              "open_id": "ou_bot"
                          }
                      }]), binding)
            scopes.append(router.route_message(event.message).session_id)
        other_channel = event.message.model_copy(update={
            "channel": tenant.channel_bindings[1].channel,
            "webhook_public_id": "wecom_scope_test"
        })
        scopes.append(router.route_message(other_channel).session_id)
        assert len(set(scopes)) == 6
    finally:
        await adapter.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("result,outcome,retry", [
    (None, "accepted", False),
    (httpx.Response(429, headers={"Retry-After": "8"}), "rejected", True),
    (httpx.Response(403, json={
        "code": 999,
        "msg": "secret MUST NOT ESCAPE"
    }), "rejected", False),
    (httpx.Response(503), "unknown", False),
    (httpx.ReadTimeout("secret MUST NOT ESCAPE"), "unknown", False),
    (httpx.Response(200, json={
        "code": 0,
        "data": {}
    }), "unknown", False),
])
async def test_protocol_send_receipts_and_uncertain_outcomes(result, outcome, retry):
    protocol = Protocol(result)
    adapter = FeishuAdapter("cli_test", "synthetic", transport=httpx.MockTransport(protocol))
    try:
        await adapter.initialize()
        binding = build_tenant("feishu", "cli_test").channel_bindings[0]
        event = adapter.normalize(frame(), binding)
        receipt = await adapter.send_text(event.reply_context, "reply", idempotency_key="outbox-1-0")
        assert (receipt.outcome, receipt.retryable) == (outcome, retry)
        assert "secret" not in receipt.model_dump_json()
        if outcome == "accepted":
            assert receipt.external_message_id == "om_reply"
            await adapter.send_text(event.reply_context, "reply", idempotency_key="outbox-1-0")
            assert protocol.sent[0][1]["uuid"] == protocol.sent[1][1]["uuid"]
            assert protocol.auth_calls == 1
        assert protocol.sent[0][1]["receive_id"] == "oc_chat"
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_thread_reply_and_wrong_account_no_network():
    protocol = Protocol()
    adapter = FeishuAdapter("cli_test", "synthetic", transport=httpx.MockTransport(protocol))
    try:
        context = {
            "app_id": "cli_test",
            "tenant_key": "enterprise",
            "chat_id": "oc_chat",
            "message_id": "om_source",
            "thread_id": "omt_thread",
            "reply_in_thread": True
        }
        receipt = await adapter.send_text(context, "reply", idempotency_key="outbox-0")
        assert receipt.outcome == "accepted"
        url, body = protocol.sent[-1]
        assert url.path.endswith("/om_source/reply") and body["reply_in_thread"] is True
        reply_context = {**context, "reply_in_thread": False}
        receipt = await adapter.send_text(reply_context, "reply", idempotency_key="outbox-1")
        assert receipt.outcome == "accepted"
        url, body = protocol.sent[-1]
        assert url.path.endswith("/om_source/reply") and "reply_in_thread" not in body
        rejected = await adapter.send_text({**context, "app_id": "other"}, "reply", idempotency_key="outbox-0")
        assert rejected.outcome == "rejected" and len(protocol.sent) == 2
    finally:
        await adapter.close()


@pytest.mark.parametrize("ack", ["commit", "reject", "wrong_nonce", "timeout"])
def test_callback_waits_for_durable_commit(ack):
    parent, child = multiprocessing.Pipe()
    result = []

    def callback():
        try:
            commit_callback(child, {"event": "synthetic"}, timeout=0.3)
            result.append("acked")
        except RuntimeError:
            result.append("failed")

    thread = threading.Thread(target=callback)
    thread.start()
    try:
        assert parent.poll(1)
        event = parent.recv()
        assert result == []  # No success ACK before the parent's commit acknowledgement.
        if ack != "timeout":
            parent.send({"nonce": "other" if ack == "wrong_nonce" else event["nonce"], "committed": ack == "commit"})
        thread.join(1)
        assert not thread.is_alive()
        assert result == (["acked"] if ack == "commit" else ["failed"])
    finally:
        parent.close()
        child.close()


def test_sdk_log_handler_exports_only_lifecycle_markers():
    events = []

    class Connection:

        def send(self, event):
            events.append(event)

    handler = LifecycleLogHandler(Connection())
    for function, message in [("_connect", "connected to wss://secret"), ("_handle_data_frame", "payload secret"),
                              ("_receive_message_loop", "receive message loop exit, err: secret")]:
        handler.emit(logging.LogRecord("Lark", 20, "test", 1, message, (), None, func=function))
    assert events == [{"kind": "connected"}, {"kind": "failed"}]


@pytest.mark.asyncio
async def test_feishu_real_runner_native_session_dedup_and_delivery(tmp_path):
    tenant = build_tenant("feishu", "cli_test")
    runtime = AcceptanceRuntime(tmp_path / "acceptance", tenant, b"k" * 32, [Fernet.generate_key()])
    protocol = Protocol()
    adapter = FeishuAdapter("cli_test", "synthetic", transport=httpx.MockTransport(protocol))
    try:
        await adapter.initialize()
        await runtime.start("synthetic", 163840, transport=httpx.MockTransport(lambda _: tool_response(2)))
        event = adapter.normalize(frame(), runtime.binding)
        await asyncio.to_thread(runtime.accept_challenge, event, "hello")
        retry = copy.deepcopy(frame())
        retry["header"]["event_id"] = "different_envelope"
        await asyncio.to_thread(runtime.accept_challenge, adapter.normalize(retry, runtime.binding), "hello")
        report = await runtime.execute(event, adapter, "hello")
        assert report["input_statuses"] == ["succeeded"]
        assert report["reply_statuses"] == ["delivered"] and report["native_session_events"] >= 2
        assert len(report["model_attempts"]) == 1 and report["model_attempts"][0]["status"] == "settled"
        assert len(protocol.sent) == 1
        with runtime.database.sessions() as session:
            assert len(list(session.scalars(select(InboundMessageRow)))) == 1
        with pytest.raises(PermissionError):
            await runtime.execute(event, adapter, "hello")
    finally:
        await adapter.close()
        await runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["rate_limit", "unknown", "revoked"])
async def test_feishu_multipart_ledger_recovery_and_permission(tmp_path, failure):
    from trpc_service.channels.delivery import IMDeliveryWorker
    from trpc_service.persistence.models import OutboxMessageRow, IMMembershipRow, utcnow
    runtime = AcceptanceRuntime(tmp_path / "delivery", build_tenant("feishu", "cli_test"), b"k" * 32,
                                [Fernet.generate_key()])
    protocol = Protocol()
    adapter = FeishuAdapter("cli_test", "synthetic", transport=httpx.MockTransport(protocol))
    try:
        await adapter.initialize()
        event = adapter.normalize(frame(), runtime.binding)
        route = runtime.accept_challenge(event, "hello")
        receipt = runtime.ingress.record(event, runtime.binding, runtime.lease, route=route)
        outbox_id = runtime.outbox.enqueue(runtime.binding.tenant_id, receipt.inbound_id, 0, {
            "final": True,
            "text": "中" * 1200
        })
        worker = IMDeliveryWorker(runtime.database, runtime.lease, adapter, runtime.cipher)
        assert await worker.run_once()
        assert len(json.loads(protocol.sent[0][1]["content"])["text"].encode()) == 3000
        if failure == "rate_limit":
            protocol.result = httpx.Response(429)
        elif failure == "unknown":
            protocol.result = httpx.ReadTimeout("provider may have accepted")
        else:
            with runtime.database.sessions.begin() as session:
                session.scalar(select(IMMembershipRow)).active = False
        assert await worker.run_once()
        assert not await worker.run_once()
        if failure == "rate_limit":
            with runtime.database.sessions.begin() as session:
                session.get(OutboxMessageRow, outbox_id).next_retry_at = utcnow() - timedelta(seconds=1)
            protocol.result = None
            assert await worker.run_once()
            assert protocol.sent[1][1]["uuid"] == protocol.sent[2][1]["uuid"]
            assert protocol.sent[0][1]["uuid"] != protocol.sent[1][1]["uuid"]
        expected = {"rate_limit": "delivered", "unknown": "delivery_unknown", "revoked": "dead_letter"}[failure]
        with runtime.database.sessions() as session:
            assert session.get(OutboxMessageRow, outbox_id).status == expected
        assert len(protocol.sent) == {"rate_limit": 3, "unknown": 2, "revoked": 1}[failure]
    finally:
        await adapter.close()
        await runtime.close()


def local_ws_process(connection, app_id, app_secret):
    """Owned process fixture exercising real IPC without any remote connection."""
    connection.send({"kind": "connected"})
    try:
        commit_callback(connection, frame())
        # Hold the process alive until the adapter explicitly shuts it down.
        connection.recv()
    except (RuntimeError, EOFError):
        pass
    finally:
        connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_commit", [False, True])
async def test_owned_ws_process_stops_on_cancel_or_ingress_failure(monkeypatch, fail_commit):
    import trpc_service.channels.feishu as module
    monkeypatch.setattr(module, "websocket_process", local_ws_process)
    adapter = FeishuAdapter("cli_test", "synthetic")
    seen, ready = asyncio.Event(), asyncio.Event()

    async def commit(event):
        assert event.message.text == "hello"
        seen.set()
        if fail_commit:
            raise PermissionError("durable ingress failed")

    task = asyncio.create_task(adapter.receive(build_tenant("feishu", "cli_test").channel_bindings[0], commit, ready))
    try:
        await asyncio.wait_for(ready.wait(), 25)
        pid = adapter._process.pid
        await asyncio.wait_for(seen.wait(), 5)
        if fail_commit:
            with pytest.raises(PermissionError):
                await task
        else:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert adapter._process is None and adapter._connection is None
        assert adapter.diagnostics["message_events"] == 1
        assert adapter.diagnostics["chat_events"] == 1
        assert pid not in {child.pid for child in multiprocessing.active_children()}
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await adapter.close()


@pytest.mark.asyncio
async def test_permission_diagnostic_exports_only_expected_fields():
    from trpc_service.channels.feishu_permissions import inspect
    protocol = Protocol()

    def handle(request):
        if request.url.path.endswith("/application/v6/scopes"):
            return httpx.Response(200,
                                  json={
                                      "code": 0,
                                      "msg": "sensitive-provider-text",
                                      "data": {
                                          "scopes": [{
                                              "scope_name": "im:message.p2p_msg:readonly",
                                              "grant_status": 1,
                                              "scope_type": "tenant",
                                              "token": "sensitive-extra"
                                          }, {
                                              "scope_name": "unrelated:permission",
                                              "grant_status": 1,
                                              "scope_type": "tenant"
                                          }]
                                      }
                                  })
        return protocol(request)

    adapter = FeishuAdapter("cli_test", "synthetic", transport=httpx.MockTransport(handle))
    try:
        result = await inspect(adapter)
        assert result == {
            "status": "queried",
            "im_scopes": [{
                "name": "im:message.p2p_msg:readonly",
                "grant_status": 1,
                "scope_type": "tenant"
            }]
        }
    finally:
        await adapter.close()
