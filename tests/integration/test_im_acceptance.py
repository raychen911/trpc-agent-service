"""Native SQL Runner + real public provider/Telegram conversion + durable IM tables."""

import httpx
import json
import pytest
from telegram import Bot

from trpc_service.channels.acceptance import AcceptanceRuntime, test_tenant as build_acceptance_tenant
from trpc_service.channels.telegram import TelegramAdapter
from cryptography.fernet import Fernet
from tests.integration.test_im_transports import ProtocolRequest, update
from tests.integration.test_openrouter_accounting import provider_response


def stream_response():
    value = provider_response()
    value["object"] = "chat.completion.chunk"
    value["choices"][0]["delta"] = value["choices"][0].pop("message")
    return httpx.Response(200,
                          content=("data: " + json.dumps(value) + "\n\ndata: [DONE]\n\n").encode(),
                          headers={"content-type": "text/event-stream"})


@pytest.mark.asyncio
async def test_one_real_runner_path_and_duplicate_suppression(tmp_path):
    tenant = build_acceptance_tenant("telegram", "123456")
    runtime = AcceptanceRuntime(tmp_path / "isolated", tenant, b"k" * 32, [Fernet.generate_key()])
    calls = []

    def handle(request):
        calls.append(request)
        return stream_response()

    request = ProtocolRequest()
    try:
        await runtime.start("synthetic-api-key", 163840, transport=httpx.MockTransport(handle))
        async with Bot("123456:synthetic-token", request=request) as bot:
            adapter = TelegramAdapter(bot)
            event = adapter.normalize(update(text="synthetic challenge"), runtime.binding)
            report = await runtime.execute(event, adapter, "synthetic challenge")
            assert report["input_statuses"] == ["succeeded"]
            assert report["reply_statuses"] and all(value == "delivered" for value in report["reply_statuses"])
            assert report["native_session_events"] >= 2 and report["duplicate_suppressed"]
            assert report["model_attempts"][0]["status"] == "settled"
            assert len(calls) == 1
            assert [name for name, _ in request.sent].count("sendMessage") == 1
            with pytest.raises(PermissionError):
                await runtime.execute(event, adapter, "synthetic challenge")
            assert len(calls) == 1
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_wecom_socket_runner_budget_native_sql_and_ack_share_trace(tmp_path):
    from websockets.asyncio.server import serve
    from wecom_aibot_sdk import WSClient
    from trpc_service.channels.wecom import WecomAdapter, QuietProtocolLogger
    from trpc_service.channels.acceptance import await_wecom
    from trpc_service.telemetry import runtime as telemetry
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    tenant = build_acceptance_tenant("wecom", "synthetic-bot")
    runtime = AcceptanceRuntime(tmp_path / "isolated", tenant, b"k" * 32, [Fernet.generate_key()])
    exporter = InMemorySpanExporter()
    telemetry.configure("acceptance-contract", span_exporter=exporter)
    sent = []

    async def protocol(socket):
        auth = json.loads(await socket.recv())
        await socket.send(json.dumps({"headers": auth["headers"], "errcode": 0}))
        await socket.send(
            json.dumps({
                "headers": {
                    "req_id": "synthetic-request"
                },
                "cmd": "aibot_msg_callback",
                "body": {
                    "aibotid": "synthetic-bot",
                    "msgid": "synthetic-message",
                    "from": {
                        "userid": "synthetic-user"
                    },
                    "chattype": "single",
                    "msgtype": "text",
                    "text": {
                        "content": "synthetic challenge"
                    }
                }
            }))
        reply = json.loads(await socket.recv())
        sent.append(reply)
        await socket.send(json.dumps({"headers": reply["headers"], "errcode": 0}))
        await socket.wait_closed()

    try:
        await runtime.start("synthetic-api-key", 163840, transport=httpx.MockTransport(lambda _: stream_response()))
        async with serve(protocol, "127.0.0.1", 0) as server:
            adapter = WecomAdapter(
                WSClient({
                    "bot_id": "synthetic-bot",
                    "secret": "synthetic-secret",
                    "ws_url": f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}",
                    "logger": QuietProtocolLogger(),
                    "max_reconnect_attempts": 1
                }))
            try:
                event = await await_wecom(adapter, runtime.binding, "synthetic challenge", 5, lambda _: None)
                report = await runtime.execute(event, adapter, "synthetic challenge")
                assert report["input_statuses"] == ["succeeded"]
                assert all(status == "delivered" for status in report["reply_statuses"])
                assert len(sent) == 1 and sent[0]["body"]["stream"]["finish"] is True
            finally:
                await adapter.close()
        for provider in telemetry._providers:
            provider.force_flush()
        spans = exporter.get_finished_spans()
        names = {span.name for span in spans}
        assert {"im.callback", "worker.execute", "session.append", "model.provider_response", "im.reply"} <= names
        chain = [span for span in spans if span.name in names - {"session.get"}]
        assert len({span.context.trace_id for span in chain}) == 1
        assert all("synthetic challenge" not in str(span.attributes) for span in spans)
        from sqlalchemy import select
        from trpc_service.persistence.models import InboundMessageRow, AuditLogRow
        with runtime.database.sessions() as session:
            inbound = session.scalar(select(InboundMessageRow))
            audit = session.scalar(select(AuditLogRow).where(AuditLogRow.decision == "execution_succeeded"))
            trace_id = f"{chain[0].context.trace_id:032x}"
            assert inbound.trace_id == audit.trace_id == trace_id
            assert audit.channel == "wecom" and audit.user_id.startswith("usr_")
    finally:
        await runtime.close()
        telemetry.shutdown()


@pytest.mark.asyncio
async def test_private_challenge_is_required_before_enrolment(tmp_path):
    tenant = build_acceptance_tenant("telegram", "123456")
    runtime = AcceptanceRuntime(tmp_path / "isolated", tenant, b"k" * 32, [Fernet.generate_key()])
    try:
        adapter = TelegramAdapter(Bot("123456:synthetic-token"))
        event = adapter.normalize(update(text="not-the-challenge"), runtime.binding)
        with pytest.raises(PermissionError):
            await runtime.execute(event, adapter, "secret challenge")
        assert not runtime.report()["input_statuses"]
    finally:
        await runtime.close()
