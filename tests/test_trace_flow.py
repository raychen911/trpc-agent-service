"""Stage 6B1 product-chain trace wiring tests (RED first).

Proves the real chain: Worker app HTTP -> agent.turn -> state.session,
gateway CLIENT span traceparent injection and cross-app correlation,
health exclusion, channel.receive/channel.reply for WeCom and Feishu,
async-generator cancellation ending agent.turn, disabled-path emitting
nothing, and exporter failure never touching business responses.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from trpc_agent_sdk.models import LlmResponse
from trpc_agent_sdk.types import Content, FunctionCall, Part

from tests.tenant_helpers import (
    FakeLLMModel,
    FakeModelProvider,
    FakeTenantConfigRepository,
    make_default_test_configs,
    make_in_memory_state_backend,
)
from trpc_agent_sdk.events import Event  # noqa: F401  (typing anchor)
from trpc_service.agent.app import AgentApp
from trpc_service.agent.tool_registry import AllowedToolRegistry
from trpc_service.channels.feishu.sdk import FeishuInboundFrame
from trpc_service.channels.feishu.service import FeishuAibotService
from trpc_service.channels.feishu.settings import FeishuSettings
from trpc_service.channels.binding import ChannelBinding
from trpc_service.channels.models import PublicChannelEvent
from trpc_service.channels.wecom.service import WeComAibotService
from trpc_service.channels.wecom.settings import WeComSettings
from trpc_service.tenant.context import TenantContext
from trpc_service.telemetry import (
    ATTR_REPLY_COUNT,
    ATTR_RESULT,
    ATTR_STATUS,
    SPAN_AGENT_TURN,
    SPAN_CHANNEL_RECEIVE,
    SPAN_CHANNEL_REPLY,
    SPAN_GATEWAY_REQUEST,
    SPAN_STATE_MEMORY,
    SPAN_STATE_SESSION,
    SPAN_TOOL_EXECUTE,
    SPAN_WORKER_REQUEST,
    TelemetryRuntime,
    TelemetrySettings,
    safe_span,
)
from trpc_service.transport.auth import InternalToken
from trpc_service.transport.models import WorkerChatResult, WorkerTask
from trpc_service.worker.app import create_worker_app
from trpc_service.worker.service import WorkerService

_TOKEN_VALUE = "a" * 48


def _wecom_binding() -> ChannelBinding:
    return ChannelBinding(
        binding_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        tenant_id="tenant_default",
        app_id="app_default",
        channel="wecom",
        external_account_id="bot-test",
        secret_ref="env:TRPC_WECOM_BOT_SECRET",
        enabled=True,
        version=1,
    )


def _feishu_binding() -> ChannelBinding:
    return ChannelBinding(
        binding_id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        tenant_id="tenant_default",
        app_id="app_default",
        channel="feishu",
        external_account_id="fs_app_test",
        secret_ref="env:TRPC_FEISHU_APP_SECRET",
        enabled=True,
        version=1,
    )


class _AllowOrderGate:

    async def accept(self, *_args) -> bool:
        return True


_SENTINEL_BODY = "SENTINEL_USER_BODY_DO_NOT_LEAK"


def _env(**overrides: str | None) -> dict[str, str]:
    env = {
        "TRPC_TRACE_ENABLED": "true",
        "TRPC_TRACE_OTLP_ENDPOINT": "http://127.0.0.1:4318/v1/traces",
    }
    for key, value in overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    return env


def _runtime(**overrides: str | None) -> tuple[TelemetryRuntime, InMemorySpanExporter]:
    settings = TelemetrySettings.from_env("flow-test", _env(**overrides))
    exporter = InMemorySpanExporter()
    return TelemetryRuntime(settings, exporter=exporter), exporter


def _by_name(spans, name: str):
    return [s for s in spans if s.name == name]


def _task_body(**overrides) -> dict:
    defaults = {
        "protocol_version": 1,
        "request_id": str(uuid.uuid4()),
        "tenant_id": "tenant_default",
        "app_id": "app_demo",
        "config_version": 1,
        "user_id": "user_default",
        "channel": "web",
        "session_id": "sess-flow-1",
        "message_id": "msg-flow-1",
        "message": _SENTINEL_BODY,
    }
    defaults.update(overrides)
    return defaults


def _make_worker_app(model: FakeLLMModel | None = None, telemetry: TelemetryRuntime | None = None):
    if model is None:
        model = FakeLLMModel()
    repo = FakeTenantConfigRepository(make_default_test_configs())
    provider = FakeModelProvider({"default": model})
    registry = AllowedToolRegistry.default() if telemetry is None else AllowedToolRegistry.default(telemetry=telemetry)
    agent_app = AgentApp(
        model_provider=provider,
        tool_registry=registry,
        state_backend=make_in_memory_state_backend(),
        telemetry=telemetry,
    )
    service = WorkerService(tenant_repository=repo, agent_app=agent_app)
    token = InternalToken(_TOKEN_VALUE)
    app = create_worker_app(worker_service=service, internal_token=token, telemetry=telemetry)
    return app


def _worker_http(app):
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"X-TRPC-Internal-Token": _TOKEN_VALUE},
    )


async def _post_chat(client: httpx.AsyncClient, **overrides) -> httpx.Response:
    return await client.post("/internal/v1/chat", json=_task_body(**overrides))


# ---------------------------------------------------------------------------
# Worker app chain: worker.request -> agent.turn -> state.session (+ tool)
# ---------------------------------------------------------------------------


class TestWorkerChain:

    @pytest.mark.asyncio
    async def test_plain_chat_creates_nested_spans(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        app = _make_worker_app(telemetry=runtime)
        async with _worker_http(app) as client:
            response = await _post_chat(client)
        assert response.status_code == 200
        await runtime.close()

        spans = exporter.get_finished_spans()
        server = _by_name(spans, SPAN_WORKER_REQUEST)
        assert len(server) == 1
        assert server[0].kind is SpanKind.SERVER
        turn = _by_name(spans, SPAN_AGENT_TURN)
        assert len(turn) == 1
        assert turn[0].parent.span_id == server[0].context.span_id
        sessions = _by_name(spans, SPAN_STATE_SESSION)
        assert sessions, "expected at least one state.session span"
        assert all(s.parent.span_id == turn[0].context.span_id for s in sessions)
        assert all(s.attributes[ATTR_STATUS] == "ok" for s in sessions)
        memories = _by_name(spans, SPAN_STATE_MEMORY)
        assert memories, "expected preload_memory to produce a state.memory span"
        assert all(m.parent.span_id == turn[0].context.span_id for m in memories)
        assert all(m.attributes[ATTR_STATUS] == "ok" for m in memories)

        # body sentinel must not appear anywhere in span names/attributes
        blob = " ".join(f"{s.name} {' '.join(f'{k}:{v}' for k, v in dict(s.attributes).items())}" for s in spans)
        assert _SENTINEL_BODY not in blob

    @pytest.mark.asyncio
    async def test_tool_turn_creates_tool_execute_span(self) -> None:
        model = FakeLLMModel(responses=[
            LlmResponse(content=Content(
                role="model", parts=[Part(function_call=FunctionCall(
                    id="fc-1",
                    name="get_current_time",
                    args={},
                ))])),
            LlmResponse(content=Content(role="model", parts=[Part.from_text(text="time reply")])),
        ])
        runtime, exporter = _runtime()
        await runtime.start()
        app = _make_worker_app(model=model, telemetry=runtime)
        async with _worker_http(app) as client:
            response = await _post_chat(client)
        assert response.status_code == 200
        await runtime.close()

        spans = exporter.get_finished_spans()
        turn = _by_name(spans, SPAN_AGENT_TURN)[0]
        tools = _by_name(spans, SPAN_TOOL_EXECUTE)
        assert len(tools) == 1
        assert tools[0].parent.span_id == turn.context.span_id
        assert tools[0].attributes[ATTR_STATUS] == "ok"
        blob = " ".join(" ".join(f"{k}:{v}" for k, v in dict(s.attributes).items()) for s in tools)
        assert "get_current_time" not in blob

    @pytest.mark.asyncio
    async def test_health_creates_no_spans(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        app = _make_worker_app(telemetry=runtime)
        async with _worker_http(app) as client:
            response = await client.get("/health")
        assert response.status_code == 200
        await runtime.close()
        assert exporter.get_finished_spans() == ()

    @pytest.mark.asyncio
    async def test_disabled_runtime_keeps_business_unchanged(self) -> None:
        settings = TelemetrySettings.from_env("flow-test", {})
        runtime = TelemetryRuntime(settings)
        await runtime.start()
        app = _make_worker_app(telemetry=runtime)
        async with _worker_http(app) as client:
            response = await _post_chat(client)
        assert response.status_code == 200
        await runtime.close()

    @pytest.mark.asyncio
    async def test_generator_cancellation_ends_agent_turn_span(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        provider = FakeModelProvider({"default": FakeLLMModel()})
        state_backend = make_in_memory_state_backend()
        agent_app = AgentApp(
            model_provider=provider,
            tool_registry=AllowedToolRegistry.default(telemetry=runtime),
            state_backend=state_backend,
            telemetry=runtime,
        )
        config = make_default_test_configs()["tenant_default"]
        context = TenantContext(tenant_id="tenant_default", app_id="app_demo", user_id="u", channel="web")
        agen = agent_app.run(config, context, "sess-cancel", "hi")
        first = await agen.__anext__()
        assert isinstance(first, Event)
        await agen.aclose()
        await runtime.close()
        turns = _by_name(exporter.get_finished_spans(), SPAN_AGENT_TURN)
        assert len(turns) == 1, "cancelled turn must still end its span"

    @pytest.mark.asyncio
    async def test_consumer_close_after_terminal_event_is_not_an_error(self) -> None:
        # Golden path of every streamed reply: the consumer returns at the
        # terminal event and aclosing() throws GeneratorExit into the span's
        # frame.  The span must END (cancellation rule) yet must not be
        # labelled error — otherwise every successful channel reply pollutes
        # the error signal.
        runtime, exporter = _runtime()
        await runtime.start()
        tracer = runtime.tracer("flow-test")

        async def _chain():
            with safe_span(tracer, SPAN_WORKER_REQUEST, kind=SpanKind.CLIENT):
                yield "delta"
                yield "done"

        agen = _chain()
        assert await agen.__anext__() == "delta"
        assert await agen.__anext__() == "done"
        await agen.aclose()  # consumer finished at the terminal event
        await runtime.close()

        spans = _by_name(exporter.get_finished_spans(), SPAN_WORKER_REQUEST)
        assert len(spans) == 1, "closed stream must still end its span"
        attrs = dict(spans[0].attributes)
        assert "exception.type" not in attrs, attrs
        assert attrs.get(ATTR_STATUS) != "error", attrs


# ---------------------------------------------------------------------------
# Gateway HttpWorkerClient: CLIENT span + traceparent injection
# ---------------------------------------------------------------------------


class TestGatewayClientPropagation:

    @pytest.mark.asyncio
    async def test_chat_injects_traceparent_under_client_span(self) -> None:
        captured: dict[str, str] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured.update(dict(request.headers))
            body = json.loads(request.content)
            result = WorkerChatResult(
                protocol_version=1,
                request_id=body["request_id"],
                response="worker reply",
                error_code=None,
            )
            return httpx.Response(200, json=result.model_dump(mode="json"))

        runtime, exporter = _runtime()
        await runtime.start()
        client = HttpWorkerClientFixture(runtime.tracer("gateway"), transport=httpx.MockTransport(handler))
        task = WorkerTask.model_validate(_task_body())
        try:
            result = await client.chat(task)
            assert result.response == "worker reply"
        finally:
            await client.close()
            await runtime.close()

        assert "traceparent" in captured
        spans = exporter.get_finished_spans()
        client_spans = _by_name(spans, SPAN_WORKER_REQUEST)
        assert len(client_spans) == 1
        assert client_spans[0].kind is SpanKind.CLIENT
        trace_id_hex = format(client_spans[0].context.trace_id, "032x")
        assert captured["traceparent"].startswith(f"00-{trace_id_hex}-")
        blob = " ".join(dict(s.attributes).get(ATTR_STATUS, "") for s in spans)
        assert _SENTINEL_BODY not in blob

    @pytest.mark.asyncio
    async def test_check_health_creates_no_span_and_no_header(self) -> None:
        captured: dict[str, str] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured.update(dict(request.headers))
            return httpx.Response(200, json={"status": "ok"})

        runtime, exporter = _runtime()
        await runtime.start()
        client = HttpWorkerClientFixture(runtime.tracer("gateway"), transport=httpx.MockTransport(handler))
        try:
            assert await client.check_health(2.0) is True
        finally:
            await client.close()
            await runtime.close()
        assert exporter.get_finished_spans() == ()
        assert "traceparent" not in captured

    @pytest.mark.asyncio
    async def test_disabled_client_sends_no_traceparent(self) -> None:
        captured: dict[str, str] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured.update(dict(request.headers))
            body = json.loads(request.content)
            result = WorkerChatResult(
                protocol_version=1,
                request_id=body["request_id"],
                response="worker reply",
                error_code=None,
            )
            return httpx.Response(200, json=result.model_dump(mode="json"))

        settings = TelemetrySettings.from_env("gateway", {})
        runtime = TelemetryRuntime(settings)
        await runtime.start()
        client = HttpWorkerClientFixture(runtime.tracer("gateway"), transport=httpx.MockTransport(handler))
        try:
            await client.chat(WorkerTask.model_validate(_task_body()))
        finally:
            await client.close()
            await runtime.close()
        assert "traceparent" not in captured


def HttpWorkerClientFixture(tracer, *, transport):  # noqa: N802 — test factory
    from trpc_service.gateway.client import HttpWorkerClient
    return HttpWorkerClient(
        base_url="http://127.0.0.1:8001",
        internal_token=InternalToken(_TOKEN_VALUE),
        transport=transport,
        tracer=tracer,
    )


# ---------------------------------------------------------------------------
# Cross-app correlation: gateway CLIENT span parents worker SERVER span
# ---------------------------------------------------------------------------


class TestCrossAppCorrelation:

    @pytest.mark.asyncio
    async def test_gateway_and_worker_share_one_trace(self) -> None:
        gw_runtime, gw_exporter = _runtime()
        wk_runtime, wk_exporter = _runtime()
        await gw_runtime.start()
        await wk_runtime.start()
        worker_app = _make_worker_app(telemetry=wk_runtime)

        client = HttpWorkerClientFixture(
            gw_runtime.tracer("gateway"),
            transport=httpx.ASGITransport(app=worker_app),
        )
        try:
            result = await client.chat(WorkerTask.model_validate(_task_body()))
            assert result.response  # real worker answered through real routes
        finally:
            await client.close()
            await gw_runtime.close()
            await wk_runtime.close()

        gw_spans = gw_exporter.get_finished_spans()
        wk_spans = wk_exporter.get_finished_spans()
        client_span = _by_name(gw_spans, SPAN_WORKER_REQUEST)[0]
        server_span = _by_name(wk_spans, SPAN_WORKER_REQUEST)[0]
        assert client_span.kind is SpanKind.CLIENT
        assert server_span.kind is SpanKind.SERVER
        assert server_span.parent is not None
        assert server_span.parent.span_id == client_span.context.span_id
        for span in (*gw_spans, *wk_spans):
            assert span.context.trace_id == client_span.context.trace_id
        turn = _by_name(wk_spans, SPAN_AGENT_TURN)
        assert len(turn) == 1
        assert turn[0].parent.span_id == server_span.context.span_id

    @pytest.mark.asyncio
    async def test_gateway_middleware_roots_console_request(self) -> None:
        from trpc_service.gateway.app import create_gateway_app
        gw_runtime, gw_exporter = _runtime()
        await gw_runtime.start()

        class _StubClient:

            def __init__(self) -> None:
                self.started = False

            async def start(self) -> None:
                self.started = True

            async def chat(self, task: WorkerTask) -> WorkerChatResult:
                return WorkerChatResult(
                    protocol_version=1,
                    request_id=task.request_id,
                    response="stub reply",
                    error_code=None,
                )

            async def close(self) -> None:
                return None

        stub = _StubClient()
        repo = FakeTenantConfigRepository(make_default_test_configs())
        app = create_gateway_app(worker_client=stub, tenant_repository=repo, telemetry=gw_runtime)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://gw") as http:
            response = await http.post(
                "/api/console/messages",
                json={
                    "tenant_id": "tenant_default",
                    "user_id": "user_abc",
                    "conversation_id": "conv_1",
                    "message_id": "msg_1",
                    "message": _SENTINEL_BODY,
                },
                headers={"X-Tenant-ID": "tenant_default"},
            )
        assert response.status_code == 200
        await gw_runtime.close()

        roots = _by_name(gw_exporter.get_finished_spans(), SPAN_GATEWAY_REQUEST)
        assert len(roots) == 1
        assert roots[0].kind is SpanKind.SERVER
        assert roots[0].parent is None
        blob = " ".join(f"{k}:{v}" for k, v in dict(roots[0].attributes).items())
        assert _SENTINEL_BODY not in blob

    @pytest.mark.asyncio
    async def test_inbound_traceparent_joins_external_trace(self) -> None:
        from trpc_service.gateway.app import create_gateway_app
        ext_trace = "0af7651916cd43dd8448eb211c80319c"
        ext_span = "b7ad6b7169203331"
        runtime, exporter = _runtime()
        await runtime.start()

        class _StubClient:

            async def start(self) -> None:
                return None

            async def chat(self, task: WorkerTask) -> WorkerChatResult:
                return WorkerChatResult(
                    protocol_version=1,
                    request_id=task.request_id,
                    response="stub reply",
                    error_code=None,
                )

            async def close(self) -> None:
                return None

        repo = FakeTenantConfigRepository(make_default_test_configs())
        app = create_gateway_app(worker_client=_StubClient(), tenant_repository=repo, telemetry=runtime)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://gw") as http:
            response = await http.post(
                "/api/console/messages",
                json={
                    "tenant_id": "tenant_default",
                    "user_id": "user_abc",
                    "conversation_id": "conv_1",
                    "message_id": "msg_1",
                    "message": "hi",
                },
                headers={
                    "X-Tenant-ID": "tenant_default",
                    "traceparent": f"00-{ext_trace}-{ext_span}-01",
                },
            )
        assert response.status_code == 200
        await runtime.close()
        root = _by_name(exporter.get_finished_spans(), SPAN_GATEWAY_REQUEST)[0]
        assert root.context.trace_id == int(ext_trace, 16)
        assert root.parent.span_id == int(ext_span, 16)


# ---------------------------------------------------------------------------
# Channel services: channel.receive root + channel.reply
# ---------------------------------------------------------------------------


class _StreamIngress:

    def __init__(self, events: list[PublicChannelEvent]) -> None:
        self._events = events
        self.calls = 0

    async def stream(self, inbound):
        self.calls += 1
        for event in self._events:
            yield event


def _wecom_client() -> MagicMock:
    client = MagicMock()
    client.on_text = AsyncMock()
    client.on_authenticated = AsyncMock()
    client.connect = AsyncMock()
    client.close = AsyncMock()
    client.reply_stream = AsyncMock()
    return client


def _wecom_frame(text: str = "hello") -> dict:
    return {
        "cmd": "aibot_msg_callback",
        "headers": {
            "req_id": "req-abc"
        },
        "body": {
            "msgtype": "text",
            "text": {
                "content": text
            },
            "chattype": "single",
            "msgid": "msg-001",
            "from": {
                "userid": "user-001"
            },
        },
    }


class _FakeWriter:

    def __init__(self) -> None:
        self.appended: list[str] = []
        self.finished = False

    async def append(self, text: str) -> None:
        self.appended.append(text)

    async def finish(self) -> None:
        self.finished = True


class _FakeFeishuClient:

    def __init__(self, writer: _FakeWriter) -> None:
        self._writer = writer

    async def open_reply_stream(self, frame) -> _FakeWriter:
        return self._writer


class TestChannelSpans:

    @pytest.mark.asyncio
    async def test_wecom_receive_and_reply_spans(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        ingress = _StreamIngress([
            PublicChannelEvent(type="delta", data="hi"),
            PublicChannelEvent(type="done"),
        ])
        service = WeComAibotService(
            settings=WeComSettings(bot_id="bot-test", secret="secret-test"),
            client=_wecom_client(),
            ingress=ingress,
            tracer=runtime.tracer("gateway"),
            binding=_wecom_binding(),
        )
        await service.handle_text_frame(_wecom_frame())
        await runtime.close()

        spans = exporter.get_finished_spans()
        receive = _by_name(spans, SPAN_CHANNEL_RECEIVE)
        reply = _by_name(spans, SPAN_CHANNEL_REPLY)
        assert len(receive) == 1 and receive[0].parent is None
        assert len(reply) == 1
        assert reply[0].parent.span_id == receive[0].context.span_id
        assert reply[0].attributes[ATTR_RESULT] == "done"
        assert reply[0].attributes[ATTR_REPLY_COUNT] == 2
        blob = " ".join(f"{s.name}:{k}:{v}" for s in spans for k, v in dict(s.attributes).items())
        assert "hello" not in blob and "msg-001" not in blob and "user-001" not in blob

    @pytest.mark.asyncio
    async def test_wecom_rejected_frame_creates_no_spans(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        service = WeComAibotService(
            settings=WeComSettings(bot_id="bot-test", secret="secret-test"),
            client=_wecom_client(),
            ingress=_StreamIngress([]),
            tracer=runtime.tracer("gateway"),
            binding=_wecom_binding(),
        )
        await service.handle_text_frame({"cmd": "unknown"})
        await runtime.close()
        assert exporter.get_finished_spans() == ()

    @pytest.mark.asyncio
    async def test_wecom_error_terminal_records_error_result(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        ingress = _StreamIngress([PublicChannelEvent(type="error", data="boom")])
        service = WeComAibotService(
            settings=WeComSettings(bot_id="bot-test", secret="secret-test"),
            client=_wecom_client(),
            ingress=ingress,
            tracer=runtime.tracer("gateway"),
            binding=_wecom_binding(),
        )
        await service.handle_text_frame(_wecom_frame())
        await runtime.close()
        reply = _by_name(exporter.get_finished_spans(), SPAN_CHANNEL_REPLY)[0]
        assert reply.attributes[ATTR_RESULT] == "error"

    @pytest.mark.asyncio
    async def test_feishu_receive_and_reply_spans(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        writer = _FakeWriter()
        ingress = _StreamIngress([
            PublicChannelEvent(type="delta", data="hi"),
            PublicChannelEvent(type="done"),
        ])
        service = FeishuAibotService(
            settings=FeishuSettings(app_id="fs_app_test", app_secret="secret-test"),
            client=_FakeFeishuClient(writer),
            ingress=ingress,
            tracer=runtime.tracer("gateway"),
            binding=_feishu_binding(),
            order_gate=_AllowOrderGate(),
        )
        await service.handle_text_frame(
            FeishuInboundFrame(
                external_account_id="fs_app_test",
                external_user_id="fsu_abc123",
                external_conversation_id="oc_xyz789",
                external_message_id="om_msg001",
                text="hello world",
            ))
        await runtime.close()

        spans = exporter.get_finished_spans()
        receive = _by_name(spans, SPAN_CHANNEL_RECEIVE)
        reply = _by_name(spans, SPAN_CHANNEL_REPLY)
        assert len(receive) == 1 and receive[0].parent is None
        assert len(reply) == 1 and reply[0].parent.span_id == receive[0].context.span_id
        assert reply[0].attributes[ATTR_RESULT] == "done"
        assert reply[0].attributes[ATTR_REPLY_COUNT] == 2
        blob = " ".join(f"{s.name}:{k}:{v}" for s in spans for k, v in dict(s.attributes).items())
        assert "hello world" not in blob and "fsu_abc123" not in blob and "om_msg001" not in blob


# ---------------------------------------------------------------------------
# Exporter failure never touches the business path (focused; full matrix Step 5)
# ---------------------------------------------------------------------------


class TestExporterFailureBusinessSafety:

    @pytest.mark.asyncio
    async def test_raising_exporter_keeps_http_200(self) -> None:

        class _RaisingExporter:

            def export(self, spans):
                raise RuntimeError("collector unreachable: secret internal detail")

            def shutdown(self):
                return None

        settings = TelemetrySettings.from_env("flow-test", _env())
        runtime = TelemetryRuntime(settings, exporter=_RaisingExporter())
        await runtime.start()
        app = _make_worker_app(telemetry=runtime)
        async with _worker_http(app) as client:
            response = await _post_chat(client)
            assert response.status_code == 200
            health = await client.get("/health")
            assert health.status_code == 200
        await runtime.close()  # bounded, never raises


class TestRealOtlpEndpointUnreachable:
    """Plan Step 5: real OTLP/HTTP exporter pointed at a dead port.

    A Collector that is not configured/available must not change sync or SSE
    responses, readiness, or shutdown; only fixed ``telemetry.*`` events are
    allowed in the log and the shutdown worker thread must be reclaimed.
    """

    @staticmethod
    def _dead_endpoint() -> str:
        import socket
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        return f"http://127.0.0.1:{port}/v1/traces"

    @pytest.mark.asyncio
    async def test_unreachable_collector_leaves_business_untouched(self, caplog) -> None:
        import logging
        import threading
        import time as _time

        settings = TelemetrySettings.from_env(
            "flow-test",
            _env(
                TRPC_TRACE_OTLP_ENDPOINT=self._dead_endpoint(),
                TRPC_TRACE_EXPORT_TIMEOUT_SECONDS="0.5",
            ),
        )
        runtime = TelemetryRuntime(settings)  # REAL OTLPSpanExporter
        await runtime.start()
        try:
            app = _make_worker_app(telemetry=runtime)
            with caplog.at_level(logging.DEBUG, logger="trpc_service.telemetry"):
                async with _worker_http(app) as client:
                    chat = await _post_chat(client)
                    assert chat.status_code == 200
                    assert chat.json()["response"]  # unchanged contract
                    stream = await client.post("/internal/v1/chat/stream", json=_task_body())
                    assert stream.status_code == 200
                    assert stream.text.strip()  # SSE frames still flow
                    health = await client.get("/health")
                    assert health.status_code == 200  # readiness unchanged
                    assert '"status":"ok"' in health.text.replace(" ", "")
        finally:
            started = _time.monotonic()
            await runtime.close()  # bounded, never raises
            assert _time.monotonic() - started < 8.0
        # The bounded shutdown thread finishes shortly after close() returns;
        # it must not linger unreclaimed.
        deadline = _time.monotonic() + 8.0
        while _time.monotonic() < deadline:
            if not [t for t in threading.enumerate() if "telemetry" in t.name]:
                break
            _time.sleep(0.1)
        assert not [t for t in threading.enumerate() if "telemetry" in t.name], "telemetry-shutdown thread leaked"
        # After the forced flush on close, the telemetry logger may emit only
        # the fixed telemetry.* event names — no exception text, no
        # dead-endpoint URL contents.
        host_port = settings.otlp_endpoint.split("//", 1)[1].split("/", 1)[0]
        telemetry_records = [r for r in caplog.records if r.name == "trpc_service.telemetry"]
        assert telemetry_records, "expected sanitized export-failure events"
        for record in telemetry_records:
            message = record.getMessage()
            assert message.startswith("telemetry."), message
            assert host_port not in message, f"endpoint details leaked into log: {message}"
