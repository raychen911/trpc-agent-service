"""Unit tests for FeishuAibotService — P0 fix: streaming via writer."""

from __future__ import annotations

from uuid import uuid4

import pytest

from trpc_service.channels.binding import ChannelBinding
from trpc_service.channels.feishu.sdk import FeishuInboundFrame, FeishuReplyWriter
from trpc_service.channels.models import PublicChannelEvent

TENANT_ID = "tenant_default"


class _AllowOrderGate:

    async def accept(self, *_args) -> bool:
        return True


def _binding(tenant_id: str = TENANT_ID) -> ChannelBinding:
    return ChannelBinding(
        binding_id=uuid4(),
        tenant_id=tenant_id,
        app_id="cli_test",
        channel="feishu",
        external_account_id="cli_test",
        secret_ref="env:TRPC_FEISHU_SECRET",
        enabled=True,
        version=1,
    )


def _frame(**overrides: str) -> FeishuInboundFrame:
    defaults = {
        "external_user_id": "ou_abc123",
        "external_conversation_id": "oc_xyz789",
        "external_message_id": "om_msg001",
        "text": "hello world",
        "external_account_id": "cli_test",
        "conversation_kind": "direct",
        "kind": "text",
    }
    defaults.update(overrides)
    return FeishuInboundFrame(**defaults)


class FakeReplyWriter:
    """Test double for FeishuReplyWriter."""

    def __init__(self) -> None:
        self.append_calls = []
        self.finished = False

    async def append(self, text: str) -> None:
        self.append_calls.append(text)

    async def finish(self) -> None:
        self.finished = True


class FakeFeishuClient:
    """Test double for FeishuClient Protocol with streaming."""

    def __init__(self) -> None:
        self.on_raw_event_handler = None
        self.connect_count = 0
        self.close_count = 0
        self.stream_calls = []

    async def on_raw_event(self, handler) -> None:
        self.on_raw_event_handler = handler

    async def connect_until_ready(self, timeout_seconds: float) -> None:
        self.connect_count += 1

    async def close(self) -> None:
        self.close_count += 1

    async def open_reply_stream(self, frame: FeishuInboundFrame) -> FeishuReplyWriter:
        writer = FakeReplyWriter()
        self.stream_calls.append({"frame": frame, "writer": writer})
        return writer


class FakeIngressService:
    """Test double for ChannelIngressService."""

    def __init__(self, events: list[PublicChannelEvent] | None = None) -> None:
        self.events = events or []
        self.stream_calls = []

    async def stream(self, inbound):
        self.stream_calls.append(inbound)
        for event in self.events:
            yield event


class TestFeishuAibotServiceStart:
    """start() must register callback and connect."""

    @pytest.mark.asyncio
    async def test_start_registers_handler_and_connects(self):
        from trpc_service.channels.feishu.service import FeishuAibotService
        from trpc_service.channels.feishu.settings import FeishuSettings

        settings = FeishuSettings(app_id="cli_test", app_secret="secret-test")
        client = FakeFeishuClient()
        ingress = FakeIngressService()
        service = FeishuAibotService(settings=settings,
                                     client=client,
                                     ingress=ingress,
                                     binding=_binding(),
                                     order_gate=_AllowOrderGate())

        await service.start()

        assert client.on_raw_event_handler is not None
        assert client.connect_count == 1


class TestFeishuAibotServiceClose:
    """close() must be idempotent."""

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self):
        from trpc_service.channels.feishu.service import FeishuAibotService
        from trpc_service.channels.feishu.settings import FeishuSettings

        settings = FeishuSettings(app_id="cli_test", app_secret="secret-test")
        client = FakeFeishuClient()
        ingress = FakeIngressService()
        service = FeishuAibotService(settings=settings,
                                     client=client,
                                     ingress=ingress,
                                     binding=_binding(),
                                     order_gate=_AllowOrderGate())

        await service.close()
        await service.close()

        assert client.close_count == 1


class TestFeishuAibotServiceHandleTextFrame:
    """handle_text_frame must call ingress.stream() exactly once."""

    @pytest.mark.asyncio
    async def test_valid_frame_calls_ingress_once(self):
        from trpc_service.channels.feishu.service import FeishuAibotService
        from trpc_service.channels.feishu.settings import FeishuSettings

        settings = FeishuSettings(app_id="cli_test", app_secret="secret-test")
        client = FakeFeishuClient()
        events = [
            PublicChannelEvent(type="delta", data="partial"),
            PublicChannelEvent(type="done"),
        ]
        ingress = FakeIngressService(events=events)
        service = FeishuAibotService(settings=settings,
                                     client=client,
                                     ingress=ingress,
                                     binding=_binding(),
                                     order_gate=_AllowOrderGate())

        frame = _frame()
        await service.handle_text_frame(frame)

        assert len(ingress.stream_calls) == 1
        assert ingress.stream_calls[0].external_message_id == "om_msg001"

    @pytest.mark.asyncio
    async def test_invalid_frame_does_not_call_ingress(self):
        from trpc_service.channels.feishu.service import FeishuAibotService
        from trpc_service.channels.feishu.settings import FeishuSettings

        settings = FeishuSettings(app_id="cli_test", app_secret="secret-test")
        client = FakeFeishuClient()
        ingress = FakeIngressService()
        service = FeishuAibotService(settings=settings,
                                     client=client,
                                     ingress=ingress,
                                     binding=_binding(),
                                     order_gate=_AllowOrderGate())

        frame = _frame(text="")
        await service.handle_text_frame(frame)

        assert len(ingress.stream_calls) == 0


class TestFeishuAibotServiceReplyChain:
    """Reply chain must use writer.append/finish, not per-delta reply()."""

    @pytest.mark.asyncio
    async def test_delta_then_done_appends_and_finishes(self):
        from trpc_service.channels.feishu.service import FeishuAibotService
        from trpc_service.channels.feishu.settings import FeishuSettings

        settings = FeishuSettings(app_id="cli_test", app_secret="secret-test")
        client = FakeFeishuClient()
        events = [
            PublicChannelEvent(type="delta", data="chunk1"),
            PublicChannelEvent(type="delta", data="chunk2"),
            PublicChannelEvent(type="done"),
        ]
        ingress = FakeIngressService(events=events)
        service = FeishuAibotService(settings=settings,
                                     client=client,
                                     ingress=ingress,
                                     binding=_binding(),
                                     order_gate=_AllowOrderGate())

        frame = _frame()
        await service.handle_text_frame(frame)

        assert len(client.stream_calls) == 1
        writer = client.stream_calls[0]["writer"]
        assert writer.append_calls == ["chunk1", "chunk2"]
        assert writer.finished is True

    @pytest.mark.asyncio
    async def test_error_event_appends_safe_text_and_finishes(self):
        from trpc_service.channels.feishu.service import FeishuAibotService
        from trpc_service.channels.feishu.settings import FeishuSettings

        settings = FeishuSettings(app_id="cli_test", app_secret="secret-test")
        client = FakeFeishuClient()
        events = [
            PublicChannelEvent(type="delta", data="partial"),
            PublicChannelEvent(type="error", data="internal detail"),
        ]
        ingress = FakeIngressService(events=events)
        service = FeishuAibotService(settings=settings,
                                     client=client,
                                     ingress=ingress,
                                     binding=_binding(),
                                     order_gate=_AllowOrderGate())

        frame = _frame()
        await service.handle_text_frame(frame)

        assert len(client.stream_calls) == 1
        writer = client.stream_calls[0]["writer"]
        assert len(writer.append_calls) >= 1
        assert writer.finished is True
        assert "internal detail" not in writer.append_calls[-1]

    @pytest.mark.asyncio
    async def test_tool_event_is_suppressed(self):
        from trpc_service.channels.feishu.service import FeishuAibotService
        from trpc_service.channels.feishu.settings import FeishuSettings

        settings = FeishuSettings(app_id="cli_test", app_secret="secret-test")
        client = FakeFeishuClient()
        events = [
            PublicChannelEvent(type="tool", data={"name": "get_time"}),
            PublicChannelEvent(type="done"),
        ]
        ingress = FakeIngressService(events=events)
        service = FeishuAibotService(settings=settings,
                                     client=client,
                                     ingress=ingress,
                                     binding=_binding(),
                                     order_gate=_AllowOrderGate())

        frame = _frame()
        await service.handle_text_frame(frame)

        # tool suppressed, but done still finishes the stream
        assert len(client.stream_calls) == 1
        writer = client.stream_calls[0]["writer"]
        assert writer.finished is True

    @pytest.mark.asyncio
    async def test_missing_done_appends_safe_text_and_finishes(self):
        from trpc_service.channels.feishu.service import FeishuAibotService
        from trpc_service.channels.feishu.settings import FeishuSettings

        settings = FeishuSettings(app_id="cli_test", app_secret="secret-test")
        client = FakeFeishuClient()
        events = [
            PublicChannelEvent(type="delta", data="chunk1"),
        ]
        ingress = FakeIngressService(events=events)
        service = FeishuAibotService(settings=settings,
                                     client=client,
                                     ingress=ingress,
                                     binding=_binding(),
                                     order_gate=_AllowOrderGate())

        frame = _frame()
        await service.handle_text_frame(frame)

        assert len(client.stream_calls) == 1
        writer = client.stream_calls[0]["writer"]
        assert len(writer.append_calls) >= 2
        assert writer.finished is True

    @pytest.mark.asyncio
    async def test_sdk_stream_failure_does_not_retry(self):
        from trpc_service.channels.feishu.service import FeishuAibotService
        from trpc_service.channels.feishu.settings import FeishuSettings

        settings = FeishuSettings(app_id="cli_test", app_secret="secret-test")
        client = FakeFeishuClient()

        class FailingWriter:

            async def append(self, text: str) -> None:
                raise RuntimeError("SDK write failed")

            async def finish(self) -> None:
                pass

        async def failing_open_reply_stream(frame):
            return FailingWriter()

        client.open_reply_stream = failing_open_reply_stream
        events = [
            PublicChannelEvent(type="delta", data="chunk1"),
            PublicChannelEvent(type="done"),
        ]
        ingress = FakeIngressService(events=events)
        service = FeishuAibotService(settings=settings,
                                     client=client,
                                     ingress=ingress,
                                     binding=_binding(),
                                     order_gate=_AllowOrderGate())

        frame = _frame()
        await service.handle_text_frame(frame)

        assert len(ingress.stream_calls) == 1


class TestCreateFeishuService:
    """create_feishu_service factory must handle None settings."""

    def test_none_settings_returns_none(self):
        from trpc_service.channels.feishu.service import create_feishu_service

        ingress = FakeIngressService()
        result = create_feishu_service(settings=None, ingress=ingress)
        assert result is None

    def test_valid_settings_returns_service(self):
        from trpc_service.channels.feishu.service import create_feishu_service
        from trpc_service.channels.feishu.settings import FeishuSettings

        settings = FeishuSettings(app_id="cli_test", app_secret="secret-test")
        ingress = FakeIngressService()
        client = FakeFeishuClient()
        result = create_feishu_service(settings=settings,
                                       ingress=ingress,
                                       binding=_binding(),
                                       order_gate=_AllowOrderGate(),
                                       client=client)
        assert result is not None


class _FailingFinishWriter(FakeReplyWriter):

    async def finish(self) -> None:
        raise RuntimeError("SDK finish failed")


class _FailingAppendWriter:

    def __init__(self) -> None:
        self.append_calls: list[str] = []

    async def append(self, text: str) -> None:
        self.append_calls.append(text)
        raise RuntimeError("SDK write failed")

    async def finish(self) -> None:
        pass


def _service(client, events, tenant=TENANT_ID):
    from trpc_service.channels.feishu.service import FeishuAibotService
    from trpc_service.channels.feishu.settings import FeishuSettings

    settings = FeishuSettings(app_id="cli_test", app_secret="secret-test")
    ingress = FakeIngressService(events=events)
    service = FeishuAibotService(
        settings=settings,
        client=client,
        ingress=ingress,
        binding=_binding(tenant),
        order_gate=_AllowOrderGate(),
    )
    return service, ingress


class TestTerminalDoneLogging:
    """terminal=done may only be logged when the SDK stream really finished."""

    @pytest.mark.asyncio
    async def test_done_with_successful_finish_logs_terminal_done(self, caplog):
        client = FakeFeishuClient()
        service, _ = _service(
            client,
            [
                PublicChannelEvent(type="delta", data="chunk1"),
                PublicChannelEvent(type="done"),
            ],
        )
        with caplog.at_level("INFO"):
            await service.handle_text_frame(_frame())
        assert "terminal=done" in caplog.text
        assert "completed" in caplog.text

    @pytest.mark.asyncio
    async def test_failed_finish_does_not_log_terminal_done(self, caplog):
        client = FakeFeishuClient()

        async def open_failing(frame):
            return _FailingFinishWriter()

        client.open_reply_stream = open_failing
        service, _ = _service(
            client,
            [
                PublicChannelEvent(type="delta", data="chunk1"),
                PublicChannelEvent(type="done"),
            ],
        )
        with caplog.at_level("INFO"):
            await service.handle_text_frame(_frame())
        assert "terminal=done" not in caplog.text
        assert "completed" not in caplog.text

    @pytest.mark.asyncio
    async def test_error_event_does_not_log_terminal_done(self, caplog):
        client = FakeFeishuClient()
        service, _ = _service(
            client,
            [PublicChannelEvent(type="error", data="internal detail")],
        )
        with caplog.at_level("INFO"):
            await service.handle_text_frame(_frame())
        assert "terminal=done" not in caplog.text
        assert "completed" not in caplog.text

    @pytest.mark.asyncio
    async def test_missing_done_does_not_log_terminal_done(self, caplog):
        client = FakeFeishuClient()
        service, _ = _service(client, [PublicChannelEvent(type="delta", data="chunk1")])
        with caplog.at_level("INFO"):
            await service.handle_text_frame(_frame())
        assert "terminal=done" not in caplog.text
        assert "completed" not in caplog.text

    @pytest.mark.asyncio
    async def test_append_failure_no_terminal_done_no_retry(self, caplog):
        client = FakeFeishuClient()

        async def open_failing(frame):
            return _FailingAppendWriter()

        client.open_reply_stream = open_failing
        service, ingress = _service(
            client,
            [
                PublicChannelEvent(type="delta", data="chunk1"),
                PublicChannelEvent(type="done"),
            ],
        )
        with caplog.at_level("INFO"):
            await service.handle_text_frame(_frame())
        assert "terminal=done" not in caplog.text
        assert len(ingress.stream_calls) == 1  # never re-runs the agent


class TestRedeliveryPassThrough:
    """Duplicate platform message ids must each reach the ingress; the
    Stage 4C PostgreSQL receipt (already covered elsewhere) decides once."""

    @pytest.mark.asyncio
    async def test_same_message_id_reaches_ingress_twice(self):
        client = FakeFeishuClient()
        service, ingress = _service(client, [PublicChannelEvent(type="done")])
        await service.handle_text_frame(_frame())
        await service.handle_text_frame(_frame())
        assert len(ingress.stream_calls) == 2
        assert {c.external_message_id for c in ingress.stream_calls} == {"om_msg001"}


class TestLogSafety:

    @pytest.mark.asyncio
    async def test_logs_never_contain_platform_fields_or_secrets(self, caplog):
        client = FakeFeishuClient()
        service, _ = _service(
            client,
            [
                PublicChannelEvent(type="delta", data="partial answer"),
                PublicChannelEvent(type="error", data="upstream secret detail"),
            ],
        )
        with caplog.at_level("DEBUG"):
            await service.handle_text_frame(_frame())
        text = caplog.text
        for leak in (
                "om_msg001",
                "ou_abc123",
                "oc_xyz789",
                "hello world",
                "upstream secret detail",
                "secret-test",
                "SDK write failed",
                "SDK finish failed",
        ):
            assert leak not in text


class TestAdapterSingleSourceOfTruth:
    """Service must not keep its own copy of event-conversion rules (P2)."""

    @pytest.mark.asyncio
    async def test_service_routes_events_through_adapter_encode_event(self, monkeypatch):
        from trpc_service.channels.feishu.adapter import FeishuChannelAdapter

        seen: list[str] = []
        original = FeishuChannelAdapter.encode_event

        def spy(self, event):
            seen.append(event.type)
            return original(self, event)

        monkeypatch.setattr(FeishuChannelAdapter, "encode_event", spy)

        client = FakeFeishuClient()
        events = [
            PublicChannelEvent(type="tool", data={"name": "get_time"}),
            PublicChannelEvent(type="delta", data="chunk"),
            PublicChannelEvent(type="done"),
        ]
        service, _ = _service(client, events)
        await service.handle_text_frame(_frame())

        assert seen == ["tool", "delta", "done"]

    def test_service_has_no_duplicated_safe_error_text(self):
        import inspect

        from trpc_service.channels.feishu import service as service_module

        source = inspect.getsource(service_module)
        assert '"An internal error occurred."' not in source, \
            "safe text must come from the Adapter, not a second copy"
