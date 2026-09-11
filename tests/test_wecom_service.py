"""Unit tests for WeComAibotService — SDK lifecycle, frame handling, event forwarding."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from trpc_service.channels.models import PublicChannelEvent
from trpc_service.channels.binding import ChannelBinding
from trpc_service.channels.wecom.settings import WeComSettings
from uuid import UUID


def _make_settings() -> WeComSettings:
    return WeComSettings(bot_id="bot-test", secret="secret-test")


def _make_binding() -> ChannelBinding:
    return ChannelBinding(
        binding_id=UUID("11111111-1111-1111-1111-111111111111"),
        tenant_id="tenant_default",
        app_id="app_default",
        channel="wecom",
        external_account_id="bot-test",
        secret_ref="env:TRPC_WECOM_BOT_SECRET",
        enabled=True,
        version=1,
    )


def _make_text_frame(text: str = "hello") -> dict:
    """Real single-chat shape (SDK 1.0.2): chattype=single, no chatid."""
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


def _make_fake_client(auth_fires: bool = True) -> MagicMock:
    client = MagicMock()
    client.on_text = AsyncMock()
    client.on_authenticated = AsyncMock()
    client.connect = AsyncMock()
    client.close = AsyncMock()
    client.reply_stream = AsyncMock()
    if auth_fires:

        def _fire_auth(handler):
            handler()

        client.on_authenticated.side_effect = _fire_auth
    return client


def _make_fake_ingress(events: list[PublicChannelEvent]) -> MagicMock:
    ingress = MagicMock()

    async def _stream(inbound):
        for event in events:
            yield event

    ingress.stream = MagicMock(side_effect=lambda inbound: _stream(inbound))
    return ingress


def _texts(client: MagicMock) -> list[str]:
    return [call.kwargs.get("text") for call in client.reply_stream.call_args_list]


def _finished(client: MagicMock) -> list[bool]:
    return [call.kwargs.get("finished") for call in client.reply_stream.call_args_list]


@pytest.fixture(autouse=True)
def _supply_binding_to_legacy_service_tests(monkeypatch: pytest.MonkeyPatch):
    """The pre-R2B stream tests exercise a real binding-aware service."""
    from trpc_service.channels.wecom import service as service_module

    constructor = service_module.WeComAibotService

    def create_bound_service(*args, **kwargs):
        kwargs.setdefault("binding", _make_binding())
        return constructor(*args, **kwargs)

    monkeypatch.setattr(service_module, "WeComAibotService", create_bound_service)


class _OrderGate:

    def __init__(self, accepted: bool = True) -> None:
        self.accepted = accepted
        self.calls: list[tuple] = []

    async def accept(self, *args):
        self.calls.append(args)
        return self.accepted


class TestWeComBindingAndOrderGate:

    @pytest.mark.asyncio
    async def test_timed_event_without_shared_order_gate_is_rejected_before_ingress(self):
        from trpc_service.channels.wecom.service import WeComAibotService

        client = _make_fake_client()
        ingress = _make_fake_ingress([PublicChannelEvent(type="done")])
        service = WeComAibotService(_make_settings(), client, ingress, binding=_make_binding())
        frame = _make_text_frame()
        frame["body"]["create_time_ms"] = 1234

        await service.handle_text_frame(frame)

        assert ingress.stream.call_count == 0
        assert service.request_count == 0

    @pytest.mark.asyncio
    async def test_old_platform_event_is_rejected_before_ingress(self):
        from trpc_service.channels.wecom.service import WeComAibotService

        client = _make_fake_client()
        ingress = _make_fake_ingress([PublicChannelEvent(type="done")])
        gate = _OrderGate(accepted=False)
        service = WeComAibotService(_make_settings(), client, ingress, binding=_make_binding(), order_gate=gate)
        frame = _make_text_frame()
        frame["body"]["create_time_ms"] = 1234

        await service.handle_text_frame(frame)

        assert ingress.stream.call_count == 0
        assert service.request_count == 0
        assert len(gate.calls) == 1

    @pytest.mark.asyncio
    async def test_media_is_replied_fixed_text_without_ingress(self):
        from trpc_service.channels.wecom.service import WeComAibotService

        client = _make_fake_client()
        ingress = _make_fake_ingress([PublicChannelEvent(type="done")])
        service = WeComAibotService(_make_settings(), client, ingress, binding=_make_binding())
        frame = _make_text_frame()
        frame["body"]["msgtype"] = "image"
        frame["body"].pop("text")

        await service.handle_text_frame(frame)

        assert ingress.stream.call_count == 0
        assert _texts(client) == ["This message type is not supported."]
        assert _finished(client) == [True]


class TestWeComAibotServiceStart:
    """start() must register handlers, connect, and wait for authentication."""

    @pytest.mark.asyncio
    async def test_start_registers_handler_and_connects(self):
        from trpc_service.channels.wecom.service import WeComAibotService

        client = _make_fake_client()
        ingress = _make_fake_ingress([])
        service = WeComAibotService(_make_settings(), client, ingress)

        await service.start()

        client.on_text.assert_awaited_once()
        client.on_authenticated.assert_awaited_once()
        client.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_auth_success_completes_start(self):
        """When authenticated event fires, start() completes successfully."""
        from trpc_service.channels.wecom.service import WeComAibotService

        client = _make_fake_client(auth_fires=True)
        ingress = _make_fake_ingress([])
        service = WeComAibotService(_make_settings(), client, ingress)

        await service.start()

        assert client.connect.await_count == 1

    @pytest.mark.asyncio
    async def test_auth_timeout_raises_and_closes(self):
        """When authentication times out, start() raises and closes SDK exactly once."""
        from trpc_service.channels.wecom.service import (
            WeComAibotService,
            WeComAuthenticationError,
        )

        client = _make_fake_client(auth_fires=False)
        ingress = _make_fake_ingress([])
        service = WeComAibotService(_make_settings(), client, ingress, auth_timeout=0.05)

        with pytest.raises(WeComAuthenticationError):
            await service.start()

        assert client.close.await_count == 1

    @pytest.mark.asyncio
    async def test_close_after_auth_timeout_is_idempotent(self):
        """After auth timeout already closed, another close() must not call SDK again."""
        from trpc_service.channels.wecom.service import (
            WeComAibotService,
            WeComAuthenticationError,
        )

        client = _make_fake_client(auth_fires=False)
        ingress = _make_fake_ingress([])
        service = WeComAibotService(_make_settings(), client, ingress, auth_timeout=0.05)

        with pytest.raises(WeComAuthenticationError):
            await service.start()

        assert client.close.await_count == 1
        await service.close()
        assert client.close.await_count == 1

    @pytest.mark.asyncio
    async def test_close_disconnects(self):
        from trpc_service.channels.wecom.service import WeComAibotService

        client = _make_fake_client()
        ingress = _make_fake_ingress([])
        service = WeComAibotService(_make_settings(), client, ingress)

        await service.start()
        await service.close()

        client.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self):
        from trpc_service.channels.wecom.service import WeComAibotService

        client = _make_fake_client()
        ingress = _make_fake_ingress([])
        service = WeComAibotService(_make_settings(), client, ingress)

        await service.close()
        await service.close()

        assert client.close.await_count <= 1


class TestWeComAibotServiceHandleFrame:
    """handle_text_frame must call ingress once and forward events."""

    @pytest.mark.asyncio
    async def test_rapid_deltas_are_coalesced_after_immediate_first_reply(self):
        from trpc_service.channels.wecom.service import WeComAibotService

        events = [
            PublicChannelEvent(type="delta", data="A"),
            PublicChannelEvent(type="delta", data="B"),
            PublicChannelEvent(type="delta", data="C"),
            PublicChannelEvent(type="delta", data="D"),
            PublicChannelEvent(type="done"),
        ]
        client = _make_fake_client()
        service = WeComAibotService(
            _make_settings(),
            client,
            _make_fake_ingress(events),
            reply_flush_interval=60.0,
        )

        await service.handle_text_frame(_make_text_frame())

        assert _texts(client) == ["A", "ABCD"]
        assert _finished(client) == [False, True]

    @pytest.mark.asyncio
    async def test_overlong_reply_is_delivered_in_channel_sized_parts(self):
        from trpc_service.channels.wecom.service import WeComAibotService

        text = "你" * 4001
        client = _make_fake_client()
        service = WeComAibotService(
            _make_settings(),
            client,
            _make_fake_ingress([
                PublicChannelEvent(type="delta", data=text),
                PublicChannelEvent(type="done"),
            ]),
            reply_flush_interval=0.0,
        )

        await service.handle_text_frame(_make_text_frame())

        assert _texts(client) == ["你" * 4000, "你"]
        assert _finished(client) == [True, True]

    @pytest.mark.asyncio
    async def test_delta_then_done(self):
        """WeCom stream content is a FULL SNAPSHOT per frame, not an appended delta.

        Official SDK example sends intermediate snapshots and the complete text
        with finish=True; the service must accumulate deltas within one
        handle_text_frame call.
        """
        from trpc_service.channels.wecom.service import WeComAibotService

        events = [
            PublicChannelEvent(type="delta", data="Hello "),
            PublicChannelEvent(type="delta", data="world"),
            PublicChannelEvent(type="done"),
        ]
        client = _make_fake_client()
        ingress = _make_fake_ingress(events)
        service = WeComAibotService(_make_settings(), client, ingress)

        frame = _make_text_frame()
        await service.handle_text_frame(frame)

        assert _texts(client) == ["Hello ", "Hello world"]
        assert _finished(client) == [False, True]

    @pytest.mark.asyncio
    async def test_delta_flushes_again_after_interval(self):
        from trpc_service.channels.wecom import service as service_module

        events = [
            PublicChannelEvent(type="delta", data="A"),
            PublicChannelEvent(type="delta", data="B"),
            PublicChannelEvent(type="delta", data="C"),
            PublicChannelEvent(type="done"),
        ]
        clock = iter((0.0, 0.1, 0.21, 0.21, 0.22))
        client = _make_fake_client()
        service = service_module.WeComAibotService(
            _make_settings(),
            client,
            _make_fake_ingress(events),
            reply_flush_interval=0.2,
            clock=lambda: next(clock),
        )

        await service.handle_text_frame(_make_text_frame())

        assert _texts(client) == ["A", "ABC", "ABC"]
        assert _finished(client) == [False, False, True]

    @pytest.mark.asyncio
    async def test_tool_events_are_suppressed(self):
        """Tool events must not produce any SDK reply."""
        from trpc_service.channels.wecom.service import WeComAibotService

        events = [
            PublicChannelEvent(type="delta", data="Thinking"),
            PublicChannelEvent(
                type="tool",
                data={
                    "kind": "call",
                    "name": "get_time",
                    "args": {}
                },
            ),
            PublicChannelEvent(type="done"),
        ]
        client = _make_fake_client()
        ingress = _make_fake_ingress(events)
        service = WeComAibotService(_make_settings(), client, ingress)

        await service.handle_text_frame(_make_text_frame())

        assert client.reply_stream.await_count == 2
        for call in client.reply_stream.call_args_list:
            text = call.kwargs.get("text", call.args[2] if len(call.args) > 2 else "")
            assert "get_time" not in str(text)

    @pytest.mark.asyncio
    async def test_error_event_sends_safe_finish(self):
        """Error event must send safe text and finish the stream."""
        from trpc_service.channels.wecom.service import WeComAibotService

        events = [
            PublicChannelEvent(type="error", data="Internal details"),
        ]
        client = _make_fake_client()
        ingress = _make_fake_ingress(events)
        service = WeComAibotService(_make_settings(), client, ingress)

        await service.handle_text_frame(_make_text_frame())

        assert client.reply_stream.await_count == 1
        call = client.reply_stream.call_args
        text = call.kwargs.get("text", call.args[2] if len(call.args) > 2 else "")
        assert "Internal details" not in str(text)

    @pytest.mark.asyncio
    async def test_error_event_does_not_log_successful_reply_chain(self, caplog):
        """An error terminal must not look like a successful platform reply."""
        from trpc_service.channels.wecom.service import WeComAibotService

        events = [PublicChannelEvent(type="error", data="Internal details")]
        client = _make_fake_client()
        ingress = _make_fake_ingress(events)
        service = WeComAibotService(_make_settings(), client, ingress)

        caplog.set_level(logging.INFO)
        await service.handle_text_frame(_make_text_frame())

        assert client.reply_stream.await_count == 1
        assert "WeCom reply chain completed" not in caplog.text

    @pytest.mark.asyncio
    async def test_stream_without_done_finishes_with_safe_error(self, caplog):
        """A truncated ingress stream must receive a safe terminal reply."""
        from trpc_service.channels.wecom.service import WeComAibotService

        events = [PublicChannelEvent(type="delta", data="partial")]
        client = _make_fake_client()
        ingress = _make_fake_ingress(events)
        service = WeComAibotService(_make_settings(), client, ingress)

        caplog.set_level(logging.INFO)
        await service.handle_text_frame(_make_text_frame())

        assert client.reply_stream.await_count == 2
        final_call = client.reply_stream.call_args_list[-1]
        assert final_call.kwargs.get("finished") is True
        assert "WeCom reply chain completed" not in caplog.text

    @pytest.mark.asyncio
    async def test_sdk_reply_failure_does_not_retry_agent(self):
        """If SDK reply_stream fails, ingress.stream must not be called again."""
        from trpc_service.channels.wecom.service import WeComAibotService

        events = [
            PublicChannelEvent(type="delta", data="Hello"),
            PublicChannelEvent(type="done"),
        ]
        client = _make_fake_client()
        client.reply_stream = AsyncMock(side_effect=RuntimeError("SDK send failed"))
        ingress = _make_fake_ingress(events)
        service = WeComAibotService(_make_settings(), client, ingress)

        await service.handle_text_frame(_make_text_frame())

        assert ingress.stream.call_count == 1
        assert client.reply_stream.await_count == 3

    @pytest.mark.asyncio
    async def test_bad_frame_does_not_crash(self):
        """Non-text or malformed frame must not raise out of handle_text_frame."""
        from trpc_service.channels.wecom.service import WeComAibotService

        client = _make_fake_client()
        ingress = _make_fake_ingress([])
        service = WeComAibotService(_make_settings(), client, ingress)

        bad_frame = {"body": {"msgtype": "image"}}
        await service.handle_text_frame(bad_frame)

        assert ingress.stream.call_count == 0

    @pytest.mark.asyncio
    async def test_approval_then_done_sends_full_pending_text(self):
        """approval → done: both frames carry the complete pending text; only the terminal finishes."""
        import uuid

        from trpc_service.channels.wecom.service import WeComAibotService
        from trpc_service.governance.approval import pending_reply_for

        aid = uuid.uuid4()
        pending = pending_reply_for(aid)
        events = [
            PublicChannelEvent(type="approval", data={
                "approval_id": str(aid),
                "tool_name": "get_current_time"
            }),
            PublicChannelEvent(type="done"),
        ]
        client = _make_fake_client()
        ingress = _make_fake_ingress(events)
        service = WeComAibotService(_make_settings(), client, ingress)

        await service.handle_text_frame(_make_text_frame())

        assert _texts(client) == [pending, pending]
        assert _finished(client) == [False, True]

    @pytest.mark.asyncio
    async def test_tool_event_does_not_break_accumulation(self):
        """delta → tool → delta → done: suppressed tool frames must not reset the snapshot."""
        from trpc_service.channels.wecom.service import WeComAibotService

        events = [
            PublicChannelEvent(type="delta", data="A"),
            PublicChannelEvent(
                type="tool",
                data={
                    "kind": "call",
                    "name": "get_time",
                    "args": {}
                },
            ),
            PublicChannelEvent(type="delta", data="B"),
            PublicChannelEvent(type="done"),
        ]
        client = _make_fake_client()
        ingress = _make_fake_ingress(events)
        service = WeComAibotService(_make_settings(), client, ingress)

        await service.handle_text_frame(_make_text_frame())

        assert _texts(client) == ["A", "AB"]
        assert _finished(client) == [False, True]

    @pytest.mark.asyncio
    async def test_error_terminal_sends_only_safe_text(self):
        """delta → error: terminal frame is the fixed safe text, no accumulated body or internals."""
        from trpc_service.channels.wecom.service import WeComAibotService

        events = [
            PublicChannelEvent(type="delta", data="部分回答"),
            PublicChannelEvent(type="error", data="Internal details"),
        ]
        client = _make_fake_client()
        ingress = _make_fake_ingress(events)
        service = WeComAibotService(_make_settings(), client, ingress)

        await service.handle_text_frame(_make_text_frame())

        texts = _texts(client)
        assert texts == ["部分回答", "An internal error occurred."]
        assert _finished(client) == [False, True]
        assert "Internal details" not in " ".join(texts)


class TestWeComServiceObservabilityLogs:
    """Fixed chain-observation records must be visible and free of frame content."""

    @pytest.mark.asyncio
    async def test_auth_record_visible_at_warning_level(self, caplog):
        """'authenticated and started' must survive the default (WARNING) root level."""
        import logging

        from trpc_service.channels.wecom.service import WeComAibotService

        caplog.set_level(logging.DEBUG)
        client = _make_fake_client()
        service = WeComAibotService(_make_settings(), client, _make_fake_ingress([]))

        await service.start()

        records = [r for r in caplog.records if "WeCom AI Bot authenticated and started" in r.getMessage()]
        assert len(records) == 1
        assert records[0].levelno >= logging.WARNING
        assert "bot-test" not in caplog.text
        assert "secret-test" not in caplog.text

    @pytest.mark.asyncio
    async def test_accepted_and_completed_records_visible_without_frame_content(self, caplog):
        """Accept/complete records must be WARNING+ and carry no body, IDs or URL."""
        import logging

        from trpc_service.channels.wecom.service import WeComAibotService

        caplog.set_level(logging.DEBUG)
        events = [
            PublicChannelEvent(type="delta", data="hi"),
            PublicChannelEvent(type="done"),
        ]
        client = _make_fake_client()
        service = WeComAibotService(_make_settings(), client, _make_fake_ingress(events))
        frame = _make_text_frame(text="SENTINEL-BODY-frame")

        await service.handle_text_frame(frame)

        for needle in ("WeCom text frame accepted", "WeCom reply chain completed"):
            records = [r for r in caplog.records if needle in r.getMessage()]
            assert records, f"missing observation record: {needle}"
            assert all(r.levelno >= logging.WARNING for r in records), needle
        assert "SENTINEL-BODY-frame" not in caplog.text
        assert "user-001" not in caplog.text
        assert "msg-001" not in caplog.text
        assert "req-abc" not in caplog.text


class TestWeComAibotServiceRequestCount:
    """Service must track request count for diagnostics."""

    @pytest.mark.asyncio
    async def test_request_count_increments(self):
        from trpc_service.channels.wecom.service import WeComAibotService

        events = [PublicChannelEvent(type="done")]
        client = _make_fake_client()
        ingress = _make_fake_ingress(events)
        service = WeComAibotService(_make_settings(), client, ingress)

        assert service.request_count == 0
        await service.handle_text_frame(_make_text_frame())
        assert service.request_count == 1
        await service.handle_text_frame(_make_text_frame())
        assert service.request_count == 2
