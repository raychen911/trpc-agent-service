"""Unit tests for the Feishu SDK facade — pinned to the real lark-channel-sdk 1.4.0 contract.

Evidence (installed SDK source):
- ``FeishuChannel.on_raw_event("im.message.receive_v1", handler)`` dispatches a
  **dict** payload produced by ``_coerce.obj_to_dict`` (``channel/raw_events.py``),
  outside the safety pipeline: no SeenCache dedup; redelivered events run the
  handler again.
- Payload shape (verified by marshaling a real ``P2ImMessageReceiveV1``):
  ``{"header": {...}, "event": {"sender": {"sender_id": {"open_id": ...}},
  "message": {"message_id", "chat_id", "chat_type", "message_type",
  "content": "<JSON string>"}}}``.
- ``stream(to, {"markdown": producer}, opts)`` returns ``SendResult`` only after
  the producer (``async def producer(controller)``) returns normally;
  ``controller.append(chunk)`` accumulates and a normal return completes the
  card (``channel/outbound/streaming/markdown_stream.py``).
"""

from __future__ import annotations

import asyncio
import builtins
import importlib
import inspect
import json
import sys
import types
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


def _real_p2p_payload(
    *,
    text: str = "hello world",
    message_id: str = "om_test1",
    chat_id: str = "oc_chat1",
    open_id: str = "ou_abc",
    chat_type: str = "p2p",
    message_type: str = "text",
    content: object = None,
) -> dict:
    """Build a raw event payload exactly like the SDK's obj_to_dict output."""
    if content is None:
        content = json.dumps({"text": text})
    return {
        "header": {
            "event_id": "evt-1",
            "event_type": "im.message.receive_v1"
        },
        "event": {
            "sender": {
                "sender_id": {
                    "user_id": "",
                    "open_id": open_id,
                    "union_id": ""
                },
                "sender_type": "user",
            },
            "message": {
                "message_id": message_id,
                "chat_id": chat_id,
                "chat_type": chat_type,
                "message_type": message_type,
                "content": content,
            },
        },
    }


class FakeController:
    """Mirrors MarkdownStreamController's producer-facing API."""

    def __init__(self, channel: "FakeSdkChannel") -> None:
        self._channel = channel

    async def append(self, chunk: str) -> None:
        if self._channel.fail_append_after is not None and \
                len(self._channel.controller_appends) >= self._channel.fail_append_after:
            raise RuntimeError("controller write failed")
        self._channel.controller_appends.append(chunk)


class FakeSdkChannel:
    """Duck-typed stand-in for the real FeishuChannel, faithful to its contract."""

    def __init__(self) -> None:
        self.fail_before_producer = False
        self.fail_after_producer = False
        self.raw_handler = None
        self.raw_event_types: list[str] = []
        self.on_calls: list[str] = []
        self.reply_calls: list = []
        self.stream_calls: list = []
        self.controller_appends: list[str] = []
        self.stream_outcome: str | None = None
        self.fail_append_after: int | None = None
        self.disconnect_count = 0
        self.connect_timeouts: list = []
        self.unsubscribe_called = False

    def on(self, name, handler):  # must never be used by the facade
        self.on_calls.append(name)

    def on_raw_event(self, event_type, handler):
        self.raw_event_types.append(event_type)
        self.raw_handler = handler

        def unsubscribe():
            self.unsubscribe_called = True

        return unsubscribe

    async def connect_until_ready(self, *, timeout=None):
        self.connect_timeouts.append(timeout)

    async def disconnect(self):
        self.disconnect_count += 1

    async def reply(self, msg, message, opts=None):
        self.reply_calls.append((msg, message, opts))
        raise AssertionError("reply() must not be used by the facade")

    async def stream(self, to, spec, opts=None):
        self.stream_calls.append((to, spec, opts))
        if self.fail_before_producer:
            self.stream_outcome = "failed"
            raise RuntimeError("stream setup failed")
        producer = spec["markdown"]
        controller = FakeController(self)
        try:
            await producer(controller)
        except asyncio.CancelledError:
            self.stream_outcome = "cancelled"
            raise
        except Exception:
            self.stream_outcome = "failed"
            raise
        if self.fail_after_producer:
            self.stream_outcome = "failed"
            raise RuntimeError("final card commit failed")
        self.stream_outcome = "completed"
        return SimpleNamespace(message_id="om_reply_ok")


def _make_facade() -> tuple:
    from trpc_service.channels.feishu.sdk import FacadeClient

    sdk = FakeSdkChannel()
    return FacadeClient(sdk), sdk


class TestFeishuInboundFrame:

    def test_frame_has_four_fields(self):
        from trpc_service.channels.feishu.sdk import FeishuInboundFrame

        frame = FeishuInboundFrame(
            external_user_id="ou_abc",
            external_conversation_id="oc_xyz",
            external_message_id="om_123",
            text="hello",
        )
        assert frame.external_user_id == "ou_abc"
        assert frame.external_conversation_id == "oc_xyz"
        assert frame.external_message_id == "om_123"
        assert frame.text == "hello"

    def test_frame_is_frozen(self):
        from trpc_service.channels.feishu.sdk import FeishuInboundFrame

        frame = FeishuInboundFrame(
            external_user_id="ou_abc",
            external_conversation_id="oc_xyz",
            external_message_id="om_123",
            text="hello",
        )
        with pytest.raises(AttributeError):
            frame.text = "other"  # type: ignore[misc]


class TestModuleHygiene:

    def test_no_global_warnings_filter(self):
        from trpc_service.channels.feishu import sdk

        source = inspect.getsource(sdk)
        assert "filterwarnings" not in source

    def test_lark_channel_not_imported_at_module_level(self, monkeypatch):
        real_import = builtins.__import__

        def blocker(name, *args, **kwargs):
            if name == "lark_channel" or name.startswith("lark_channel."):
                raise AssertionError("sdk.py must not import lark_channel at module level")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", blocker)
        module = sys.modules.get("trpc_service.channels.feishu.sdk")
        assert module is not None
        importlib.reload(module)
        # restore a normally-importable module for later tests
        monkeypatch.undo()
        importlib.reload(module)


class TestFacadeRegistration:

    @pytest.mark.asyncio
    async def test_registers_on_raw_event_with_exact_type(self):
        facade, sdk = _make_facade()
        handler = AsyncMock()
        await facade.on_raw_event(handler)

        assert sdk.on_calls == []
        assert sdk.raw_event_types == ["im.message.receive_v1"]
        assert sdk.raw_handler is not None

    @pytest.mark.asyncio
    async def test_wrapper_forwards_frame_to_handler(self):
        facade, sdk = _make_facade()
        handler = AsyncMock()
        await facade.on_raw_event(handler)

        await sdk.raw_handler(_real_p2p_payload())

        assert handler.await_count == 1
        frame = handler.await_args.args[0]
        assert frame.external_user_id == "ou_abc"
        assert frame.external_conversation_id == "oc_chat1"
        assert frame.external_message_id == "om_test1"
        assert frame.text == "hello world"

    @pytest.mark.asyncio
    async def test_duplicate_message_id_forwarded_twice(self):
        """Platform redelivery MUST reach the product handler twice so the
        Stage 4C PostgreSQL receipt (not the SDK SeenCache) decides exactly-once."""
        facade, sdk = _make_facade()
        handler = AsyncMock()
        await facade.on_raw_event(handler)

        payload = _real_p2p_payload()
        await sdk.raw_handler(payload)
        await sdk.raw_handler(payload)

        assert handler.await_count == 2
        ids = [c.args[0].external_message_id for c in handler.await_args_list]
        assert ids == ["om_test1", "om_test1"]


class TestRawPayloadFiltering:

    @pytest.mark.asyncio
    async def test_group_chat_is_forwarded_as_group_input(self):
        facade, sdk = _make_facade()
        handler = AsyncMock()
        await facade.on_raw_event(handler)

        await sdk.raw_handler(_real_p2p_payload(chat_type="group"))
        assert handler.await_count == 1
        assert handler.await_args.args[0].conversation_kind == "group"

    @pytest.mark.asyncio
    async def test_non_text_message_is_forwarded_for_local_rejection(self):
        facade, sdk = _make_facade()
        handler = AsyncMock()
        await facade.on_raw_event(handler)

        await sdk.raw_handler(_real_p2p_payload(message_type="image", content=json.dumps({"image_key": "k"})))
        assert handler.await_count == 1
        assert handler.await_args.args[0].kind == "image"
        assert handler.await_args.args[0].text is None

    @pytest.mark.asyncio
    async def test_blank_text_rejected(self):
        facade, sdk = _make_facade()
        handler = AsyncMock()
        await facade.on_raw_event(handler)

        await sdk.raw_handler(_real_p2p_payload(text="   "))
        assert handler.await_count == 0

    @pytest.mark.asyncio
    async def test_malformed_content_json_rejected(self):
        facade, sdk = _make_facade()
        handler = AsyncMock()
        await facade.on_raw_event(handler)

        await sdk.raw_handler(_real_p2p_payload(content="{not json"))
        assert handler.await_count == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload",
        [
            None,
            "not-a-dict",
            {},
            {
                "event": None
            },
            _real_p2p_payload(message_id=""),
            _real_p2p_payload(chat_id=""),
            _real_p2p_payload(open_id=""),
        ],
    )
    async def test_malformed_payloads_rejected_without_error(self, payload):
        facade, sdk = _make_facade()
        handler = AsyncMock()
        await facade.on_raw_event(handler)

        await sdk.raw_handler(payload)  # must not raise
        assert handler.await_count == 0

    @pytest.mark.asyncio
    async def test_rejection_logs_no_platform_fields(self, caplog):
        facade, sdk = _make_facade()
        handler = AsyncMock()
        await facade.on_raw_event(handler)

        with caplog.at_level("DEBUG"):
            await sdk.raw_handler(_real_p2p_payload(chat_type="group", text="SECRET-TEXT", message_id="om_SECRET"))

        assert "SECRET-TEXT" not in caplog.text
        assert "om_SECRET" not in caplog.text
        assert "ou_abc" not in caplog.text

    @pytest.mark.asyncio
    async def test_text_is_stripped(self):
        facade, sdk = _make_facade()
        handler = AsyncMock()
        await facade.on_raw_event(handler)

        await sdk.raw_handler(_real_p2p_payload(text="  padded  "))
        assert handler.await_args.args[0].text == "padded"


class TestReplyStream:

    @pytest.mark.asyncio
    async def test_uses_sdk_stream_not_reply(self):
        from trpc_service.channels.feishu.sdk import FeishuInboundFrame

        facade, sdk = _make_facade()
        frame = FeishuInboundFrame(
            external_user_id="ou_abc",
            external_conversation_id="oc_chat1",
            external_message_id="om_test1",
            text="hi",
        )
        writer = await asyncio.wait_for(facade.open_reply_stream(frame), timeout=1.0)
        await asyncio.sleep(0)  # let the stream task start

        assert sdk.reply_calls == []
        assert len(sdk.stream_calls) == 1
        to, spec, opts = sdk.stream_calls[0]
        assert to == "oc_chat1"
        assert callable(spec["markdown"])
        assert opts["reply_to"] == "om_test1"
        assert hasattr(writer, "append")
        assert hasattr(writer, "finish")

    @pytest.mark.asyncio
    async def test_multiple_deltas_then_finish_completes_card(self):
        from trpc_service.channels.feishu.sdk import FeishuInboundFrame

        facade, sdk = _make_facade()
        frame = FeishuInboundFrame(
            external_user_id="ou_abc",
            external_conversation_id="oc_chat1",
            external_message_id="om_test1",
            text="hi",
        )
        writer = await asyncio.wait_for(facade.open_reply_stream(frame), timeout=1.0)
        await writer.append("chunk1")
        await writer.append("chunk2")
        await writer.finish()

        assert sdk.controller_appends == ["chunk1", "chunk2"]
        assert sdk.stream_outcome == "completed"

    @pytest.mark.asyncio
    async def test_finish_with_no_appends_still_completes(self):
        from trpc_service.channels.feishu.sdk import FeishuInboundFrame

        facade, sdk = _make_facade()
        frame = FeishuInboundFrame(
            external_user_id="ou_abc",
            external_conversation_id="oc_chat1",
            external_message_id="om_test1",
            text="hi",
        )
        writer = await asyncio.wait_for(facade.open_reply_stream(frame), timeout=1.0)
        await writer.finish()
        assert sdk.stream_outcome == "completed"

    @pytest.mark.asyncio
    async def test_finish_is_idempotent(self):
        from trpc_service.channels.feishu.sdk import FeishuInboundFrame

        facade, sdk = _make_facade()
        frame = FeishuInboundFrame(
            external_user_id="ou_abc",
            external_conversation_id="oc_chat1",
            external_message_id="om_test1",
            text="hi",
        )
        writer = await asyncio.wait_for(facade.open_reply_stream(frame), timeout=1.0)
        await writer.append("x")
        await writer.finish()
        await writer.finish()

        assert sdk.controller_appends == ["x"]
        assert sdk.stream_outcome == "completed"

    @pytest.mark.asyncio
    async def test_controller_write_failure_propagates_and_marks_failed(self):
        from trpc_service.channels.feishu.sdk import FeishuInboundFrame

        facade, sdk = _make_facade()
        sdk.fail_append_after = 1  # second controller.append raises
        frame = FeishuInboundFrame(
            external_user_id="ou_abc",
            external_conversation_id="oc_chat1",
            external_message_id="om_test1",
            text="hi",
        )
        writer = await asyncio.wait_for(facade.open_reply_stream(frame), timeout=1.0)
        await asyncio.sleep(0)
        await writer.append("ok")
        from trpc_service.channels.delivery import ChannelSendError
        with pytest.raises(ChannelSendError) as exc_info:
            await asyncio.wait_for(writer.append("bad"), timeout=1.0)
        assert exc_info.value.sent is False

        assert sdk.stream_outcome == "failed"
        # a failed writer must not hang on finish, and must not swallow either
        with pytest.raises(ChannelSendError):
            await asyncio.wait_for(writer.finish(), timeout=1.0)
        assert list(facade._stream_tasks) == []

    @pytest.mark.asyncio
    async def test_stream_failing_before_producer_raises_fast(self):
        """append must propagate immediately when the SDK task died before
        the producer ever consumed the queue (no forever-pending ACK)."""
        from trpc_service.channels.feishu.sdk import FeishuInboundFrame

        facade, sdk = _make_facade()
        sdk.fail_before_producer = True
        frame = FeishuInboundFrame(
            external_user_id="ou_abc",
            external_conversation_id="oc_chat1",
            external_message_id="om_test1",
            text="hi",
        )
        writer = await asyncio.wait_for(facade.open_reply_stream(frame), timeout=1.0)
        # no sleep(0) here: this also pins the race where the task dies
        # after append's pre-check but before the ACK is ever served
        from trpc_service.channels.delivery import ChannelSendError
        with pytest.raises(ChannelSendError) as exc_info:
            await asyncio.wait_for(writer.append("x"), timeout=1.0)
        assert exc_info.value.sent is False
        assert sdk.stream_outcome == "failed"
        assert list(facade._stream_tasks) == []

    @pytest.mark.asyncio
    async def test_final_commit_failure_propagates_through_finish(self):
        """Producer returns normally, then the SDK's card-completion step
        fails: finish() must raise, not report success."""
        from trpc_service.channels.feishu.sdk import FeishuInboundFrame

        facade, sdk = _make_facade()
        sdk.fail_after_producer = True
        frame = FeishuInboundFrame(
            external_user_id="ou_abc",
            external_conversation_id="oc_chat1",
            external_message_id="om_test1",
            text="hi",
        )
        writer = await asyncio.wait_for(facade.open_reply_stream(frame), timeout=1.0)
        await asyncio.sleep(0)
        await writer.append("chunk")
        from trpc_service.channels.delivery import ChannelSendError
        with pytest.raises(ChannelSendError) as exc_info:
            await asyncio.wait_for(writer.finish(), timeout=1.0)
        assert exc_info.value.sent is False
        assert sdk.stream_outcome == "failed"
        assert list(facade._stream_tasks) == []

    @pytest.mark.asyncio
    async def test_close_cancels_pending_stream_task(self):
        from trpc_service.channels.feishu.sdk import FeishuInboundFrame

        facade, sdk = _make_facade()
        frame = FeishuInboundFrame(
            external_user_id="ou_abc",
            external_conversation_id="oc_chat1",
            external_message_id="om_test1",
            text="hi",
        )
        await asyncio.wait_for(facade.open_reply_stream(frame), timeout=1.0)  # never finished

        await facade.close()
        assert sdk.stream_outcome in ("cancelled", None)
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        assert pending == []


class TestLifecycle:

    @pytest.mark.asyncio
    async def test_connect_delegates_keyword_only_timeout(self):
        facade, sdk = _make_facade()
        await facade.connect_until_ready(timeout_seconds=10.0)
        assert sdk.connect_timeouts == [10.0]

    @pytest.mark.asyncio
    async def test_close_disconnects_public_api_idempotently(self):
        facade, sdk = _make_facade()
        await facade.close()
        await facade.close()
        assert sdk.disconnect_count == 1

    @pytest.mark.asyncio
    async def test_close_unsubscribes_raw_handler(self):
        facade, sdk = _make_facade()
        await facade.on_raw_event(AsyncMock())
        await facade.close()
        assert sdk.unsubscribe_called is True


class TestFeishuClientProtocol:

    def test_protocol_shape(self):
        from trpc_service.channels.feishu.sdk import FeishuClient

        assert isinstance(_make_facade()[0], FeishuClient)

    def test_incomplete_object_fails_protocol_check(self):
        from trpc_service.channels.feishu.sdk import FeishuClient

        class IncompleteClient:

            async def connect_until_ready(self, timeout_seconds: float) -> None:
                pass

        assert not isinstance(IncompleteClient(), FeishuClient)


class TestSdkImportBoundary:

    def test_lark_channel_only_imported_in_facade(self):
        from pathlib import Path

        service_root = Path(inspect.getsourcefile(importlib.import_module("trpc_service"))).parent
        allowed = service_root / "channels" / "feishu" / "sdk.py"
        offenders = []
        for path in service_root.rglob("*.py"):
            if path == allowed:
                continue
            if "lark_channel" in path.read_text(encoding="utf-8"):
                offenders.append(str(path.relative_to(service_root)))
        assert offenders == []

    def test_gateway_preloads_sdk_before_starting_server(self, monkeypatch: pytest.MonkeyPatch):
        from trpc_service import _cli
        from trpc_service.channels.feishu import sdk

        events: list[str] = []
        monkeypatch.setattr(sdk, "preload_feishu_sdk", lambda: events.append("preload"))

        def fake_runner(*args, **kwargs):
            events.append("server")

        result = _cli.main(["gateway"], server_runner=fake_runner, environ={})

        assert result == 0
        assert events == ["preload", "server"]


class TestCreateFeishuClient:

    def test_constructs_real_channel_with_credentials_only(self, monkeypatch: pytest.MonkeyPatch):
        from trpc_service.channels.feishu.sdk import FeishuClient, create_feishu_client
        from trpc_service.channels.feishu.settings import FeishuSettings

        constructed = {}

        class FakeFeishuChannel:

            def __init__(self, **kwargs):
                constructed.update(kwargs)

        fake_module = types.ModuleType("lark_channel")
        fake_module.FeishuChannel = FakeFeishuChannel  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "lark_channel", fake_module)

        settings = FeishuSettings(app_id="cli_abc", app_secret="s3cret")
        client = create_feishu_client(settings)

        assert isinstance(client, FeishuClient)
        assert constructed == {"app_id": "cli_abc", "app_secret": "s3cret"}

    def test_missing_sdk_raises_runtime_error(self, monkeypatch: pytest.MonkeyPatch):
        from trpc_service.channels.feishu.sdk import create_feishu_client
        from trpc_service.channels.feishu.settings import FeishuSettings

        monkeypatch.setitem(sys.modules, "lark_channel", None)
        settings = FeishuSettings(app_id="cli_abc", app_secret="s3cret")
        with pytest.raises(RuntimeError):
            create_feishu_client(settings)
