"""Unit tests for WeCom SDK facade and client protocol."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


class TestWeComClientProtocol:
    """WeComClient must satisfy the Protocol contract."""

    def test_protocol_requires_on_text_connect_close_reply_stream(self):
        """Any object implementing the Protocol must have the five methods."""
        from trpc_service.channels.wecom.sdk import WeComClient

        class FakeClient:

            async def on_text(self, handler):
                pass

            async def on_authenticated(self, handler):
                pass

            async def connect(self):
                pass

            async def close(self):
                pass

            async def reply_stream(self, frame, stream_id, text, finished):
                pass

        assert isinstance(FakeClient(), WeComClient)

    def test_incomplete_object_fails_protocol_check(self):
        """An object missing a method must not satisfy the Protocol."""
        from trpc_service.channels.wecom.sdk import WeComClient

        class IncompleteClient:

            async def connect(self):
                pass

        assert not isinstance(IncompleteClient(), WeComClient)


class TestCreateWeComClient:
    """create_wecom_client() is the only factory that imports the SDK."""

    def test_create_returns_client_with_required_methods(self, monkeypatch: pytest.MonkeyPatch):
        """Factory must return an object satisfying WeComClient Protocol."""
        from trpc_service.channels.wecom.sdk import WeComClient, create_wecom_client

        mock_ws_client = MagicMock()
        mock_ws_client.connect = AsyncMock()
        mock_ws_client.disconnect = MagicMock()
        mock_ws_client.on = MagicMock()
        mock_ws_client.reply_stream = AsyncMock()

        mock_ws_options_class = MagicMock()
        mock_ws_client_class = MagicMock(return_value=mock_ws_client)

        monkeypatch.setattr(
            "trpc_service.channels.wecom.sdk._WSClientOptions",
            mock_ws_options_class,
        )
        monkeypatch.setattr(
            "trpc_service.channels.wecom.sdk._WSClient",
            mock_ws_client_class,
        )

        from trpc_service.channels.wecom.settings import WeComSettings

        settings = WeComSettings(bot_id="bot-test", secret="secret-test")
        client = create_wecom_client(settings)

        assert isinstance(client, WeComClient)

    def test_create_passes_bot_id_and_secret_only(self, monkeypatch: pytest.MonkeyPatch):
        """Factory must pass bot_id and secret to SDK options, nothing else sensitive."""
        mock_ws_options_class = MagicMock()
        mock_ws_client = MagicMock()
        mock_ws_client.connect = AsyncMock()
        mock_ws_client.disconnect = MagicMock()
        mock_ws_client.on = MagicMock()
        mock_ws_client.reply_stream = AsyncMock()
        mock_ws_client_class = MagicMock(return_value=mock_ws_client)

        monkeypatch.setattr(
            "trpc_service.channels.wecom.sdk._WSClientOptions",
            mock_ws_options_class,
        )
        monkeypatch.setattr(
            "trpc_service.channels.wecom.sdk._WSClient",
            mock_ws_client_class,
        )

        from trpc_service.channels.wecom.settings import WeComSettings
        from trpc_service.channels.wecom.sdk import create_wecom_client

        settings = WeComSettings(bot_id="bot-123", secret="s3cret")
        create_wecom_client(settings)

        mock_ws_options_class.assert_called_once()
        call_kwargs = mock_ws_options_class.call_args
        assert call_kwargs.kwargs.get("bot_id") == "bot-123" or (len(call_kwargs.args) >= 1
                                                                 and call_kwargs.args[0] == "bot-123")
        assert call_kwargs.kwargs.get("secret") == "s3cret" or (len(call_kwargs.args) >= 2
                                                                and call_kwargs.args[1] == "s3cret")

    def test_create_injects_safe_logger(self, monkeypatch: pytest.MonkeyPatch):
        """Factory must explicitly inject the safe logger; never fall back to SDK DefaultLogger."""
        mock_ws_options_class = MagicMock()
        mock_ws_client = MagicMock()
        mock_ws_client.connect = AsyncMock()
        mock_ws_client.disconnect = MagicMock()
        mock_ws_client.on = MagicMock()
        mock_ws_client.reply_stream = AsyncMock()
        mock_ws_client_class = MagicMock(return_value=mock_ws_client)

        monkeypatch.setattr("trpc_service.channels.wecom.sdk._WSClientOptions", mock_ws_options_class)
        monkeypatch.setattr("trpc_service.channels.wecom.sdk._WSClient", mock_ws_client_class)

        from trpc_service.channels.wecom.settings import WeComSettings
        from trpc_service.channels.wecom.sdk import _SafeWeComLogger, create_wecom_client

        settings = WeComSettings(bot_id=_BOTID_SENTINEL, secret=_SECRET_SENTINEL)
        create_wecom_client(settings)

        injected = mock_ws_options_class.call_args.kwargs.get("logger")
        assert injected is not None, "logger must be passed explicitly (no DefaultLogger fallback)"
        assert isinstance(injected, _SafeWeComLogger)
        assert _SECRET_SENTINEL not in repr(injected)
        assert _BOTID_SENTINEL not in repr(injected)


_SECRET_SENTINEL = "SENTINEL-SECRET-9f3c4e5d"
_BODY_SENTINEL = "SENTINEL-BODY-77d1a2b3"
_USERID_SENTINEL = "SENTINEL-USERID-a1b2c3d4"
_MSGID_SENTINEL = "SENTINEL-MSGID-c3d4e5f6"
_URL_SENTINEL = "https://SENTINEL-response-url.example.com/wfcb/9a8b7c6d"
_BOTID_SENTINEL = "SENTINEL-BOTID-5e6f7a8b"


class TestSafeWeComLogger:
    """The SDK-facing logger must never propagate payload content anywhere."""

    _PAYLOAD_SENTINELS = (
        _SECRET_SENTINEL,
        _BODY_SENTINEL,
        _USERID_SENTINEL,
        _MSGID_SENTINEL,
        _URL_SENTINEL,
        _BOTID_SENTINEL,
    )

    def _payload_message(self) -> str:
        import json

        frame = {
            "cmd": "aibot_msg_callback",
            "body": {
                "msgid": _MSGID_SENTINEL,
                "from": {
                    "userid": _USERID_SENTINEL
                },
                "text": {
                    "content": _BODY_SENTINEL
                },
                "response_url": _URL_SENTINEL,
                "bot_id": _BOTID_SENTINEL,
                "secret": _SECRET_SENTINEL,
            },
        }
        return "Received push message: " + json.dumps(frame, ensure_ascii=False)

    def _assert_clean(self, captured_out: str, captured_err: str, caplog_text: str) -> None:
        for sentinel in self._PAYLOAD_SENTINELS:
            assert sentinel not in captured_out
            assert sentinel not in captured_err
            assert sentinel not in caplog_text

    @pytest.mark.parametrize("method", ["debug", "info", "warn", "error"])
    def test_no_payload_leak_on_any_level(self, method, capsys, caplog):
        """debug/info/warn/error given real payload must emit nothing leaky."""
        import logging

        from trpc_service.channels.wecom.sdk import _SafeWeComLogger

        caplog.set_level(logging.DEBUG)
        safe_logger = _SafeWeComLogger()
        message = self._payload_message()
        getattr(safe_logger, method)(message, _SECRET_SENTINEL, _BODY_SENTINEL)

        captured = capsys.readouterr()
        self._assert_clean(captured.out, captured.err, caplog.text)

    @pytest.mark.parametrize("method", ["debug", "info"])
    def test_debug_and_info_are_dropped(self, method, capsys, caplog):
        """debug/info must not forward SDK text at all (no records, no prints)."""
        import logging

        from trpc_service.channels.wecom.sdk import _SafeWeComLogger

        caplog.set_level(logging.DEBUG)
        safe_logger = _SafeWeComLogger()
        getattr(safe_logger, method)("some sdk chatter")

        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == ""
        assert "some sdk chatter" not in caplog.text

    @pytest.mark.parametrize("method", ["warn", "error"])
    def test_warn_and_error_emit_fixed_category_text(self, method, caplog):
        """warn/error must still be observable, but only as fixed category text."""
        import logging

        from trpc_service.channels.wecom.sdk import _SafeWeComLogger

        caplog.set_level(logging.DEBUG)
        safe_logger = _SafeWeComLogger()
        getattr(safe_logger, method)(self._payload_message())

        assert caplog.records, "warn/error must remain observable"
        for record in caplog.records:
            text = record.getMessage()
            assert "WeCom SDK" in text
            assert "Received push message" not in text

    def test_repr_contains_no_credentials(self):
        """repr() of the logger must not carry bot credentials."""
        from trpc_service.channels.wecom.sdk import _SafeWeComLogger

        text = repr(_SafeWeComLogger())
        assert _SECRET_SENTINEL not in text
        assert _BOTID_SENTINEL not in text

    def test_logger_has_no_state_of_frames(self):
        """Logger must not retain frames/messages between calls."""
        from trpc_service.channels.wecom.sdk import _SafeWeComLogger

        safe_logger = _SafeWeComLogger()
        safe_logger.warn(self._payload_message())
        safe_logger.error("another " + _SECRET_SENTINEL)
        assert _SECRET_SENTINEL not in vars(safe_logger).values()
        assert not any(_SECRET_SENTINEL in str(v) for v in vars(safe_logger).values())


class TestFacadeClient:
    """FacadeClient wraps the SDK and exposes the WeComClient Protocol."""

    @pytest.mark.asyncio
    async def test_on_text_registers_handler(self):
        """on_text() must register a handler for text messages on the SDK client."""
        from trpc_service.channels.wecom.sdk import FacadeClient

        sdk_mock = MagicMock()
        sdk_mock.on = MagicMock()
        sdk_mock.connect = AsyncMock()
        sdk_mock.disconnect = MagicMock()
        sdk_mock.reply_stream = AsyncMock()

        facade = FacadeClient(sdk_mock)
        handler = AsyncMock()
        await facade.on_text(handler)

        sdk_mock.on.assert_called_once()
        call_args = sdk_mock.on.call_args
        assert call_args.args[0] == "message"

    @pytest.mark.asyncio
    async def test_on_authenticated_registers_handler(self):
        """on_authenticated() must register a handler for the 'authenticated' event."""
        from trpc_service.channels.wecom.sdk import FacadeClient

        sdk_mock = MagicMock()
        sdk_mock.on = MagicMock()
        sdk_mock.connect = AsyncMock()
        sdk_mock.disconnect = MagicMock()
        sdk_mock.reply_stream = AsyncMock()

        facade = FacadeClient(sdk_mock)
        handler = MagicMock()
        await facade.on_authenticated(handler)

        sdk_mock.on.assert_called_once_with("authenticated", handler)

    @pytest.mark.asyncio
    async def test_connect_delegates_to_sdk(self):
        """connect() must call the SDK's connect()."""
        from trpc_service.channels.wecom.sdk import FacadeClient

        sdk_mock = MagicMock()
        sdk_mock.connect = AsyncMock()
        sdk_mock.disconnect = MagicMock()

        facade = FacadeClient(sdk_mock)
        await facade.connect()

        sdk_mock.connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_delegates_to_sdk_disconnect(self):
        """close() must call the SDK's disconnect()."""
        from trpc_service.channels.wecom.sdk import FacadeClient

        sdk_mock = MagicMock()
        sdk_mock.connect = AsyncMock()
        sdk_mock.disconnect = MagicMock()

        facade = FacadeClient(sdk_mock)
        await facade.close()

        sdk_mock.disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self):
        """Multiple close() calls must not raise."""
        from trpc_service.channels.wecom.sdk import FacadeClient

        sdk_mock = MagicMock()
        sdk_mock.connect = AsyncMock()
        sdk_mock.disconnect = MagicMock()

        facade = FacadeClient(sdk_mock)
        await facade.close()
        await facade.close()

        assert sdk_mock.disconnect.call_count == 1

    @pytest.mark.asyncio
    async def test_reply_stream_delegates_to_sdk(self):
        """reply_stream() must call SDK reply_stream with correct parameters."""
        from trpc_service.channels.wecom.sdk import FacadeClient

        sdk_mock = MagicMock()
        sdk_mock.connect = AsyncMock()
        sdk_mock.disconnect = MagicMock()
        sdk_mock.reply_stream = AsyncMock()

        facade = FacadeClient(sdk_mock)
        frame = {"headers": {"req_id": "req-1"}, "body": {}}
        await facade.reply_stream(frame, "stream-1", "Hello", finished=False)

        sdk_mock.reply_stream.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reply_stream_finish(self):
        """reply_stream with finished=True must pass finish=True to SDK."""
        from trpc_service.channels.wecom.sdk import FacadeClient

        sdk_mock = MagicMock()
        sdk_mock.connect = AsyncMock()
        sdk_mock.disconnect = MagicMock()
        sdk_mock.reply_stream = AsyncMock()

        facade = FacadeClient(sdk_mock)
        frame = {"headers": {"req_id": "req-1"}, "body": {}}
        await facade.reply_stream(frame, "stream-1", "Done", finished=True)

        call_kwargs = sdk_mock.reply_stream.call_args.kwargs
        assert call_kwargs.get("finish") is True
