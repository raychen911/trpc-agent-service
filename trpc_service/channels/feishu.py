"""Feishu SDK boundary and provider-neutral event mapping."""

from __future__ import annotations

import asyncio
import inspect
import logging
import re
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Callable
from uuid import UUID, uuid4

# Import the SDK before the CLI enters asyncio.run(). lark-channel-sdk 1.4.0
# captures its WebSocket event loop at module import time; importing it from
# authenticate() while our loop is already running makes its background
# thread call run_until_complete() on the active application loop.
from lark_channel import FeishuChannel as _SdkFeishuChannel
from lark_channel.core.enum import LogLevel as _SdkLogLevel

from trpc_service.channels.base import (
    BaseChannelAdapter,
    ConnectionState,
    ParsedProviderEvent,
    ProviderAuthenticationError,
    ProviderOutcomeUnknown,
    ProviderProtocolError,
    ProviderSendAck,
)
from trpc_service.channels.contracts import Channel, ConversationType
from trpc_service.channels.identity import (
    AuthenticatedSender,
    ChannelIdentity,
    ProviderReplyContext,
    RuntimeBotIdentity,
)
from trpc_service.storage.contracts import SecretBytes


logger = logging.getLogger(__name__)


def _value(obj: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj[name]
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


def _nested(obj: Any, *path: str) -> Any:
    current = obj
    for name in path:
        current = _value(current, name, default=None)
        if current is None:
            return None
    return current


_LEADING_MENTION_BEFORE_LATIN_TEXT = re.compile(
    r"^@(?:\S+\s+)+(?=[A-Z][A-Za-z0-9_-]*(?:\s|$))"
)


def _normalize(text: Any, mention_keys: list[str], *, bot_mentioned: bool = False) -> str:
    value = str(text or "")
    for key in mention_keys:
        if key:
            value = value.replace(key, " ")
    # Some SDK event shapes set mentioned_bot but omit mentions[]. In that
    # case the display name (which may contain spaces/CJK) remains at the
    # beginning of the text. Only strip a leading @ span before a clearly
    # separate Latin-text message; inline @ content is left untouched.
    if bot_mentioned and value.lstrip().startswith("@"):
        value = _LEADING_MENTION_BEFORE_LATIN_TEXT.sub("", value.lstrip(), count=1)
    return re.sub(r"\s+", " ", value).strip()


def _mention_id(mention: Any) -> str | None:
    identifier = _value(mention, "id", "sender_id", default=None)
    if isinstance(identifier, str):
        return identifier
    return _value(identifier, "open_id", "user_id", default=None) or _value(
        mention, "open_id", "user_id", default=None
    )


def parse_feishu_event(
    provider_event: Any,
    *,
    channel_identity: ChannelIdentity,
    runtime_bot_identity: RuntimeBotIdentity,
    received_at: datetime,
    trace_id: UUID,
) -> ParsedProviderEvent:
    message_id = _value(provider_event, "message_id")
    chat_id = _value(provider_event, "chat_id")
    if not message_id or not chat_id:
        raise ProviderProtocolError()
    sender_obj = _value(provider_event, "sender", default=None)
    sender_type = _value(sender_obj, "sender_type", "type", default=None) or _value(
        provider_event, "sender_type", default=None
    )
    sender_id_obj = _value(sender_obj, "sender_id", default=None)
    sender_id = (
        _value(sender_id_obj, "open_id", "user_id", default=None)
        if sender_id_obj is not None
        else None
    ) or _value(provider_event, "sender_id", default=None)
    if sender_id is not None and not isinstance(sender_id, str):
        sender_id = _value(sender_id, "open_id", "user_id", default=None)
    explicit_is_bot = _value(provider_event, "sender_is_bot", default=None)
    if explicit_is_bot is None and sender_type:
        explicit_is_bot = True if str(sender_type).lower() == "bot" else False if str(sender_type).lower() in {"user", "human"} else None
    sender = (
        AuthenticatedSender(sender_type=str(sender_type), sender_id=str(sender_id), is_bot=explicit_is_bot)
        if sender_type and sender_id
        else None
    )
    mentions = list(_value(provider_event, "mentions", default=[]) or [])
    bot_mentions = [
        mention for mention in mentions if _mention_id(mention) == runtime_bot_identity.sender_id
    ]
    mention_keys = [
        str(_value(mention, "key", "name", default="") or "") for mention in bot_mentions
    ]
    raw_text = _value(
        provider_event,
        "safe_content_text",
        "content_text",
        "text",
        "body_text",
        default="",
    )
    message_type = _value(
        provider_event,
        "message_type",
        "msg_type",
        "raw_content_type",
        default=None,
    )
    if not message_type:
        message_type = _value(
            _value(provider_event, "content", default=None),
            "kind",
            default="unknown",
        )
    message_type = str(message_type or "unknown")
    bot_mentioned = bool(bot_mentions) or bool(
        _value(provider_event, "mentioned_bot", default=False)
    )
    chat_type = str(_value(provider_event, "chat_type", default="p2p")).lower()
    conversation_type = ConversationType.DIRECT if chat_type in {"p2p", "single", "direct"} else ConversationType.GROUP
    return ParsedProviderEvent(
        channel=Channel.FEISHU,
        channel_identity_digest=channel_identity.identity_digest,
        external_message_id=str(message_id),
        external_conversation_id=str(chat_id),
        conversation_type=conversation_type,
        sender=sender,
        message_type=message_type,
        text=_normalize(raw_text, mention_keys, bot_mentioned=bot_mentioned),
        bot_mentioned=bot_mentioned,
        reply_context=ProviderReplyContext(
            channel=Channel.FEISHU,
            conversation_type=conversation_type,
            reply_target_id=str(chat_id),
            provider_message_id=str(message_id),
        ),
        received_at=received_at,
        trace_id=trace_id,
    )


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


class FeishuProviderClient:
    """Small wrapper around lark-channel-sdk; SDK objects stay in this module."""

    def __init__(
        self,
        identity: ChannelIdentity,
        app_id: SecretBytes,
        *,
        sdk_channel: Any | None = None,
        channel_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.identity = identity
        self._app_id = app_id
        self._channel = sdk_channel
        self._channel_factory = channel_factory
        self._state = ConnectionState.DISCONNECTED
        self._runtime_identity: RuntimeBotIdentity | None = None
        self._callback_tasks: set[asyncio.Task[Any]] = set()

    async def authenticate(self, secret: SecretBytes) -> RuntimeBotIdentity:
        try:
            if self._channel is None:
                if self._channel_factory is None:
                    self._channel_factory = _SdkFeishuChannel
                self._channel = self._channel_factory(
                    app_id=self._app_id.reveal().decode("utf-8"),
                    app_secret=secret.reveal().decode("utf-8"),
                    transport="ws",
                    log_level=_SdkLogLevel.WARNING,
                )
            bot = None
            resolver = getattr(self._channel, "resolve_bot_identity", None)
            if resolver is not None:
                bot = await _maybe_await(resolver())
            bot = bot or getattr(self._channel, "bot_identity", None)
            sender_id = _value(bot, "open_id", "sender_id", default=None) or self.identity.provider_app_or_bot_id
            sender_type = str(_value(bot, "sender_type", default="bot"))
            self._runtime_identity = RuntimeBotIdentity(
                channel=Channel.FEISHU,
                sender_type=sender_type,
                sender_id=str(sender_id),
                channel_identity_digest=self.identity.identity_digest,
                authenticated_at=datetime.now(timezone.utc),
            )
            self._state = ConnectionState.AUTHENTICATED
            return self._runtime_identity
        except Exception:
            raise ProviderAuthenticationError() from None

    async def connect(self, on_event, on_disconnect, on_error) -> None:
        if self._channel is None:
            raise ProviderAuthenticationError()
        self._state = ConnectionState.CONNECTING

        application_loop = asyncio.get_running_loop()

        def schedule(callback, *args):
            def submit() -> None:
                task = application_loop.create_task(callback(*args))
                self._callback_tasks.add(task)

                def completed(done: asyncio.Task[Any]) -> None:
                    self._callback_tasks.discard(done)
                    if done.cancelled():
                        return
                    try:
                        result = done.result()
                    except Exception as error:
                        logger.error(
                            "Feishu provider callback failed: error_type=%s",
                            type(error).__name__,
                        )
                        return
                    safe_code = getattr(result, "safe_code", None)
                    if safe_code is not None:
                        print(
                            "Feishu provider event handled: "
                            f"disposition={getattr(result, 'disposition', 'unknown')} "
                            f"safe_code={safe_code} "
                            f"trace_id={getattr(result, 'trace_id', None)}",
                            flush=True,
                        )

                task.add_done_callback(completed)

            application_loop.call_soon_threadsafe(submit)

        register = getattr(self._channel, "on", None)
        if register is not None:
            register("message", lambda event: schedule(on_event, event))
            register("error", lambda error=None: schedule(on_error, ProviderOutcomeUnknown()))
        del on_disconnect  # The SDK owns transparent reconnects on this connection.
        starter = getattr(self._channel, "start_background", None)
        ready = getattr(self._channel, "connect_until_ready", None)
        if ready is not None:
            await _maybe_await(ready())
        elif starter is not None:
            await _maybe_await(starter())
        else:
            await _maybe_await(self._channel.connect())
        self._state = ConnectionState.READY

    async def close(self) -> None:
        if self._channel is not None:
            closer = getattr(self._channel, "stop_background", None) or getattr(self._channel, "close", None) or getattr(self._channel, "stop", None)
            if closer is not None:
                await _maybe_await(closer())
        self._state = ConnectionState.DISCONNECTED

    async def send_text(self, reply_context: ProviderReplyContext, text: str) -> ProviderSendAck:
        if self._channel is None:
            raise ProviderOutcomeUnknown()
        try:
            response = await _maybe_await(self._channel.send(reply_context.reply_target_id, text))
            request_id = _value(response, "message_id", "request_id", default=reply_context.provider_message_id)
            return ProviderSendAck(acknowledged=True, provider_request_digest=sha256(str(request_id).encode()).hexdigest())
        except Exception:
            raise ProviderOutcomeUnknown() from None

    def connection_state(self) -> ConnectionState:
        return self._state


class FeishuChannelAdapter(BaseChannelAdapter):
    channel = Channel.FEISHU

    def parse_provider_event(self, provider_event: Any) -> ParsedProviderEvent:
        if self._channel_identity is None or self._runtime_bot_identity is None:
            raise ProviderProtocolError()
        return parse_feishu_event(
            provider_event,
            channel_identity=self._channel_identity,
            runtime_bot_identity=self._runtime_bot_identity,
            received_at=self._now(),
            trace_id=uuid4(),
        )


__all__ = ["FeishuChannelAdapter", "FeishuProviderClient", "parse_feishu_event"]
