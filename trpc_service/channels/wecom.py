"""WeCom SDK boundary and provider-neutral event mapping."""

from __future__ import annotations

import asyncio
import inspect
import re
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Callable
from uuid import UUID, uuid4

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


def _value(obj: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj[name]
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


def _normalize(text: Any, keys: list[str]) -> str:
    value = str(text or "")
    for key in keys:
        if key:
            value = value.replace(key, " ")
    return re.sub(r"\s+", " ", value).strip()


_LEADING_BOT_MENTION = re.compile(
    r"^@(?:\S+\s+)+(?=(?:Remember|Recall)\b)", re.IGNORECASE
)


def parse_wecom_event(
    provider_event: Any,
    *,
    channel_identity: ChannelIdentity,
    runtime_bot_identity: RuntimeBotIdentity,
    received_at: datetime,
    trace_id: UUID,
) -> ParsedProviderEvent:
    headers = _value(provider_event, "headers", default={}) or {}
    body = _value(provider_event, "body", default={}) or {}
    message_id = _value(body, "msgid", default=None)
    sender_obj = _value(body, "from", "sender", default={}) or {}
    if isinstance(sender_obj, str):
        # The real WeCom SDK uses `from` as a bare userid for single-chat
        # callbacks, while our provider-neutral fixtures use an object.
        sender_id = sender_obj
        sender_type = "user"
    else:
        sender_id = _value(sender_obj, "userid", "user_id", default=None)
        sender_type = _value(sender_obj, "type", "sender_type", default="user")
    chat_id = _value(body, "chatid", "userid", default=None)
    if not chat_id:
        if isinstance(sender_obj, str):
            chat_id = sender_obj
        else:
            # Real WeCom single-chat callbacks omit chatid and identify the
            # target by from.userid.
            chat_id = sender_id
    if not message_id or not chat_id:
        raise ProviderProtocolError()
    normalized_sender_type = str(sender_type) if sender_type else None
    is_bot = None
    if normalized_sender_type:
        lowered = normalized_sender_type.lower()
        is_bot = True if lowered == "bot" else False if lowered in {"user", "human"} else None
    sender = (
        AuthenticatedSender(sender_type=normalized_sender_type, sender_id=str(sender_id), is_bot=is_bot)
        if normalized_sender_type and sender_id
        else None
    )
    mentions = list(_value(body, "mentions", default=[]) or [])
    bot_mentions = [
        mention for mention in mentions
        if _value(mention, "userid", "user_id", "id", default=None) == runtime_bot_identity.sender_id
    ]
    keys = [str(_value(mention, "key", "name", default="") or "") for mention in bot_mentions]
    text_obj = _value(body, "text", default={}) or {}
    text = _value(text_obj, "content", default=text_obj if isinstance(text_obj, str) else "")
    # WeCom's real aibot callback omits `msgtype` for ordinary text pushes;
    # the presence of the structured `text.content` payload is authoritative.
    message_type_value = _value(body, "msgtype", "message_type", default=None)
    message_type = str(message_type_value or ("text" if text else "unknown"))
    chat_type = str(_value(body, "chattype", "chat_type", default="single")).lower()
    conversation_type = ConversationType.DIRECT if chat_type in {"single", "direct", "p2p"} else ConversationType.GROUP
    normalized_text = _normalize(text, keys)
    text_has_leading_mention = bool(_LEADING_BOT_MENTION.match(normalized_text))
    if text_has_leading_mention:
        normalized_text = _LEADING_BOT_MENTION.sub("", normalized_text, count=1).strip()
    bot_mentioned = bool(bot_mentions) or bool(
        _value(body, "mentioned_bot", default=False)
    ) or runtime_bot_identity.sender_id in list(
        _value(text_obj, "mentioned_list", default=[]) or []
    ) or (conversation_type == ConversationType.GROUP and text_has_leading_mention)
    request_id = _value(headers, "req_id", "request_id", default=None)
    return ParsedProviderEvent(
        channel=Channel.WECOM,
        channel_identity_digest=channel_identity.identity_digest,
        external_message_id=str(message_id),
        external_conversation_id=str(chat_id),
        conversation_type=conversation_type,
        sender=sender,
        message_type=message_type,
        text=normalized_text,
        bot_mentioned=bot_mentioned,
        reply_context=ProviderReplyContext(
            channel=Channel.WECOM,
            conversation_type=conversation_type,
            reply_target_id=str(chat_id),
            protocol_request_id=str(request_id) if request_id else None,
            provider_message_id=str(message_id),
        ),
        received_at=received_at,
        trace_id=trace_id,
    )


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


class WeComProviderClient:
    """Small wrapper around wecom-aibot-python-sdk."""

    def __init__(
        self,
        identity: ChannelIdentity,
        bot_id: SecretBytes,
        *,
        sdk_client: Any | None = None,
        client_factory: Callable[..., Any] | None = None,
        options_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.identity = identity
        self._bot_id = bot_id
        self._client = sdk_client
        self._client_factory = client_factory
        self._options_factory = options_factory
        self._state = ConnectionState.DISCONNECTED
        self._callback_tasks: set[asyncio.Task[Any]] = set()

    async def authenticate(self, secret: SecretBytes) -> RuntimeBotIdentity:
        try:
            if self._client is None:
                if self._client_factory is None or self._options_factory is None:
                    from aibot import WSClient, WSClientOptions

                    self._client_factory = WSClient
                    self._options_factory = WSClientOptions
                options = self._options_factory(
                    bot_id=self._bot_id.reveal().decode("utf-8"),
                    secret=secret.reveal().decode("utf-8"),
                )
                self._client = self._client_factory(options)
            self._state = ConnectionState.AUTHENTICATED
            return RuntimeBotIdentity(
                channel=Channel.WECOM,
                sender_type="bot",
                sender_id=self.identity.provider_app_or_bot_id,
                channel_identity_digest=self.identity.identity_digest,
                authenticated_at=datetime.now(timezone.utc),
            )
        except Exception:
            raise ProviderAuthenticationError() from None

    async def connect(self, on_event, on_disconnect, on_error) -> None:
        if self._client is None:
            raise ProviderAuthenticationError()
        self._state = ConnectionState.CONNECTING
        register = getattr(self._client, "on", None)
        if register is not None:
            def dispatch(event: Any) -> None:
                task = asyncio.create_task(on_event(event))
                self._callback_tasks.add(task)

                def completed(done: asyncio.Task[Any]) -> None:
                    self._callback_tasks.discard(done)
                    if done.cancelled():
                        return
                    try:
                        result = done.result()
                    except Exception as error:
                        print(
                            "WeCom provider callback failed: "
                            f"error_type={type(error).__name__}",
                            flush=True,
                        )
                        return
                    safe_code = getattr(result, "safe_code", None)
                    if safe_code is not None:
                        print(
                            "WeCom provider event handled: "
                            f"disposition={getattr(result, 'disposition', 'unknown')} "
                            f"safe_code={safe_code} "
                            f"trace_id={getattr(result, 'trace_id', None)}",
                            flush=True,
                        )

                task.add_done_callback(completed)

            register("message", dispatch)
            register("disconnected", on_disconnect)
            register("error", lambda *_: asyncio.create_task(on_error(ProviderOutcomeUnknown())))
        await _maybe_await(self._client.connect())
        self._state = ConnectionState.READY

    async def close(self) -> None:
        if self._client is not None:
            closer = getattr(self._client, "disconnect", None) or getattr(self._client, "close", None)
            if closer is not None:
                await _maybe_await(closer())
        self._state = ConnectionState.DISCONNECTED

    async def send_text(self, reply_context: ProviderReplyContext, text: str) -> ProviderSendAck:
        if self._client is None:
            raise ProviderOutcomeUnknown()
        body = {"msgtype": "markdown", "markdown": {"content": text}}
        try:
            if reply_context.protocol_request_id and hasattr(self._client, "reply"):
                frame = {
                    "headers": {"req_id": reply_context.protocol_request_id},
                    "body": {
                        "chatid": reply_context.reply_target_id,
                        "msgid": reply_context.provider_message_id,
                    },
                }
                response = await _maybe_await(self._client.reply(frame, body))
            else:
                response = await _maybe_await(self._client.send_message(reply_context.reply_target_id, body))
            error_code = _value(response, "errcode", "code", default=0)
            acknowledged = error_code in {0, "0", None}
            request_id = _value(response, "request_id", "req_id", default=reply_context.provider_message_id)
            return ProviderSendAck(
                acknowledged=acknowledged,
                provider_request_digest=sha256(str(request_id).encode()).hexdigest(),
            )
        except Exception:
            raise ProviderOutcomeUnknown() from None

    def connection_state(self) -> ConnectionState:
        return self._state


class WeComChannelAdapter(BaseChannelAdapter):
    channel = Channel.WECOM

    def parse_provider_event(self, provider_event: Any) -> ParsedProviderEvent:
        if self._channel_identity is None or self._runtime_bot_identity is None:
            raise ProviderProtocolError()
        return parse_wecom_event(
            provider_event,
            channel_identity=self._channel_identity,
            runtime_bot_identity=self._runtime_bot_identity,
            received_at=self._now(),
            trace_id=uuid4(),
        )


__all__ = ["WeComChannelAdapter", "WeComProviderClient", "parse_wecom_event"]
