"""Enterprise WeChat AI Bot long-connection adapter.

The SDK is optional because the traditional WeCom callback adapter remains
usable without it. When ``wecom_aibot_sdk`` is installed, ``WeComAIBotConnector``
maintains one reconnecting WebSocket client per configured binding.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import inspect
import ipaddress
import logging
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol, cast
from urllib.parse import unquote, urlsplit
from urllib.request import Request, urlopen

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from trpc_service.channels.base import (
    Attachment,
    ChannelCapabilities,
    InboundMessage,
    OutboundMessage,
    SendResult,
    sanitize_event_payload,
)
from trpc_service.channels.sdk import run_async
from trpc_service.channels.simple import SimpleJsonChannelAdapter
from trpc_service.security.secrets import SecretManager, redact_secret_text
from trpc_service.tenant.models import ChannelBinding


logger = logging.getLogger(__name__)


class WeComAIBotClient(Protocol):
    is_connected: bool

    def on(self, event: str, handler: Callable[..., Awaitable[None] | None]) -> Any: ...

    async def connect_async(self) -> None: ...

    def disconnect(self) -> Any: ...

    async def send_message(self, chat_id: str, body: Mapping[str, Any]) -> Any: ...


WeComAIBotClientFactory = Callable[[str, str], WeComAIBotClient]
InboundSink = Callable[[InboundMessage, ChannelBinding], Awaitable[None]]


_GROUP_MENTION_RE = re.compile(r"^\s*(?:@[^\s@]+|<@[^>]+>)\s*", flags=re.UNICODE)


def sdk_client_factory(bot_id: str, secret: str) -> WeComAIBotClient:
    try:
        from wecom_aibot_sdk import WSClient
    except ImportError as exc:
        raise RuntimeError(
            "WeCom AI Bot requires the optional 'wecom-aibot-sdk-python' package"
        ) from exc
    return cast(
        WeComAIBotClient,
        WSClient({"bot_id": bot_id, "secret": secret, "max_reconnect_attempts": -1}),
    )


class WeComAIBotAdapter(SimpleJsonChannelAdapter):
    channel_name = "wecom_ai_bot"
    capabilities = ChannelCapabilities(
        max_text_length=4096,
        supports_media=False,
        supports_cards=False,
        delivery_mode="long_connection",
    )
    message_id_fields = ("message_id", "msgid")
    user_id_fields = ("from_user_id", "userid", "user_id")
    group_id_fields = ("chat_id", "chatid")
    text_fields = ("text", "message")

    def __init__(
        self,
        secrets: SecretManager | None = None,
        *,
        connector: WeComAIBotConnector | None = None,
    ) -> None:
        self.secrets = secrets or SecretManager()
        self.connector = connector

    def verify_callback(self, payload: dict[str, Any], binding: ChannelBinding) -> None:
        del payload, binding
        # AI Bot traffic is authenticated by the SDK's WebSocket session.

    def parse_event(self, payload: dict[str, Any], binding: ChannelBinding) -> InboundMessage:
        frame = payload.get("_wecom_ai_bot_frame")
        if frame is not None:
            return parse_wecom_ai_bot_frame(frame, binding)
        return super().parse_event(payload, binding)

    def is_noop(self, payload: dict[str, Any], binding: ChannelBinding) -> bool:
        frame = payload.get("_wecom_ai_bot_frame")
        if frame is None:
            return False
        body = _frame_body(frame)
        return str(body.get("msgtype") or "").lower() not in {"text", "image", "file", "video", "voice", "mixed"}

    def send(self, message: OutboundMessage, binding: ChannelBinding) -> SendResult:
        if message.attachments:
            return SendResult(False, "", "WeCom AI Bot outbound media is not supported", {"unsupported": True})
        if self.connector is None:
            return SendResult(False, "", "WeCom AI Bot connector is not running")
        try:
            response = run_async(lambda: self.connector.send(message, binding))
        except Exception as exc:
            return SendResult(
                False,
                "",
                redact_secret_text(f"WeCom AI Bot send failed: {type(exc).__name__}: {exc}"),
            )
        code = _response_code(response)
        if code not in (None, 0):
            return SendResult(False, "", f"WeCom AI Bot provider error: {code}")
        provider_id = _provider_message_id(response) or message.session_id
        return SendResult(True, f"wecom_ai_bot:{provider_id}", metadata={"message_type": "text"})

    def download_media(
        self,
        attachment: Attachment,
        *,
        max_bytes: int = 20 * 1024 * 1024,
    ) -> tuple[bytes, str | None, str | None]:
        if self.connector is None:
            raise ValueError("WeCom AI Bot connector is not running")
        return self.connector.download_media(attachment, max_bytes=max_bytes)


def parse_wecom_ai_bot_frame(frame: object, binding: ChannelBinding) -> InboundMessage:
    body = _frame_body(frame)
    sender = body.get("from")
    if not isinstance(sender, Mapping) or not sender.get("userid"):
        raise ValueError("WeCom AI Bot frame sender is invalid")
    user_id = str(sender["userid"]).strip()
    message_id = str(body.get("msgid") or "").strip()
    if not message_id:
        message_id = "frame_" + hashlib.sha256(repr(sorted(body.items())).encode()).hexdigest()
    chat_type = str(body.get("chattype") or "single").lower()
    group_id = str(body.get("chatid") or user_id) if chat_type == "group" else None
    message_type = str(body.get("msgtype") or "event").lower()
    bot_id = str(body.get("aibotid") or body.get("botid") or binding.account_id)
    expected_bot_id = str(binding.config.get("bot_id") or binding.account_id)
    if expected_bot_id and bot_id != expected_bot_id:
        raise ValueError("WeCom AI Bot frame bot identity is invalid")
    mentioned = _bot_mentioned(body.get("atuserlist", body.get("mentioned_list")), bot_id)

    text: str | None = None
    attachments: list[Attachment] = []
    if message_type == "text":
        value = body.get("text")
        text = _group_text(value.get("content") if isinstance(value, Mapping) else "", mentioned)
    elif message_type == "voice":
        value = body.get("voice")
        text = str(value.get("content") or "") if isinstance(value, Mapping) else ""
    elif message_type == "mixed":
        value = body.get("mixed")
        items = value.get("msg_item", []) if isinstance(value, Mapping) else []
        parts: list[str] = []
        for item in items if isinstance(items, list) else []:
            if not isinstance(item, Mapping):
                continue
            item_type = str(item.get("msgtype") or "").lower()
            if item_type == "text" and isinstance(item.get("text"), Mapping):
                parts.append(_group_text(item["text"].get("content"), mentioned))
            if item_type in {"image", "file", "video"} and isinstance(item.get(item_type), Mapping):
                attachment = _media_attachment(item[item_type], item_type)
                if attachment:
                    attachments.append(attachment)
        text = "\n".join(part for part in parts if part) or None
    elif message_type in {"image", "file", "video"}:
        value = body.get(message_type)
        if isinstance(value, Mapping):
            attachment = _media_attachment(value, message_type)
            if attachment:
                attachments.append(attachment)

    created_at = _frame_timestamp(body.get("create_time"))
    raw_event = sanitize_event_payload({
        "normalized_event_type": "message",
        "event_type": str((body.get("event") or {}).get("eventtype") or "")
        if isinstance(body.get("event"), Mapping)
        else "",
        "message_type": message_type,
        "chat_type": chat_type,
        "occurred_at": created_at.isoformat(),
        "aibot_id": bot_id,
    })
    raw_event = {key: value for key, value in raw_event.items() if value}
    return InboundMessage(
        channel="wecom_ai_bot",
        account_id=binding.account_id,
        external_message_id=message_id,
        external_user_id=user_id,
        text=text,
        group_id=group_id,
        attachments=attachments,
        raw_event=raw_event,
        internal_user_id=binding.resolve_user_id(user_id),
        received_at=created_at,
    )


class WeComAIBotConnector:
    """Reconnectable SDK connector with bounded frame and media handling."""

    def __init__(
        self,
        secrets: SecretManager | None = None,
        *,
        client_factory: WeComAIBotClientFactory = sdk_client_factory,
        max_media_bytes: int = 20 * 1024 * 1024,
        reconnect_delay_seconds: float = 0.5,
        max_reconnect_delay_seconds: float = 30.0,
    ) -> None:
        self.secrets = secrets or SecretManager()
        self.client_factory = client_factory
        self.max_media_bytes = max_media_bytes
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.max_reconnect_delay_seconds = max_reconnect_delay_seconds
        self._clients: dict[str, WeComAIBotClient] = {}
        self._stop_events: dict[str, asyncio.Event] = {}

    async def run(
        self,
        binding: ChannelBinding,
        sink: InboundSink,
        stop_event: asyncio.Event,
    ) -> None:
        bot_secret_ref = str(binding.config.get("bot_secret_ref") or binding.secret_ref or "")
        if not bot_secret_ref:
            raise RuntimeError("WeCom AI Bot secret reference is not configured")
        try:
            secret = self.secrets.resolve(bot_secret_ref)
        except Exception:
            logger.exception("WeCom AI Bot secret resolution failed for binding %s", binding.binding_id)
            raise
        logger.info("WeCom AI Bot connector starting for binding %s", binding.binding_id)
        delay = self.reconnect_delay_seconds
        local_stop = asyncio.Event()
        self._stop_events[binding.binding_id] = local_stop
        try:
            while not stop_event.is_set() and not local_stop.is_set():
                disconnected = asyncio.Event()
                client: WeComAIBotClient | None = None
                try:
                    client = self.client_factory(binding.account_id, secret)
                    self._clients[binding.binding_id] = client

                    async def on_disconnected(*_: object) -> None:
                        disconnected.set()

                    async def on_frame(frame: object) -> None:
                        body = _frame_body(frame)
                        if str(body.get("msgtype") or "").lower() not in {
                            "text",
                            "image",
                            "file",
                            "video",
                            "voice",
                            "mixed",
                        }:
                            return
                        inbound = parse_wecom_ai_bot_frame(frame, binding)
                        await sink(inbound, binding)

                    client.on("disconnected", on_disconnected)
                    client.on("event.disconnected_event", on_disconnected)
                    for event_name in (
                        "message.text",
                        "message.image",
                        "message.mixed",
                        "message.voice",
                        "message.file",
                        "event",
                    ):
                        client.on(event_name, on_frame)
                    await client.connect_async()
                    delay = self.reconnect_delay_seconds
                    await _wait_for_disconnect(disconnected, stop_event, local_stop)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("WeCom AI Bot connection failed for binding %s", binding.binding_id)
                    if stop_event.is_set() or local_stop.is_set():
                        break
                    await asyncio.sleep(delay)
                    delay = min(self.max_reconnect_delay_seconds, delay * 2)
                finally:
                    if client is not None:
                        self._clients.pop(binding.binding_id, None)
                        result = client.disconnect()
                        if inspect.isawaitable(result):
                            await result
        finally:
            self._stop_events.pop(binding.binding_id, None)

    async def send(self, message: OutboundMessage, binding: ChannelBinding) -> Any:
        client = self._clients.get(binding.binding_id)
        if client is None or not getattr(client, "is_connected", False):
            raise RuntimeError("WeCom AI Bot connector is unavailable")
        target = message.group_id or message.external_user_id
        return await client.send_message(
            target,
            {
                "msgtype": "markdown",
                "markdown": {"content": message.text},
                "client_msg_id": str(message.metadata.get("idempotency_key") or message.session_id),
            },
        )

    def stop(self, binding_id: str) -> None:
        event = self._stop_events.get(binding_id)
        if event:
            event.set()

    def download_media(
        self,
        attachment: Attachment,
        *,
        max_bytes: int | None = None,
    ) -> tuple[bytes, str | None, str | None]:
        url = attachment.metadata.get("provider_url")
        aes_key = attachment.metadata.get("aes_key")
        return _download_media(
            str(url or ""),
            str(aes_key or ""),
            max_bytes=max_bytes or self.max_media_bytes,
        )


async def _wait_for_disconnect(
    disconnected: asyncio.Event,
    stop_event: asyncio.Event,
    local_stop: asyncio.Event,
) -> None:
    tasks = [
        asyncio.create_task(disconnected.wait()),
        asyncio.create_task(stop_event.wait()),
        asyncio.create_task(local_stop.wait()),
    ]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _frame_body(frame: object) -> Mapping[str, Any]:
    body = frame.get("body", {}) if isinstance(frame, Mapping) else getattr(frame, "body", {})
    if not isinstance(body, Mapping):
        raise ValueError("WeCom AI Bot frame body is invalid")
    _validate_shape(body)
    return body


def _validate_shape(value: object, *, depth: int = 0, fields: list[int] | None = None) -> None:
    fields = fields or [0]
    if depth > 10:
        raise ValueError("WeCom AI Bot frame is too deeply nested")
    if isinstance(value, Mapping):
        fields[0] += len(value)
        if fields[0] > 256:
            raise ValueError("WeCom AI Bot frame has too many fields")
        for key, item in value.items():
            if not isinstance(key, str) or len(key) > 128:
                raise ValueError("WeCom AI Bot frame field name is invalid")
            _validate_shape(item, depth=depth + 1, fields=fields)
    elif isinstance(value, (list, tuple)):
        if len(value) > 256:
            raise ValueError("WeCom AI Bot frame list is too large")
        for item in value:
            _validate_shape(item, depth=depth + 1, fields=fields)
    elif isinstance(value, str) and len(value.encode("utf-8")) > 128 * 1024:
        raise ValueError("WeCom AI Bot frame field is too large")


def _media_attachment(value: Mapping[str, Any], media_type: str) -> Attachment | None:
    url = value.get("url")
    aes_key = value.get("aeskey") or value.get("aes_key")
    if not isinstance(url, str) or not isinstance(aes_key, str):
        return None
    return Attachment(
        kind="image" if media_type == "image" else media_type,
        name=_safe_filename(value.get("filename") or value.get("name")),
        content_type={"image": "image/*", "video": "video/*"}.get(media_type),
        metadata={
            "provider_url": url,
            "aes_key": aes_key,
            "source": f"wecom_ai_bot.{media_type}",
        },
    )


def _download_media(
    url: str,
    aes_key: str,
    *,
    max_bytes: int,
) -> tuple[bytes, str | None, str | None]:
    _validate_url(url)
    key = _decode_aes_key(aes_key)
    request = Request(url, headers={"Accept-Encoding": "identity"}, method="GET")
    with urlopen(request, timeout=30) as response:
        content_length = response.headers.get("Content-Length")
        if content_length and int(content_length) > max_bytes + 64:
            raise ValueError("WeCom AI Bot media exceeds limit")
        ciphertext = response.read(max_bytes + 65)
        if len(ciphertext) > max_bytes + 64:
            raise ValueError("WeCom AI Bot media exceeds limit")
        filename = _content_disposition_filename(response.headers.get("Content-Disposition"))
        content_type = response.headers.get("Content-Type")
    if not ciphertext or len(ciphertext) % 16:
        raise ValueError("WeCom AI Bot media decryption failed")
    decryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).decryptor()
    plain = decryptor.update(ciphertext) + decryptor.finalize()
    padding = plain[-1]
    if padding < 1 or padding > 32 or plain[-padding:] != bytes([padding]) * padding:
        raise ValueError("WeCom AI Bot media decryption failed")
    plain = plain[:-padding]
    if len(plain) > max_bytes:
        raise ValueError("WeCom AI Bot media exceeds limit")
    return plain, content_type, filename


def _validate_url(value: str) -> None:
    parsed = urlsplit(value)
    if parsed.scheme.lower() != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("WeCom AI Bot media URL is invalid")
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError:
        return
    if not address.is_global:
        raise ValueError("WeCom AI Bot media URL is invalid")


def _decode_aes_key(value: str) -> bytes:
    if not value or any(char.isspace() for char in value) or len(value) % 4 == 1:
        raise ValueError("WeCom AI Bot media key is invalid")
    try:
        key = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except (binascii.Error, ValueError, TypeError):
        raise ValueError("WeCom AI Bot media key is invalid") from None
    if len(key) != 32:
        raise ValueError("WeCom AI Bot media key is invalid")
    return key


def _group_text(value: object, mentioned: bool) -> str:
    text = str(value or "")
    return _GROUP_MENTION_RE.sub("", text, count=1) if mentioned else text


def _bot_mentioned(value: object, bot_id: str) -> bool:
    if not isinstance(value, (list, tuple)):
        return False
    return any(str(item.get("userid") if isinstance(item, Mapping) else item) == bot_id for item in value)


def _frame_timestamp(value: object):
    from datetime import UTC, datetime

    try:
        timestamp = int(value)
        now = int(time.time())
        if timestamp < 0 or timestamp > now + 86_400:
            raise ValueError
        return datetime.fromtimestamp(timestamp, UTC)
    except (TypeError, ValueError, OverflowError, OSError):
        return datetime.now(UTC)


def _safe_filename(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.replace("\x00", "").strip().strip('"')
    value = value.replace("\\", "/").rsplit("/", 1)[-1]
    if not value or any(ord(char) < 32 for char in value):
        return None
    return value[:512]


def _content_disposition_filename(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    match = re.search(r'filename\*?=(?:"([^"]+)"|([^;]+))', value, flags=re.IGNORECASE)
    if not match:
        return None
    candidate = unquote(match.group(1) or match.group(2) or "").strip()
    return _safe_filename(candidate.split("''", 1)[-1])


def _response_code(value: object) -> int | None:
    if isinstance(value, Mapping):
        raw = value.get("errcode", value.get("code"))
    else:
        raw = getattr(value, "errcode", getattr(value, "code", None))
    if raw is None or isinstance(raw, bool):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _provider_message_id(value: object) -> str | None:
    if isinstance(value, Mapping):
        raw = value.get("msgid") or value.get("message_id") or value.get("req_id")
    else:
        raw = getattr(value, "msgid", None) or getattr(value, "message_id", None) or getattr(value, "req_id", None)
    return str(raw) if raw else None


__all__ = [
    "WeComAIBotAdapter",
    "WeComAIBotClient",
    "WeComAIBotConnector",
    "parse_wecom_ai_bot_frame",
    "sdk_client_factory",
]
