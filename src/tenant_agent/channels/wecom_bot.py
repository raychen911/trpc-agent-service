"""WeCom intelligent bots over the official authenticated WebSocket protocol.

Protocol reference: https://github.com/WecomTeam/aibot-node-sdk
This is distinct from the CorpID/AgentID enterprise-application callback API.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from websockets.asyncio.client import ClientConnection, connect

from tenant_agent.channels.base import (
    DeliveryError,
    DeliveryResult,
    ParsedWebhook,
    PermanentDeliveryError,
    RateLimited,
    SignatureError,
    UnsupportedMessage,
    WebhookRequest,
)
from tenant_agent.ids import stable_checksum
from tenant_agent.models import (
    Attachment,
    ChannelBindingConfig,
    ChannelType,
    ChatType,
    InboundEnvelope,
    OutboundMessage,
    TenantConfig,
)
from tenant_agent.security import CompositeSecretResolver

WECOM_BOT_URL = "wss://openws.work.weixin.qq.com"
WECOM_BOT_STREAM_BYTES = 20_480
FrameHandler = Callable[[dict[str, Any]], Awaitable[None]]


def bot_outbox_kind(tenant_id: str, binding_id: str) -> str:
    return "wb_" + stable_checksum(tenant_id, binding_id)[:29]


def validate_bot_credentials(bot_id: str, bot_secret: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", bot_id):
        raise ValueError("invalid WeCom Bot ID format")
    if not re.fullmatch(r"[A-Za-z0-9_+/=-]{16,256}", bot_secret):
        raise ValueError("invalid WeCom bot secret format")


class BotDisconnected(DeliveryError):
    """The provider reports another owner; stop instead of fighting for ownership."""


class WeComBotConnection:
    """One socket reader, ACK-correlated sends, finite buffers, and cancellation cleanup."""

    def __init__(
        self,
        *,
        url: str = WECOM_BOT_URL,
        timeout: float = 10.0,
        heartbeat_seconds: float = 30.0,
    ) -> None:
        self.url = url
        self.timeout = timeout
        self.heartbeat_seconds = heartbeat_seconds
        self.socket: ClientConnection | None = None
        self.bot_id = ""
        self.closed = asyncio.Event()
        self.failure: Exception | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._send_lock = asyncio.Lock()

    @asynccontextmanager
    async def connected(
        self, bot_id: str, secret: str, handler: FrameHandler
    ) -> AsyncIterator[WeComBotConnection]:
        validate_bot_credentials(bot_id, secret)
        self.closed.clear()
        self.failure = None
        # Wire-level DEBUG logging contains authentication frames. This private
        # logger cannot inherit the application's configured debug level.
        wire_logger = logging.Logger("wecom-wire", level=logging.CRITICAL + 1)
        async with connect(
            self.url,
            open_timeout=self.timeout,
            close_timeout=3,
            ping_interval=None,
            max_size=1_048_576,
            max_queue=16,
            proxy=None,
            logger=wire_logger,
        ) as socket:
            self.socket = socket
            self.bot_id = bot_id
            reader = asyncio.create_task(self._read(handler), name="wecom-bot-reader")
            heartbeat: asyncio.Task[None] | None = None
            try:
                try:
                    await self.request("aibot_subscribe", {"bot_id": bot_id, "secret": secret})
                except DeliveryError as exc:
                    if self.failure is not None or self.closed.is_set():
                        raise
                    raise SignatureError("WeCom bot authentication failed") from exc
                heartbeat = asyncio.create_task(self._heartbeat(), name="wecom-bot-heartbeat")
                yield self
            finally:
                self.socket = None
                tasks = [reader, *([heartbeat] if heartbeat else [])]
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self.closed.set()

    async def request(
        self, command: str, body: dict[str, Any] | None = None, *, req_id: str | None = None
    ) -> dict[str, Any]:
        async with self._send_lock:
            socket = self.socket
            if socket is None or self.closed.is_set():
                raise DeliveryError("WeCom bot socket is unavailable")
            request_id = req_id or f"{command}_{uuid.uuid4().hex}"
            future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
            self._pending[request_id] = future
            frame: dict[str, Any] = {"cmd": command, "headers": {"req_id": request_id}}
            if body is not None:
                frame["body"] = body
            try:
                async with asyncio.timeout(self.timeout):
                    await socket.send(json.dumps(frame, ensure_ascii=False))
                    result = await future
                code = result.get("errcode")
                if code in {45009, 45033, 45047}:
                    raise RateLimited(60)
                if type(code) is not int or code != 0:
                    raise DeliveryError("WeCom bot request was rejected")
                return result
            except TimeoutError:
                # A late ACK must never satisfy a later send sharing the callback
                # req_id. Close the connection to invalidate that ACK generation.
                self.failure = DeliveryError("WeCom bot acknowledgement timed out")
                self.closed.set()
                await socket.close()
                raise DeliveryError("WeCom bot acknowledgement timed out") from None
            finally:
                self._pending.pop(request_id, None)
                if not future.done():
                    future.cancel()

    async def _read(self, handler: FrameHandler) -> None:
        assert self.socket is not None
        callback_tasks: set[asyncio.Task[None]] = set()

        def callback_done(task: asyncio.Task[None]) -> None:
            callback_tasks.discard(task)
            if task.cancelled():
                return
            error = task.exception()
            if error is not None:
                self.failure = (
                    error if isinstance(error, Exception) else RuntimeError("WeCom bot callback failed")
                )
                self.closed.set()
                if self.socket is not None:
                    close_task = asyncio.create_task(self.socket.close(), name="wecom-bot-close")
                    close_task.add_done_callback(
                        lambda completed: None if completed.cancelled() else completed.exception()
                    )

        try:
            async for raw in self.socket:
                frame = json.loads(raw)
                if not isinstance(frame, dict):
                    raise DeliveryError("invalid WeCom bot frame")
                headers = frame.get("headers")
                req_id = headers.get("req_id") if isinstance(headers, dict) else None
                if frame.get("cmd") in {"aibot_msg_callback", "aibot_event_callback"}:
                    body = frame.get("body", {})
                    event = body.get("event", {}) if isinstance(body, dict) else {}
                    if isinstance(event, dict) and event.get("eventtype") == "disconnected_event":
                        raise BotDisconnected("WeCom bot connection replaced")
                    if len(callback_tasks) >= 256:
                        raise DeliveryError("WeCom bot callback buffer is full")

                    async def invoke_callback(callback_frame: dict[str, Any] = frame) -> None:
                        await handler(callback_frame)

                    callback_task: asyncio.Task[None] = asyncio.create_task(
                        invoke_callback(), name="wecom-bot-callback"
                    )
                    callback_tasks.add(callback_task)
                    callback_task.add_done_callback(callback_done)
                elif isinstance(req_id, str) and req_id in self._pending:
                    future = self._pending[req_id]
                    if not future.done():
                        future.set_result(frame)
        except Exception as exc:
            self.failure = exc
        finally:
            for callback_task in callback_tasks:
                callback_task.cancel()
            await asyncio.gather(*callback_tasks, return_exceptions=True)
            self.closed.set()
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(DeliveryError("WeCom bot connection closed"))

    async def _heartbeat(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.heartbeat_seconds)
                await self.request("ping")
        except Exception as exc:
            self.failure = exc
            self.closed.set()
            if self.socket is not None:
                await self.socket.close()


class WeComBotAdapter:
    name = "wecom_bot"

    def __init__(self, connection: WeComBotConnection | None = None) -> None:
        self.connection = connection
        self._last_delivery = 0.0

    async def parse(
        self,
        request: WebhookRequest,
        *,
        tenant: TenantConfig,
        binding: ChannelBindingConfig,
        secrets: CompositeSecretResolver,
    ) -> ParsedWebhook:
        del request, tenant, binding, secrets
        raise UnsupportedMessage("WeCom bot messages require an authenticated socket")

    def parse_frame(
        self, frame: dict[str, Any], *, tenant: TenantConfig, binding: ChannelBindingConfig, bot_id: str
    ) -> InboundEnvelope | None:
        if frame.get("cmd") != "aibot_msg_callback":
            return None
        body = frame.get("body", {})
        if not isinstance(body, dict):
            raise UnsupportedMessage("invalid WeCom bot message")
        if not hmac.compare_digest(str(body.get("aibotid", "")).encode(), bot_id.encode()):
            raise SignatureError("WeCom Bot ID does not match the authenticated binding")
        sender = body.get("from", {})
        user_id = sender.get("userid") if isinstance(sender, dict) else None
        msgid = body.get("msgid")
        headers = frame.get("headers", {})
        req_id = headers.get("req_id") if isinstance(headers, dict) else None
        if not all(isinstance(value, str) and 0 < len(value) <= 512 for value in (user_id, msgid, req_id)):
            raise UnsupportedMessage("WeCom bot message identity is missing or invalid")
        chat_type = body.get("chattype")
        if chat_type not in {"single", "group"}:
            raise UnsupportedMessage("unsupported WeCom bot chat type")
        chat_id = body.get("chatid") if chat_type == "group" else user_id
        if not isinstance(chat_id, str) or not chat_id:
            raise UnsupportedMessage("WeCom bot group chat ID is missing")
        text: list[str] = []
        attachments: list[Attachment] = []

        def extract(item: dict[str, Any]) -> None:
            kind = item.get("msgtype")
            value = item.get(str(kind), {})
            if not isinstance(value, dict):
                raise UnsupportedMessage("invalid WeCom bot content")
            if kind in {"text", "voice"}:
                content = value.get("content", "")
                if not isinstance(content, str):
                    raise UnsupportedMessage("invalid WeCom bot text")
                text.append(content)
            elif kind in {"image", "file", "video"}:
                # URLs are short-lived bearer capabilities and aeskey is a
                # secret. Neither enters events, memory, telemetry, or a queue.
                attachments.append(
                    Attachment(
                        kind=kind,
                        external_id=f"{msgid}:{len(attachments)}",
                    )
                )
            else:
                raise UnsupportedMessage("unsupported WeCom bot message type")

        if body.get("msgtype") == "mixed":
            mixed = body.get("mixed", {})
            items = mixed.get("msg_item") if isinstance(mixed, dict) else None
            if not isinstance(items, list) or len(items) > 100:
                raise UnsupportedMessage("invalid WeCom bot mixed message")
            for item in items:
                if not isinstance(item, dict):
                    raise UnsupportedMessage("invalid WeCom bot mixed item")
                extract(item)
        else:
            extract(body)
        return InboundEnvelope(
            message_id=str(msgid),
            tenant_id=tenant.tenant_id,
            app_id=binding.app_id,
            binding_id=binding.binding_id,
            channel=ChannelType.WECOM_BOT,
            external_account_id=binding.external_account_id,
            external_user_id=str(user_id),
            external_chat_id=chat_id,
            chat_type=ChatType.GROUP if chat_type == "group" else ChatType.DIRECT,
            text="\n".join(text) or "[Attachment received; content is not downloaded]",
            attachments=tuple(attachments),
            metadata={
                "platform_message_id": msgid,
                "wecom_bot_req_id": req_id,
                "wecom_bot_stream_id": "s_" + hashlib.sha256(str(req_id).encode()).hexdigest()[:48],
            },
        )

    async def deliver(
        self,
        message: OutboundMessage,
        *,
        tenant: TenantConfig,
        binding: ChannelBindingConfig,
        secrets: CompositeSecretResolver,
    ) -> DeliveryResult:
        connection = self.connection
        if connection is None or connection.closed.is_set():
            raise DeliveryError("WeCom bot connection owner is unavailable")
        bot_id = await secrets.resolve(binding.credential_refs["bot_id"])
        if (
            message.tenant_id != tenant.tenant_id
            or message.binding_id != binding.binding_id
            or not hmac.compare_digest(bot_id.encode(), connection.bot_id.encode())
        ):
            raise PermanentDeliveryError("WeCom bot delivery scope mismatch")
        if message.attachments:
            raise PermanentDeliveryError("WeCom bot media upload is not configured")
        loop = asyncio.get_running_loop()
        if loop.time() - self._last_delivery < 2.1:
            raise RateLimited(2.1 - (loop.time() - self._last_delivery))
        req_id = message.metadata.get("wecom_bot_req_id")
        if not isinstance(req_id, str) or not req_id:
            raise PermanentDeliveryError("WeCom bot reply correlation is missing")
        if len(message.text.encode()) > WECOM_BOT_STREAM_BYTES:
            raise PermanentDeliveryError("WeCom bot reply exceeds the stream limit")
        stream_id = message.metadata.get("wecom_bot_stream_id")
        if stream_id is None:
            stream_id = "s_" + hashlib.sha256((message.stream_key or req_id).encode()).hexdigest()[:48]
        elif not isinstance(stream_id, str) or not re.fullmatch(r"s_[0-9a-f]{48}", stream_id):
            raise PermanentDeliveryError("WeCom bot stream correlation is invalid")
        stream: dict[str, Any] = {"id": stream_id, "finish": True, "content": message.text}
        body: dict[str, Any] = {"msgtype": "stream", "stream": stream}
        if message.cards:
            if len(message.cards) != 1:
                raise PermanentDeliveryError("one WeCom bot reply supports one card")
            body.update(msgtype="stream_with_template_card", template_card=message.cards[0])
        self._last_delivery = loop.time()
        await connection.request("aibot_respond_msg", body, req_id=req_id)
        return DeliveryResult((stream_id,))
