"""WeCom Smart Bot long-connection transport.

The Smart Bot API is deliberately separate from the traditional WeCom
application/webhook adapter.  A bot authenticates over one outbound WebSocket
with its Bot ID and Secret, receives ``aibot_msg_callback`` frames, and must
use the callback ``req_id`` when replying.  This module owns that connection
in a singleton gateway process; it never exposes a public callback endpoint.

Only normalized messages leave this module.  The supplied inbound handler is
expected to durably accept a message before returning, while the normal
Inbox/Outbox workers produce replies later through :class:`AIBotRegistry`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from trpc_service.config import SecretProvider, parse_secret_json
from trpc_service.metrics import current_trace_context

from .adapters import (
    Attachment,
    CallbackRequest,
    ChannelBinding,
    ChannelError,
    DeliveryCapability,
    DeliveryResult,
    InboundEnvelope,
    Principal,
    ReplyEnvelope,
    _stable_id,
    deterministic_session_id,
)

DEFAULT_WS_URL = "wss://openws.work.weixin.qq.com"
_AUTH_COMMAND = "aibot_subscribe"
_HEARTBEAT_COMMAND = "ping"
_CALLBACK_COMMAND = "aibot_msg_callback"
_EVENT_COMMAND = "aibot_event_callback"
_REPLY_COMMAND = "aibot_respond_msg"
_PEER_DISCONNECTED_EVENT = "disconnected_event"

LOGGER = logging.getLogger(__name__)


class AIBotSocket(Protocol):
    """The small WebSocket surface used by the transport and fake test clients."""

    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


AIBotConnector = Callable[[str], Awaitable[AIBotSocket]]
AIBotInboundHandler = Callable[[ChannelBinding, CallbackRequest], Awaitable[None]]


async def default_connector(url: str) -> AIBotSocket:
    """Open the production client lazily so unit tests require no network client."""

    from websockets.asyncio.client import connect

    return await connect(url, ping_interval=None, close_timeout=3)


def _frame(value: str | bytes) -> Mapping[str, Any]:
    raw = value.decode("utf-8") if isinstance(value, bytes) else value
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ChannelError("invalid_aibot_frame", "WeCom Smart Bot sent invalid JSON") from exc
    if not isinstance(parsed, dict):
        raise ChannelError("invalid_aibot_frame", "WeCom Smart Bot frame must be an object")
    return parsed


def _header(frame: Mapping[str, Any], name: str) -> str:
    headers = frame.get("headers")
    if not isinstance(headers, Mapping):
        return ""
    value = headers.get(name)
    return str(value) if value is not None else ""


def _timestamp(value: object, fallback: datetime) -> datetime:
    try:
        seconds = float(value)
        # Smart Bot timestamps are normally seconds, but accept the millisecond
        # form used by some event producers without producing 1970 sessions.
        if seconds > 10_000_000_000:
            seconds /= 1000
        return datetime.fromtimestamp(seconds, UTC)
    except (TypeError, ValueError, OSError):
        return fallback


def _message_text(body: Mapping[str, Any]) -> str | None:
    message_type = str(body.get("msgtype") or "")
    if message_type in {"text", "voice"}:
        section = body.get(message_type)
        if isinstance(section, Mapping) and section.get("content") is not None:
            return str(section["content"])
    if message_type == "mixed":
        mixed = body.get("mixed")
        items = mixed.get("msg_item") if isinstance(mixed, Mapping) else None
        if isinstance(items, list):
            texts = [
                str(item["text"].get("content", ""))
                for item in items
                if isinstance(item, Mapping)
                and item.get("msgtype") == "text"
                and isinstance(item.get("text"), Mapping)
            ]
            return "\n".join(text for text in texts if text) or None
    return None


def _attachments(body: Mapping[str, Any]) -> tuple[Attachment, ...]:
    message_type = str(body.get("msgtype") or "")
    if message_type in {"image", "file", "video"}:
        section = body.get(message_type)
        if isinstance(section, Mapping):
            return (Attachment(kind=message_type, url=str(section.get("url") or "") or None),)
    if message_type == "mixed":
        mixed = body.get("mixed")
        items = mixed.get("msg_item") if isinstance(mixed, Mapping) else None
        if isinstance(items, list):
            return tuple(
                Attachment(kind="image", url=str(item["image"].get("url") or "") or None)
                for item in items
                if isinstance(item, Mapping)
                and item.get("msgtype") == "image"
                and isinstance(item.get("image"), Mapping)
            )
    return ()


class WeComAIBotAdapter:
    """Normalize Smart Bot frames and deliver a durable reply via its live socket."""

    provider = "wecom_aibot"

    def __init__(self, registry: AIBotRegistry) -> None:
        self._registry = registry

    async def validate_and_normalize(
        self, binding: ChannelBinding, request: CallbackRequest
    ) -> InboundEnvelope:
        binding.require_active(self.provider)
        if not isinstance(request.body, Mapping):
            raise ChannelError("invalid_aibot_frame", "Smart Bot frame must be an object")
        frame = request.body
        if str(frame.get("cmd") or "") != _CALLBACK_COMMAND:
            raise ChannelError("unsupported_aibot_frame", "Smart Bot frame is not a message callback")
        request_id = str(request.headers.get("x-wecom-aibot-request-id") or "")
        if not request_id:
            raise ChannelError("invalid_aibot_frame", "Smart Bot callback is missing req_id")
        body = frame.get("body")
        if not isinstance(body, Mapping):
            raise ChannelError("invalid_aibot_frame", "Smart Bot callback body is missing")
        message_id = str(body.get("msgid") or "")
        bot_id = str(body.get("aibotid") or "")
        sender = body.get("from")
        principal_id = str(sender.get("userid") or "") if isinstance(sender, Mapping) else ""
        if not message_id or not bot_id or not principal_id:
            raise ChannelError(
                "invalid_aibot_message", "Smart Bot message requires msgid, aibotid, and sender"
            )
        if bot_id != binding.external_account_id:
            raise ChannelError("aibot_binding_mismatch", "Smart Bot ID does not match this binding")
        conversation_type = "direct" if str(body.get("chattype") or "single") == "single" else "group"
        conversation_id = (
            principal_id if conversation_type == "direct" else str(body.get("chatid") or "")
        )
        if not conversation_id:
            raise ChannelError("invalid_aibot_message", "group Smart Bot message is missing chatid")
        trace = current_trace_context()
        message_type = str(body.get("msgtype") or "unknown")
        return InboundEnvelope(
            event_id=_stable_id("evt", self.provider, binding.external_account_id, message_id),
            idempotency_key=f"wecom_aibot:{binding.external_account_id}:{message_id}",
            tenant_id=binding.tenant_id,
            binding_id=binding.binding_id,
            agent_id=binding.agent_id,
            channel=self.provider,
            channel_account_id=binding.external_account_id,
            external_message_id=message_id,
            conversation_id=conversation_id,
            conversation_type=conversation_type,
            principal=Principal(external_user_id=principal_id),
            text=_message_text(body),
            attachments=_attachments(body),
            occurred_at=_timestamp(body.get("create_time"), request.received_at),
            session_id=deterministic_session_id(
                binding,
                conversation_id=conversation_id,
                conversation_type=conversation_type,
                principal_id=principal_id,
            ),
            traceparent=trace.traceparent,
            raw_type=message_type,
            transport_context={"wecom_aibot_request_id": request_id},
        )

    async def deliver(self, binding: ChannelBinding, reply: ReplyEnvelope) -> DeliveryResult:
        binding.require_active(self.provider)
        request_id = str(reply.metadata.get("wecom_aibot_request_id") or "")
        if not request_id:
            return DeliveryResult(
                "failed", DeliveryCapability.NON_RETRIABLE, error_code="aibot_request_id_unavailable"
            )
        outcome = await self._registry.reply(
            binding.binding_id,
            request_id,
            stream_id=reply.delivery_id,
            text=reply.plain_text(2048),
        )
        if outcome.status != "accepted":
            return outcome
        return DeliveryResult(
            "accepted",
            DeliveryCapability.NON_RETRIABLE,
            provider_message_id=outcome.provider_message_id or reply.delivery_id,
        )


class AIBotRegistry:
    """In-process registry shared only by the singleton Smart Bot gateway."""

    def __init__(self) -> None:
        self._connections: dict[str, WeComAIBotConnection] = {}

    def register(self, binding_id: str, connection: WeComAIBotConnection) -> None:
        self._connections[binding_id] = connection

    def unregister(self, binding_id: str, connection: WeComAIBotConnection) -> None:
        if self._connections.get(binding_id) is connection:
            self._connections.pop(binding_id, None)

    async def reply(
        self, binding_id: str, request_id: str, *, stream_id: str, text: str
    ) -> DeliveryResult:
        connection = self._connections.get(binding_id)
        if connection is None or not connection.authenticated:
            return DeliveryResult(
                "failed", DeliveryCapability.NON_RETRIABLE, error_code="aibot_connection_unavailable"
            )
        return await connection.reply(request_id, stream_id=stream_id, text=text)


@dataclass(slots=True)
class WeComAIBotConnection:
    """One authenticated Smart Bot socket with ack-aware reply delivery."""

    binding: ChannelBinding
    secrets_provider: SecretProvider
    registry: AIBotRegistry
    on_inbound: AIBotInboundHandler
    connector: AIBotConnector = default_connector
    ws_url: str = DEFAULT_WS_URL
    heartbeat_seconds: float = 30.0
    reply_ack_seconds: float = 5.0
    reconnect_max_seconds: float = 30.0
    max_auth_failures: int = 5
    _stop: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _socket: AIBotSocket | None = field(default=None, init=False)
    _authenticated: bool = field(default=False, init=False)
    _send_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _pending: dict[str, asyncio.Future[Mapping[str, Any]]] = field(default_factory=dict, init=False)

    @property
    def authenticated(self) -> bool:
        return self._authenticated and self._socket is not None and not self._stop.is_set()

    async def stop(self) -> None:
        self._stop.set()
        await self._close_socket()

    async def run(self) -> None:
        reconnect_attempt = 0
        auth_failures = 0
        while not self._stop.is_set():
            heartbeat: asyncio.Task[None] | None = None
            try:
                credentials = await self._credentials()
                socket = await self.connector(self.ws_url)
                self._socket = socket
                auth_request_id = self._request_id(_AUTH_COMMAND)
                await self._send(
                    {
                        "cmd": _AUTH_COMMAND,
                        "headers": {"req_id": auth_request_id},
                        "body": credentials,
                    }
                )
                auth = _frame(await asyncio.wait_for(socket.recv(), timeout=self.reply_ack_seconds))
                if _header(auth, "req_id") != auth_request_id or int(auth.get("errcode", -1)) != 0:
                    raise _AuthenticationFailure(str(auth.get("errmsg") or "Smart Bot authentication failed"))
                auth_failures = 0
                reconnect_attempt = 0
                self._authenticated = True
                self.registry.register(self.binding.binding_id, self)
                LOGGER.info("WeCom Smart Bot connection authenticated for binding=%s", self.binding.binding_id)
                heartbeat = asyncio.create_task(self._heartbeat(), name=f"aibot-heartbeat:{self.binding.binding_id}")
                await self._receive_loop(socket)
            except asyncio.CancelledError:
                raise
            except _PeerSuperseded:
                LOGGER.warning(
                    "WeCom Smart Bot binding=%s was superseded by another connection; stopping",
                    self.binding.binding_id,
                )
                return
            except _AuthenticationFailure as exc:
                auth_failures += 1
                LOGGER.warning(
                    "WeCom Smart Bot authentication failed for binding=%s (attempt=%s/%s): %s",
                    self.binding.binding_id,
                    auth_failures,
                    self.max_auth_failures,
                    exc,
                )
                if auth_failures >= self.max_auth_failures:
                    LOGGER.error(
                        "WeCom Smart Bot binding=%s stopped after repeated authentication failures",
                        self.binding.binding_id,
                    )
                    return
            except Exception as exc:  # A transient network error should reconnect, never drop Inbox data.
                if self._stop.is_set():
                    return
                reconnect_attempt += 1
                LOGGER.warning(
                    "WeCom Smart Bot binding=%s disconnected (%s); reconnecting",
                    self.binding.binding_id,
                    type(exc).__name__,
                )
            finally:
                self._authenticated = False
                self.registry.unregister(self.binding.binding_id, self)
                if heartbeat is not None:
                    heartbeat.cancel()
                    await _cancel(heartbeat)
                self._fail_pending()
                await self._close_socket()
            if not self._stop.is_set():
                delay = min(2 ** max(reconnect_attempt - 1, 0), self.reconnect_max_seconds)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except TimeoutError:
                    pass

    async def reply(self, request_id: str, *, stream_id: str, text: str) -> DeliveryResult:
        if not self.authenticated:
            return DeliveryResult(
                "failed", DeliveryCapability.NON_RETRIABLE, error_code="aibot_connection_unavailable"
            )
        loop = asyncio.get_running_loop()
        acknowledgement: asyncio.Future[Mapping[str, Any]] = loop.create_future()
        if request_id in self._pending:
            return DeliveryResult(
                "unknown", DeliveryCapability.NON_RETRIABLE, error_code="aibot_reply_already_pending"
            )
        self._pending[request_id] = acknowledgement
        try:
            await self._send(
                {
                    "cmd": _REPLY_COMMAND,
                    "headers": {"req_id": request_id},
                    "body": {
                        "msgtype": "stream",
                        "stream": {"id": stream_id, "finish": True, "content": text},
                    },
                }
            )
        except Exception:
            self._pending.pop(request_id, None)
            return DeliveryResult(
                "failed", DeliveryCapability.NON_RETRIABLE, error_code="aibot_reply_not_sent"
            )
        try:
            ack = await asyncio.wait_for(asyncio.shield(acknowledgement), timeout=self.reply_ack_seconds)
        except TimeoutError:
            self._pending.pop(request_id, None)
            return DeliveryResult(
                "unknown", DeliveryCapability.NON_RETRIABLE, error_code="aibot_reply_ack_timeout"
            )
        except _ConnectionLost:
            return DeliveryResult(
                "unknown", DeliveryCapability.NON_RETRIABLE, error_code="aibot_connection_lost"
            )
        finally:
            self._pending.pop(request_id, None)
        if int(ack.get("errcode", -1)) != 0:
            return DeliveryResult(
                "failed",
                DeliveryCapability.NON_RETRIABLE,
                error_code=str(ack.get("errcode") or "aibot_reply_rejected"),
            )
        body = ack.get("body")
        provider_message_id = str(body.get("msgid") or "") if isinstance(body, Mapping) else ""
        return DeliveryResult(
            "accepted", DeliveryCapability.NON_RETRIABLE, provider_message_id=provider_message_id or None
        )

    async def _credentials(self) -> Mapping[str, str]:
        raw = await self.secrets_provider.get(self.binding.secret_ref)
        try:
            parsed = parse_secret_json(raw)
        except ValueError as exc:
            raise _AuthenticationFailure("Smart Bot secret must be a JSON object") from exc
        bot_id = str(parsed.get("bot_id") or self.binding.external_account_id)
        secret = str(parsed.get("secret") or "")
        if not secret or bot_id != self.binding.external_account_id:
            raise _AuthenticationFailure("Smart Bot credentials are unavailable or do not match binding")
        return {"bot_id": bot_id, "secret": secret}

    async def _receive_loop(self, socket: AIBotSocket) -> None:
        while not self._stop.is_set():
            frame = _frame(await socket.recv())
            command = str(frame.get("cmd") or "")
            request_id = _header(frame, "req_id")
            if command == _CALLBACK_COMMAND:
                try:
                    await self.on_inbound(
                        self.binding,
                        CallbackRequest(
                            body=frame,
                            headers={"x-wecom-aibot-request-id": request_id},
                        ),
                    )
                except ChannelError as exc:
                    # A malformed provider frame cannot become valid after a
                    # reconnect.  Leave the authenticated socket healthy for
                    # following messages without logging its user content.
                    LOGGER.warning(
                        "WeCom Smart Bot rejected callback binding=%s code=%s",
                        self.binding.binding_id,
                        exc.code,
                    )
                continue
            if command == _EVENT_COMMAND:
                body = frame.get("body")
                event = body.get("event") if isinstance(body, Mapping) else None
                if isinstance(event, Mapping) and event.get("eventtype") == _PEER_DISCONNECTED_EVENT:
                    raise _PeerSuperseded()
                continue
            pending = self._pending.get(request_id)
            if pending is not None and not pending.done():
                pending.set_result(frame)

    async def _heartbeat(self) -> None:
        missed_acks = 0
        while not self._stop.is_set() and self._socket is not None:
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.heartbeat_seconds)
                return
            except TimeoutError:
                pass
            if missed_acks >= 2:
                await self._close_socket()
                return
            request_id = self._request_id(_HEARTBEAT_COMMAND)
            loop = asyncio.get_running_loop()
            acknowledgement: asyncio.Future[Mapping[str, Any]] = loop.create_future()
            self._pending[request_id] = acknowledgement
            try:
                await self._send({"cmd": _HEARTBEAT_COMMAND, "headers": {"req_id": request_id}})
                await asyncio.wait_for(asyncio.shield(acknowledgement), timeout=self.heartbeat_seconds)
                missed_acks = 0
            except (TimeoutError, _ConnectionLost):
                missed_acks += 1
            finally:
                self._pending.pop(request_id, None)

    async def _send(self, frame: Mapping[str, Any]) -> None:
        socket = self._socket
        if socket is None:
            raise _ConnectionLost()
        async with self._send_lock:
            await socket.send(json.dumps(frame, ensure_ascii=False, separators=(",", ":")))

    def _fail_pending(self) -> None:
        for pending in self._pending.values():
            if not pending.done():
                pending.set_exception(_ConnectionLost())
        self._pending.clear()

    async def _close_socket(self) -> None:
        socket, self._socket = self._socket, None
        if socket is not None:
            try:
                await socket.close()
            except Exception:
                pass

    @staticmethod
    def _request_id(prefix: str) -> str:
        return f"{prefix}_{secrets.token_hex(12)}"


@dataclass(slots=True)
class WeComAIBotSupervisor:
    """Start one reconnecting connection task for every active Smart Bot binding."""

    bindings: Callable[[], list[ChannelBinding]]
    secrets_provider: SecretProvider
    registry: AIBotRegistry
    on_inbound: AIBotInboundHandler
    connector: AIBotConnector = default_connector
    refresh_seconds: float = 30.0
    _connections: dict[str, WeComAIBotConnection] = field(default_factory=dict, init=False)
    _tasks: dict[str, asyncio.Task[None]] = field(default_factory=dict, init=False)
    _stop: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _refresh_task: asyncio.Task[None] | None = field(default=None, init=False)

    async def start(self) -> None:
        await self.refresh()
        self._refresh_task = asyncio.create_task(self._refresh_loop(), name="aibot-binding-refresh")

    async def close(self) -> None:
        self._stop.set()
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            await _cancel(self._refresh_task)
        for connection in self._connections.values():
            await connection.stop()
        for task in self._tasks.values():
            task.cancel()
        for task in self._tasks.values():
            await _cancel(task)
        self._tasks.clear()
        self._connections.clear()

    async def refresh(self) -> None:
        for binding in self.bindings():
            if binding.binding_id in self._tasks:
                continue
            ws_url = str(binding.capabilities.get("wecom_aibot_ws_url") or DEFAULT_WS_URL)
            if not ws_url.startswith("wss://"):
                LOGGER.error("WeCom Smart Bot binding=%s has a non-TLS WebSocket URL", binding.binding_id)
                continue
            connection = WeComAIBotConnection(
                binding=binding,
                secrets_provider=self.secrets_provider,
                registry=self.registry,
                on_inbound=self.on_inbound,
                connector=self.connector,
                ws_url=ws_url,
            )
            self._connections[binding.binding_id] = connection
            self._tasks[binding.binding_id] = asyncio.create_task(
                connection.run(), name=f"aibot:{binding.binding_id}"
            )

    async def _refresh_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.refresh_seconds)
                return
            except TimeoutError:
                await self.refresh()


class _AuthenticationFailure(RuntimeError):
    pass


class _ConnectionLost(RuntimeError):
    pass


class _PeerSuperseded(RuntimeError):
    pass


async def _cancel(task: asyncio.Task[Any]) -> None:
    try:
        await task
    except asyncio.CancelledError:
        pass


__all__ = [
    "AIBotRegistry",
    "DEFAULT_WS_URL",
    "WeComAIBotAdapter",
    "WeComAIBotConnection",
    "WeComAIBotSupervisor",
]
