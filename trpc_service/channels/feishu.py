"""Self-built Feishu app: authenticated WS ingress and durable plain-text replies.

The official SDK owns WS framing/ACKs. Its public blocking start() runs in an
owned process; no SDK internals or global event loops are patched.
"""

import asyncio
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import logging
import multiprocessing
import re
import time
import uuid

import httpx

from .base import CallbackVerificationError
from .events import (TransportEvent, DeliveryResult, PendingMedia, action_command,
                     safe_media_filename, media_mime, MAX_MEDIA_BYTES)
from .models import NormalizedInboundMessage, MessageType

API = "https://open.feishu.cn/open-apis"
TEXT_BYTES = 3000


def unsupported_media_reason(value):
    """Expose only Feishu's bounded type token, never message content."""
    if isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,31}", value):
        return "unsupported_media_" + value
    return "unsupported_media"


def retry_delay(value, *, now=None):
    """Parse Retry-After seconds or an HTTP date and keep retries bounded."""
    if value is None:
        return 2
    try:
        delay = float(value)
    except (TypeError, ValueError):
        try:
            target = parsedate_to_datetime(value)
            current = now or datetime.now(timezone.utc)
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            delay = (target - current).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return 2
    return min(max(delay, 0), 3600)


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,120}", value):
        raise CallbackVerificationError("invalid Feishu identity")
    return value


class LifecycleLogHandler(logging.Handler):
    """Export only lifecycle markers, never signed URLs, tokens or payloads."""

    def __init__(self, connection):
        super().__init__()
        self.connection = connection

    def emit(self, record):
        if record.funcName == "_connect" and "connected to " in str(record.msg):
            self.connection.send({"kind": "connected"})
        elif record.funcName == "_receive_message_loop" and "loop exit" in str(record.msg):
            self.connection.send({"kind": "failed"})
        elif record.funcName == "_handle_data_frame" and "receive message," in str(record.msg):
            self.connection.send({"kind": "protocol_frame"})
        elif record.funcName == "_handle_control_frame" and "receive pong" in str(record.msg):
            self.connection.send({"kind": "heartbeat"})


def commit_callback(connection, payload, timeout=15):
    """Return to the SDK (which ACKs) only after the parent commits ingress."""
    nonce = uuid.uuid4().hex
    connection.send({"kind": "event", "nonce": nonce, "payload": payload})
    if not connection.poll(timeout):
        connection.send({"kind": "failed"})
        raise RuntimeError("ingress acknowledgement timeout")
    ack = connection.recv()
    if ack != {"nonce": nonce, "committed": True}:
        connection.send({"kind": "failed"})
        raise RuntimeError("ingress not committed")


def websocket_process(connection, app_id, app_secret):
    try:
        import lark_oapi as lark
        logger = logging.getLogger("Lark")
        logger.handlers.clear()
        logger.propagate = False
        logger.addHandler(LifecycleLogHandler(connection))

        def receive(event):
            # Select known protocol fields; header.token never crosses IPC.
            frame = json.loads(lark.JSON.marshal(event))
            header = frame.get("header") or {}
            frame["header"] = {key: header.get(key) for key in ("app_id", "tenant_key", "event_type", "event_id")}
            commit_callback(connection, frame)

        handler = (lark.EventDispatcherHandler.builder("", "")
                   .register_p2_im_message_receive_v1(receive)
                   .register_p2_im_message_recalled_v1(receive)
                   .build())
        client = lark.ws.Client(app_id,
                                app_secret,
                                event_handler=handler,
                                log_level=lark.LogLevel.DEBUG,
                                auto_reconnect=False)
        client.start()
    except BaseException:
        try:
            connection.send({"kind": "failed"})
        except (BrokenPipeError, EOFError, OSError):
            pass
    finally:
        connection.close()


class FeishuAdapter:

    text_limit = TEXT_BYTES
    text_unit = "utf8_bytes"
    rate_limit_codes = {99991400, 99991403}

    def __init__(self, app_id, app_secret, *, transport=None):
        self.app_id, self._secret = identifier(app_id), app_secret
        self.http = httpx.AsyncClient(timeout=10, follow_redirects=False, transport=transport)
        self._token, self._expires = None, 0
        self._auth_lock = asyncio.Lock()
        self.bot_open_id = None
        self._process = self._connection = None
        self.diagnostics = {
            "connections": 0,
            "heartbeats": 0,
            "protocol_frames": 0,
            "message_events": 0,
            "chat_events": 0,
            "recall_events": 0,
            "ignored_events": 0
        }

    async def token(self):
        async with self._auth_lock:
            if self._token and time.monotonic() < self._expires:
                return self._token
            try:
                response = await self.http.post(API + "/auth/v3/tenant_access_token/internal",
                                                json={
                                                    "app_id": self.app_id,
                                                    "app_secret": self._secret
                                                })
                data = response.json()
                token, expire = data.get("tenant_access_token"), data.get("expire")
                if (response.status_code != 200 or data.get("code") != 0 or not isinstance(token, str) or not token
                        or type(expire) is not int or expire <= 60):
                    raise ValueError()
                self._token, self._expires = token, time.monotonic() + expire - 60
                return token
            except Exception:
                raise CallbackVerificationError("Feishu authentication failed") from None

    async def initialize(self):
        try:
            response = await self.http.get(API + "/bot/v3/info",
                                           headers={"Authorization": "Bearer " + await self.token()})
            data = response.json()
            bot = data.get("bot") or {}
            # Some official bot-info responses omit app_id; the internal token
            # was already acquired with this app's ID + secret.
            if response.status_code != 200 or data.get("code") != 0 or bot.get("app_id", self.app_id) != self.app_id:
                raise ValueError()
            self.bot_open_id = identifier(bot.get("open_id"))
        except Exception:
            raise CallbackVerificationError("Feishu bot identity could not be verified") from None

    def normalize(self, frame, binding, *, attachments=()):
        # Only receive() may feed this boundary in a running service. HTTP
        # callbacks are intentionally not exposed without signature validation.
        header = frame.get("header") or {}
        if (binding.channel.value != "feishu" or binding.external_account_id != self.app_id
                or header.get("app_id") != self.app_id):
            raise CallbackVerificationError("Feishu account mismatch")
        tenant_key = identifier(header.get("tenant_key"))
        body = frame.get("event") or {}
        if header.get("event_type") == "im.message.recalled_v1":
            event_id = identifier(header.get("event_id"))
            chat_id = identifier(body.get("chat_id"))
            return TransportEvent(event_id=event_id,
                                  kind="recall",
                                  recalled_message_id=identifier(body.get("message_id")),
                                  external_chat_id=tenant_key + ":" + chat_id)
        sender, raw = body.get("sender") or {}, body.get("message") or {}
        event_id = identifier(raw.get("message_id") or header.get("event_id"))

        def ignored(reason):
            return TransportEvent(event_id=event_id, kind="ignored", reason=reason)

        if header.get("event_type") != "im.message.receive_v1":
            return ignored("unsupported_event")
        if sender.get("sender_type") != "user":
            return ignored("non_user_sender")
        if sender.get("tenant_key") != tenant_key:
            raise CallbackVerificationError("Feishu enterprise mismatch")
        actor = tenant_key + ":" + identifier((sender.get("sender_id") or {}).get("open_id"))
        chat_id = identifier(raw.get("chat_id"))
        chat = tenant_key + ":" + chat_id
        message_type = raw.get("message_type")
        if message_type not in {"text", "image", "file"}:
            return ignored(unsupported_media_reason(message_type))
        if raw.get("chat_type") not in {"p2p", "group"}:
            return ignored("unsupported_conversation")
        try:
            content = json.loads(raw.get("content", ""))
        except (ValueError, AttributeError, TypeError):
            return ignored("invalid_content")
        pending = ()
        if message_type == "text":
            text = content.get("text")
            if not isinstance(text, str) or not text.strip() or len(text) > 100000:
                return ignored("invalid_text")
        else:
            key_name = "image_key" if message_type == "image" else "file_key"
            try:
                resource_id = identifier(content.get(key_name))
            except CallbackVerificationError:
                return ignored("invalid_media")
            filename = safe_media_filename(content.get("file_name"), message_type)
            mime_type = media_mime(filename, "image/jpeg" if message_type == "image" else "application/octet-stream")
            pending = () if attachments else (
                PendingMedia(kind=message_type,
                             resource_id=resource_id,
                             filename=filename,
                             mime_type=mime_type),
            )
            text = "" if attachments else f"[{message_type} attachment]"
        group = raw["chat_type"] == "group"
        native_thread = raw.get("thread_id") if group else None
        reply_root = (raw.get("root_id") or raw.get("parent_id")) if group else None
        if group:
            mentions = [
                item for item in raw.get("mentions") or []
                if self.bot_open_id and (item.get("id") or {}).get("open_id") == self.bot_open_id
                and (message_type != "text" or
                     isinstance(item.get("key"), str) and item["key"] and item["key"] in text)
            ]
            if message_type == "text" and not mentions and not (native_thread or reply_root):
                return ignored("bot_not_mentioned")
            if message_type == "text":
                for mention in mentions:
                    text = text.replace(mention["key"], "")
                text = text.strip()
                if not text:
                    return ignored("empty_mention")
        thread = native_thread or reply_root
        if thread:
            thread = identifier(thread)
        reply = {
            "app_id": self.app_id,
            "tenant_key": tenant_key,
            "chat_id": chat_id,
            "message_id": identifier(raw.get("message_id")),
            "thread_id": thread,
            "reply_in_thread": bool(native_thread)
        }
        message = NormalizedInboundMessage(channel="feishu",
                                           webhook_public_id=binding.webhook_public_id,
                                           external_message_id=event_id,
                                           external_user_id=actor,
                                           external_chat_id=chat,
                                           conversation_type="thread" if thread else "group" if group else "direct",
                                           message_type=MessageType(message_type),
                                           thread_id=thread,
                                           text=text,
                                           attachments=attachments,
                                           received_at=datetime.now(timezone.utc),
                                           request_id=event_id)
        return action_command(message, reply) or TransportEvent(event_id=event_id,
                                                                kind="chat",
                                                                message=message,
                                                                external_actor_id=actor,
                                                                external_chat_id=chat,
                                                                pending_media=pending,
                                                                reply_context=reply)

    async def download_media(self, pending, message_id):
        url = (API + "/im/v1/messages/" + identifier(message_id)
               + "/resources/" + identifier(pending.resource_id))
        headers = {"Authorization": "Bearer " + await self.token()}
        try:
            async with self.http.stream("GET", url, params={"type": pending.kind}, headers=headers) as response:
                if response.status_code == 429:
                    raise RuntimeError("Feishu media download rate limited")
                if response.status_code != 200:
                    raise ValueError("Feishu media download rejected")
                declared = response.headers.get("Content-Length")
                if declared and declared.isdigit() and int(declared) > MAX_MEDIA_BYTES:
                    raise ValueError("Feishu media exceeds size limit")
                chunks, size = [], 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > MAX_MEDIA_BYTES:
                        raise ValueError("Feishu media exceeds size limit")
                    chunks.append(chunk)
                if not size:
                    raise ValueError("Feishu media is empty")
                fallback = response.headers.get("Content-Type", pending.mime_type).split(";", 1)[0]
                return b"".join(chunks), pending.filename, media_mime(pending.filename, fallback)
        except (CallbackVerificationError, RuntimeError, ValueError):
            raise
        except Exception:
            raise RuntimeError("Feishu media download failed") from None

    async def receive(self, binding, callback, ready=None, *, transform=None, on_ready=None):
        if self._process is not None:
            raise RuntimeError("Feishu connection already owned")
        context = multiprocessing.get_context("spawn")
        self._connection, child = context.Pipe()
        self._process = context.Process(target=websocket_process, args=(child, self.app_id, self._secret))
        self._process.start()
        child.close()
        connected, deadline = False, time.monotonic() + 30
        try:
            while True:
                if self._connection.poll():
                    item = self._connection.recv()
                    if item["kind"] == "connected":
                        self.diagnostics["connections"] += 1
                        connected = True
                        if ready:
                            ready.set()
                        if on_ready:
                            on_ready()
                    elif item["kind"] == "event":
                        self.diagnostics["message_events"] += 1
                        try:
                            if not connected:
                                raise CallbackVerificationError("Feishu connection not authenticated")
                            event = self.normalize(item["payload"], binding)
                            if transform:
                                event = await transform(event)
                            if event.kind in {"chat", "recall", "ignored"}:
                                self.diagnostics[event.kind + "_events"] += 1
                            await asyncio.wait_for(callback(event), 1.5)
                        except BaseException:
                            self._connection.send({"nonce": item["nonce"], "committed": False})
                            raise
                        self._connection.send({"nonce": item["nonce"], "committed": True})
                    elif item["kind"] == "protocol_frame":
                        self.diagnostics["protocol_frames"] += 1
                    elif item["kind"] == "heartbeat":
                        self.diagnostics["heartbeats"] += 1
                    else:
                        raise ConnectionError("Feishu connection lost")
                elif not self._process.is_alive() or (not connected and time.monotonic() > deadline):
                    raise ConnectionError("Feishu connection unavailable")
                else:
                    await asyncio.sleep(0.02)
        finally:
            self.stop_connection()

    def stop_connection(self):
        if self._process is not None:
            if self._process.is_alive():
                self._process.terminate()
            self._process.join(2)
            if self._process.is_alive():
                self._process.kill()
                self._process.join(2)
            if self._process.is_alive():
                raise RuntimeError("Feishu connection process could not be stopped")
            self._process.close()
            self._process = None
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    async def send_text(self, context, text, *, idempotency_key):
        try:
            if context.get("app_id") != self.app_id or not text or len(text.encode()) > TEXT_BYTES:
                raise ValueError()
            chat_id = identifier(context.get("chat_id"))
            identifier(context.get("tenant_key"))
            identifier(idempotency_key)
            body = {
                "msg_type": "text",
                "content": json.dumps({"text": text}, ensure_ascii=False),
                "uuid": uuid.uuid5(uuid.NAMESPACE_URL, self.app_id + ":" + idempotency_key).hex
            }
            if context.get("thread_id"):
                url = API + "/im/v1/messages/" + identifier(context.get("message_id")) + "/reply"
                if context.get("reply_in_thread", True) is True:
                    body["reply_in_thread"] = True
                params = None
            else:
                url, params = API + "/im/v1/messages", {"receive_id_type": "chat_id"}
                body["receive_id"] = chat_id
        except (ValueError, TypeError):
            return DeliveryResult(outcome="rejected", error_type="invalid_reply_context")
        try:
            token = await self.token()
        except CallbackVerificationError:
            return DeliveryResult(outcome="rejected", error_type="authentication_failed", retryable=True)
        try:
            response = await self.http.post(url, params=params, json=body, headers={"Authorization": "Bearer " + token})
            if response.status_code == 429:
                return DeliveryResult(outcome="rejected", error_type="rate_limited", retryable=True,
                                      retry_after=retry_delay(response.headers.get("Retry-After")))
            if response.status_code >= 500:
                return DeliveryResult(outcome="unknown", error_type="provider_outcome_unknown")
            data = response.json()
            if response.status_code == 200 and type(data.get("code")) is int and data["code"] == 0:
                message_id = identifier((data.get("data") or {}).get("message_id"))
                return DeliveryResult(outcome="accepted", receipt_kind="message_id", external_message_id=message_id)
            if data.get("code") in self.rate_limit_codes:
                return DeliveryResult(outcome="rejected", error_type="rate_limited", retryable=True,
                                      retry_after=retry_delay(response.headers.get("Retry-After")))
            if response.status_code in {401, 403}:
                self._expires = 0
            if type(data.get("code")) is int and data["code"] != 0:
                return DeliveryResult(outcome="rejected", error_type="provider_rejected")
        except Exception:
            pass
        return DeliveryResult(outcome="unknown", error_type="transport_outcome_unknown")

    async def close(self):
        self.stop_connection()
        self._token, self._secret = None, None
        await self.http.aclose()
