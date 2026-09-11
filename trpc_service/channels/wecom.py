"""WeCom public WebSocket client integration; message frames never live in a per-chat cache."""

from datetime import datetime, timezone
import hashlib
from urllib.parse import urlsplit
import uuid

from wecom_aibot_sdk import WSClient
from wecom_aibot_sdk.types.api import WsFrame

from .base import CallbackVerificationError
from .events import (DeliveryResult, PendingMedia, TransportEvent, action_command,
                     safe_media_filename, media_mime, MAX_MEDIA_BYTES)
from .models import NormalizedInboundMessage, MessageType


class QuietProtocolLogger:
    """The dependency logs entire auth and message frames; only platform events are exported."""

    def debug(self, *args, **kwargs):
        pass

    info = debug
    warn = debug
    warning = debug
    error = debug


class WecomAdapter:

    text_limit = 20480
    text_unit = "utf8_bytes"
    rate_limit_errors = {45009}
    retryable_errors = {-1, 6000}

    def __init__(self, client: WSClient):
        self.client = client

    @classmethod
    def create(cls, bot_id: str, secret: str):
        return cls(
            WSClient({
                "bot_id": bot_id,
                "secret": secret,
                "logger": QuietProtocolLogger(),
                "max_reconnect_attempts": 1,
                "reconnect_interval": 60000
            }))

    def normalize(self, frame: WsFrame, binding, *, attachments=()) -> TransportEvent:
        if binding.channel.value != "wecom" or not self.client.is_authenticated:
            raise CallbackVerificationError("wecom connection is not authenticated")
        body = frame.body
        if not isinstance(body, dict) or body.get("aibotid") != binding.external_account_id:
            raise CallbackVerificationError("wecom message does not match authenticated account")
        try:
            event_id = body["msgid"]
            actor = body["from"]["userid"]
            chat = str(body.get("chatid") or actor)
            if not isinstance(event_id, str) or not event_id or not isinstance(actor, str) or not actor:
                raise ValueError("missing identity")
            context = {"req_id": frame.headers["req_id"], "chat_id": chat}
            if frame.cmd == "aibot_event_callback":
                event = body.get("event", {})
                if event.get("eventtype") == "template_card_event" and event.get("event_key"):
                    return TransportEvent(event_id=event_id,
                                          kind="action",
                                          external_actor_id=actor,
                                          external_chat_id=chat,
                                          action_data=event["event_key"],
                                          reply_context=context)
                return TransportEvent(event_id=event_id, kind="ignored", reason="unsupported_event")
            message_type = body.get("msgtype")
            if frame.cmd != "aibot_msg_callback" or message_type not in {"text", "image", "file"}:
                return TransportEvent(event_id=event_id, kind="ignored", reason="unsupported_message")
            chat_type = body.get("chattype")
            if chat_type not in {"single", "group"} or (chat_type == "group" and not body.get("chatid")):
                raise ValueError("invalid conversation")
            pending = ()
            if message_type == "text":
                text = body["text"]["content"]
                if not isinstance(text, str) or not text.strip() or len(text) > 100000:
                    raise ValueError("invalid text")
            else:
                media = body[message_type]
                url, aes_key = media["url"], media["aeskey"]
                parsed = urlsplit(url)
                if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                        or parsed.fragment or not isinstance(aes_key, str) or not aes_key):
                    raise ValueError("invalid media reference")
                filename = safe_media_filename(media.get("name"), message_type)
                mime_type = media_mime(filename, "image/jpeg" if message_type == "image"
                                       else "application/octet-stream")
                pending = () if attachments else (PendingMedia(
                    kind=message_type,
                    resource_id=hashlib.sha256(url.encode()).hexdigest(),
                    filename=filename,
                    mime_type=mime_type,
                    download_url=url,
                    encryption_key=aes_key),)
                text = "" if attachments else f"[{message_type} attachment]"
            normalized = NormalizedInboundMessage(channel="wecom",
                                                  webhook_public_id=binding.webhook_public_id,
                                                  external_message_id=event_id,
                                                  external_user_id=actor,
                                                  external_chat_id=chat,
                                                  conversation_type="direct" if chat_type == "single" else "group",
                                                  message_type=MessageType(message_type),
                                                  text=text,
                                                  attachments=attachments,
                                                  received_at=datetime.now(timezone.utc),
                                                  request_id=uuid.uuid4().hex)
            return action_command(normalized, context) or TransportEvent(event_id=event_id,
                                                                         kind="chat",
                                                                         message=normalized,
                                                                         external_actor_id=actor,
                                                                         external_chat_id=chat,
                                                                         pending_media=pending,
                                                                         reply_context=context)
        except (KeyError, TypeError, ValueError):
            raise ValueError("invalid wecom event") from None

    async def download_media(self, pending, message_id):
        try:
            payload, provider_name = await self.client.download_file(pending.download_url, pending.encryption_key)
            if not isinstance(payload, bytes) or not payload or len(payload) > MAX_MEDIA_BYTES:
                raise ValueError("WeCom media size is invalid")
            filename = safe_media_filename(provider_name or pending.filename, pending.kind)
            return payload, filename, media_mime(filename, pending.mime_type)
        except ValueError:
            raise
        except Exception:
            raise RuntimeError("WeCom media download failed") from None

    async def send_text(self, context: dict, text: str, *, stream_id: str, final: bool = True,
                        part_no: int = 0) -> DeliveryResult:
        if not self.client.is_authenticated:
            return DeliveryResult(outcome="rejected", error_type="not_connected", retryable=True)
        try:
            if part_no == 0:
                reply = await self.client.reply_stream({"req_id": context["req_id"]}, stream_id, text, finish=final)
            else:
                reply = await self.client.send_message(context["chat_id"], {
                    "msgtype": "markdown",
                    "markdown": {"content": text}
                })
            if reply.errcode == 0:
                return DeliveryResult(outcome="accepted",
                                      receipt_kind="protocol_ack",
                                      provider_request_id=reply.headers.get("req_id"))
            if reply.errcode in self.rate_limit_errors:
                return DeliveryResult(outcome="rejected", error_type="rate_limited", retryable=True, retry_after=2)
            if reply.errcode in self.retryable_errors:
                return DeliveryResult(outcome="rejected", error_type="provider_unavailable", retryable=True)
            return DeliveryResult(outcome="rejected", error_type="platform_rejected")
        except Exception:
            # The SDK embeds raw protocol data in exceptions. A lost ACK is not a safe retry signal.
            return DeliveryResult(outcome="unknown", error_type="transport_outcome_unknown")

    async def close(self):
        await self.client.disconnect()
