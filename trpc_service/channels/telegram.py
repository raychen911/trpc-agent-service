"""Telegram's public Bot client, with durable ingestion owned by the platform."""

from datetime import datetime, timezone
import hmac
import uuid

from telegram import Bot, Update
from telegram.error import BadRequest, Forbidden, InvalidToken, NetworkError, RetryAfter, TelegramError

from .base import CallbackVerificationError
from .events import DeliveryResult, TransportEvent, action_command
from .models import NormalizedInboundMessage


class TelegramAdapter:

    text_limit = 4096
    text_unit = "characters"

    def __init__(self, bot: Bot):
        self.bot = bot

    @staticmethod
    def verify_webhook(supplied: str | None, expected: str):
        if not expected or not supplied or not hmac.compare_digest(supplied.encode(), expected.encode()):
            raise CallbackVerificationError("telegram webhook authentication failed")

    def normalize(self, raw: dict, binding) -> TransportEvent:
        if binding.channel.value != "telegram":
            raise CallbackVerificationError("binding is not a telegram account")
        try:
            update = Update.de_json(raw, self.bot)
            event_id = str(update.update_id)
            if update.update_id is None:
                raise ValueError("missing update id")
            callback = update.callback_query
            if callback:
                actor = callback.from_user
                message = callback.message
                if actor.is_bot or message is None or not callback.data:
                    return TransportEvent(event_id=event_id, kind="ignored", reason="unsupported_action_origin")
                return TransportEvent(event_id=event_id,
                                      kind="action",
                                      external_actor_id=str(actor.id),
                                      external_chat_id=str(message.chat.id),
                                      action_data=callback.data,
                                      reply_context={
                                          "callback_query_id": callback.id,
                                          "chat_id": str(message.chat.id),
                                          "message_id": message.message_id,
                                          "thread_id": getattr(message, "message_thread_id", None)
                                      })
            message = update.message
            if message is None:
                return TransportEvent(event_id=event_id, kind="ignored", reason="unsupported_event")
            actor = message.from_user
            if actor is None or actor.is_bot or message.sender_chat is not None:
                return TransportEvent(event_id=event_id, kind="ignored", reason="unidentified_actor")
            if message.chat.type not in {"private", "group", "supergroup"}:
                return TransportEvent(event_id=event_id, kind="ignored", reason="unsupported_chat")
            if not message.text:
                return TransportEvent(event_id=event_id, kind="ignored", reason="media_not_enabled")
            topic = str(message.message_thread_id) if message.message_thread_id is not None else None
            kind = "thread" if topic else "direct" if message.chat.type == "private" else "group"
            normalized = NormalizedInboundMessage(channel="telegram",
                                                  webhook_public_id=binding.webhook_public_id,
                                                  external_message_id=event_id,
                                                  external_user_id=str(actor.id),
                                                  external_chat_id=str(message.chat.id),
                                                  thread_id=topic,
                                                  conversation_type=kind,
                                                  text=message.text,
                                                  received_at=datetime.now(timezone.utc),
                                                  request_id=uuid.uuid4().hex)
            context = {
                "chat_id": str(message.chat.id),
                "message_id": message.message_id,
                "thread_id": message.message_thread_id
            }
            return action_command(normalized, context) or TransportEvent(event_id=event_id,
                                                                         kind="chat",
                                                                         message=normalized,
                                                                         external_actor_id=str(actor.id),
                                                                         external_chat_id=str(message.chat.id),
                                                                         reply_context={
                                                                             "chat_id": str(message.chat.id),
                                                                             "message_id": message.message_id,
                                                                             "thread_id": message.message_thread_id
                                                                         })
        except (AttributeError, KeyError, TypeError, ValueError):
            raise ValueError("invalid telegram update") from None

    async def poll(self, offset: int | None, *, timeout: int = 20) -> tuple[Update, ...]:
        """Caller persists every update before passing a greater offset."""
        return await self.bot.get_updates(offset=offset,
                                          timeout=timeout,
                                          allowed_updates=["message", "callback_query", "edited_message"])

    async def send_text(self, context: dict, text: str, *, edit_message_id: int | None = None) -> DeliveryResult:
        try:
            if edit_message_id is None:
                message = await self.bot.send_message(chat_id=context["chat_id"],
                                                      text=text,
                                                      message_thread_id=context.get("thread_id"))
            else:
                message = await self.bot.edit_message_text(chat_id=context["chat_id"],
                                                           message_id=edit_message_id,
                                                           text=text)
            return DeliveryResult(outcome="accepted",
                                  receipt_kind="message_id",
                                  external_message_id=str(message.message_id))
        except RetryAfter as error:
            delay = error.retry_after
            return DeliveryResult(
                outcome="rejected",
                error_type="rate_limited",
                retryable=True,
                retry_after=delay.total_seconds() if hasattr(delay, "total_seconds") else float(delay))
        except (Forbidden, InvalidToken):
            return DeliveryResult(outcome="rejected", error_type="permission_denied")
        except BadRequest:
            return DeliveryResult(outcome="rejected", error_type="invalid_request")
        except (NetworkError, TelegramError, TimeoutError):
            return DeliveryResult(outcome="unknown", error_type="transport_outcome_unknown")
