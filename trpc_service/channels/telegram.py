import hmac
import uuid
from collections.abc import Mapping
from typing import Any

from trpc_service.channels.bindings import ResolvedChannelBinding
from trpc_service.gateway.contracts import NormalizedMessage


class ChannelAuthenticationError(ValueError):
    pass


class UnsupportedChannelMessageError(ValueError):
    pass


class TelegramAdapter:
    def normalize(
        self,
        payload: Mapping[str, Any],
        binding: ResolvedChannelBinding,
        provided_secret: str | None,
        expected_secret: str | None,
        trace_id: str | None = None,
    ) -> NormalizedMessage:
        if expected_secret and (
            provided_secret is None or not hmac.compare_digest(provided_secret, expected_secret)
        ):
            raise ChannelAuthenticationError("invalid Telegram webhook secret")
        update_id = payload.get("update_id")
        message = payload.get("message") or payload.get("edited_message")
        if update_id is None or not isinstance(message, Mapping):
            raise UnsupportedChannelMessageError("Telegram update has no supported message")
        text = message.get("text") or message.get("caption")
        chat = message.get("chat")
        sender = message.get("from")
        if not isinstance(chat, Mapping):
            raise UnsupportedChannelMessageError("Telegram message has no chat")
        attachments = self._attachments(message)
        if not isinstance(text, str):
            if not attachments:
                raise UnsupportedChannelMessageError("unsupported Telegram message type")
            kinds = ", ".join(str(item["type"]) for item in attachments)
            text = f"[Telegram attachment: {kinds}]"
        chat_id = str(chat.get("id"))
        chat_type = str(chat.get("type", "private"))
        sender_id = str(sender.get("id")) if isinstance(sender, Mapping) else chat_id
        conversation_type = "group" if chat_type in {"group", "supergroup", "channel"} else "direct"
        return NormalizedMessage(
            tenant_id=binding.tenant_id,
            agent_app_id=binding.agent_app_id,
            channel="telegram",
            account_id=binding.account_id,
            external_message_id=str(update_id),
            sender_user_id=sender_id,
            conversation_id=chat_id,
            conversation_type=conversation_type,
            text=text,
            trace_id=trace_id or str(uuid.uuid4()),
            metadata={
                "message_id": message.get("message_id"),
                "chat_type": chat_type,
                "thread_id": message.get("message_thread_id"),
                "attachments": attachments,
            },
        )

    @staticmethod
    def _attachments(message: Mapping[str, Any]) -> list[dict[str, Any]]:
        attachments: list[dict[str, Any]] = []
        photos = message.get("photo")
        if isinstance(photos, list) and photos:
            photo = photos[-1]
            if isinstance(photo, Mapping) and photo.get("file_id"):
                attachments.append(
                    {
                        "type": "image",
                        "provider_file_id": str(photo["file_id"]),
                        "provider_unique_id": photo.get("file_unique_id"),
                        "width": photo.get("width"),
                        "height": photo.get("height"),
                        "size_bytes": photo.get("file_size"),
                    }
                )
        for field, kind in (
            ("document", "file"),
            ("video", "video"),
            ("audio", "audio"),
            ("voice", "audio"),
        ):
            item = message.get(field)
            if isinstance(item, Mapping) and item.get("file_id"):
                attachments.append(
                    {
                        "type": kind,
                        "provider_file_id": str(item["file_id"]),
                        "provider_unique_id": item.get("file_unique_id"),
                        "file_name": item.get("file_name"),
                        "mime_type": item.get("mime_type"),
                        "size_bytes": item.get("file_size"),
                    }
                )
        return attachments

    @staticmethod
    def webhook_reply(message: NormalizedMessage, text: str) -> dict[str, Any]:
        reply: dict[str, Any] = {
            "method": "sendMessage",
            "chat_id": message.conversation_id,
            "text": text,
        }
        thread_id = message.metadata.get("thread_id")
        if thread_id is not None:
            reply["message_thread_id"] = thread_id
        return reply
