import hmac

from trpc_service.channels.base import (
    ChannelCapabilities,
    ChannelVerificationError,
    OutboundMessage,
    SendResult,
    normalize_attachment,
    parse_attachments,
)
from trpc_service.channels.media import attachment_kind, post_multipart_json, prepare_attachment_file
from trpc_service.channels.sdk import run_async
from trpc_service.channels.simple import SimpleJsonChannelAdapter
from trpc_service.security.secrets import SecretManager, redact_secret_data, redact_secret_text


class TelegramAdapter(SimpleJsonChannelAdapter):
    channel_name = "telegram"
    capabilities = ChannelCapabilities(max_text_length=4096, supports_media=True, supports_revoke=False)
    message_id_fields = ("message_id", "update_id")
    user_id_fields = ("from_user_id", "from", "user_id")
    group_id_fields = ("chat_id", "group_id")
    text_fields = ("text", "message")
    _MESSAGE_UPDATE_FIELDS = {
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
        "callback_query",
    }

    def is_noop(self, payload, binding) -> bool:
        del binding
        return not any(field in payload for field in self._MESSAGE_UPDATE_FIELDS)

    def verify_callback(self, payload, binding):
        expected = None
        if binding.secret_ref:
            try:
                expected = SecretManager().resolve(binding.secret_ref)
            except Exception as exc:
                raise ChannelVerificationError("Telegram webhook secret is unavailable") from exc
        supplied = payload.get("_callback_secret_token", payload.get("secret_token"))
        if expected and not isinstance(supplied, str):
            raise ChannelVerificationError("invalid Telegram webhook secret")
        if expected and not hmac.compare_digest(supplied, expected):
            raise ChannelVerificationError("invalid Telegram webhook secret")

    def parse_event(self, payload, binding):
        payload = {
            key: value for key, value in payload.items() if key not in {"_callback_secret_token", "secret_token"}
        }
        callback_query = payload.get("callback_query")
        message = (
            payload.get("message")
            or payload.get("edited_message")
            or payload.get("channel_post")
            or payload.get("edited_channel_post")
            or (callback_query.get("message") if isinstance(callback_query, dict) else None)
            or payload
        )
        if not isinstance(message, dict):
            message = payload
        sender = message.get("from", {})
        chat = message.get("chat", {})
        attachments = parse_attachments(payload.get("attachments") or message.get("attachments"))
        if not attachments:
            photos = message.get("photo") or []
            if photos:
                photo = sorted(
                    photos,
                    key=lambda item: int(item.get("file_size") or item.get("width") or 0),
                    reverse=True,
                )[0]
                attachments.append(
                    normalize_attachment(
                        {
                            "kind": "image",
                            "name": f"telegram-{photo.get('file_unique_id') or photo.get('file_id')}.jpg",
                            "content_type": "image/jpeg",
                            "metadata": {
                                "file_id": photo.get("file_id"),
                                "file_unique_id": photo.get("file_unique_id"),
                                "width": photo.get("width"),
                                "height": photo.get("height"),
                                "file_size": photo.get("file_size"),
                                "source": "telegram.photo",
                            },
                        },
                        default_kind="image",
                    )
                )
            if message.get("document"):
                document = message["document"]
                attachments.append(
                    normalize_attachment(
                        {
                            "kind": "file",
                            "name": document.get("file_name")
                            or document.get("file_unique_id")
                            or document.get("file_id"),
                            "content_type": document.get("mime_type"),
                            "metadata": {
                                "file_id": document.get("file_id"),
                                "file_unique_id": document.get("file_unique_id"),
                                "file_size": document.get("file_size"),
                                "source": "telegram.document",
                            },
                        },
                        default_kind="file",
                    )
                )
            if message.get("voice"):
                voice = message["voice"]
                attachments.append(
                    normalize_attachment(
                        {
                            "kind": "file",
                            "name": f"telegram-{voice.get('file_unique_id') or voice.get('file_id')}.ogg",
                            "content_type": voice.get("mime_type") or "audio/ogg",
                            "metadata": {
                                "file_id": voice.get("file_id"),
                                "file_unique_id": voice.get("file_unique_id"),
                                "duration": voice.get("duration"),
                                "file_size": voice.get("file_size"),
                                "source": "telegram.voice",
                            },
                        },
                        default_kind="file",
                    )
                )
        return super().parse_event(
            {
                **payload,
                "message_id": message.get("message_id", payload.get("update_id", "")),
                "from_user_id": sender.get(
                    "id",
                    (callback_query.get("from", {}).get("id") if isinstance(callback_query, dict) else None)
                    or payload.get("from_user_id", ""),
                ),
                "chat_id": chat.get("id", payload.get("chat_id", "")),
                "text": message.get(
                    "text",
                    message.get(
                        "caption",
                        callback_query.get("data", "") if isinstance(callback_query, dict) else payload.get("text", ""),
                    ),
                ),
                "attachments": [
                    {
                        "kind": item.kind,
                        "url": item.url,
                        "name": item.name,
                        "content_type": item.content_type,
                        "metadata": item.metadata,
                    }
                    for item in attachments
                ],
            },
            binding,
        )

    def send(self, message: OutboundMessage, binding) -> SendResult:
        token_ref = binding.token_ref
        if not token_ref:
            return super().send(message, binding)
        try:
            token = SecretManager().resolve(token_ref)
        except Exception:
            return SendResult(False, "", "Telegram bot token is unavailable")
        attachment = message.attachments[0] if message.attachments else None
        media_kind = attachment_kind(attachment) if attachment else "text"
        if binding.config.get("sdk_enabled", True):
            try:
                from telegram import Bot

                if attachment and media_kind in {"image", "file"}:
                    with prepare_attachment_file(attachment) as prepared:
                        if prepared is None:
                            return super().send(message, binding)
                        sent = run_async(
                            lambda: self._send_attachment_with_sdk(
                                Bot(token=token),
                                message.group_id or message.external_user_id,
                                message.text,
                                prepared.path,
                                media_kind,
                            )
                        )
                        return SendResult(
                            True,
                            f"telegram:{getattr(sent, 'message_id', message.session_id)}",
                            metadata={
                                "sdk": "python-telegram-bot",
                                "message_type": media_kind,
                                "filename": prepared.filename,
                            },
                        )
                sent = run_async(
                    lambda: self._send_text_with_sdk(
                        Bot(token=token),
                        message.group_id or message.external_user_id,
                        message.text,
                    )
                )
                return SendResult(
                    True,
                    f"telegram:{getattr(sent, 'message_id', message.session_id)}",
                    metadata={"sdk": "python-telegram-bot", "message_type": "text"},
                )
            except ImportError:
                pass
            except Exception as exc:
                return SendResult(
                    False, "", redact_secret_text(f"Telegram SDK send failed: {type(exc).__name__}: {exc}")
                )
        if attachment and media_kind in {"image", "file"}:
            with prepare_attachment_file(attachment) as prepared:
                if prepared is None:
                    return super().send(message, binding)
                endpoint = "sendPhoto" if media_kind == "image" else "sendDocument"
                fields = {
                    "chat_id": message.group_id or message.external_user_id,
                }
                if message.text:
                    fields["caption"] = message.text
                body = post_multipart_json(
                    f"https://api.telegram.org/bot{token}/{endpoint}",
                    fields=fields,
                    file_field="photo" if media_kind == "image" else "document",
                    file_path=prepared.path,
                    content_type=prepared.content_type,
                )
                if not body.get("ok"):
                    return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
                result = body.get("result", {}) or {}
                return SendResult(
                    True,
                    f"telegram:{result.get('message_id', message.session_id)}",
                    metadata={"message_type": media_kind, "provider_response": redact_secret_data(body)},
                )
        import json
        from urllib.request import Request, urlopen

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        request = Request(
            url,
            data=json.dumps({"chat_id": message.group_id or message.external_user_id, "text": message.text}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=10) as response:
            body = json.loads(response.read().decode())
        if not body.get("ok"):
            return SendResult(False, "", redact_secret_text(body), redact_secret_data(body))
        return SendResult(
            True,
            f"telegram:{body.get('result', {}).get('message_id', message.session_id)}",
            metadata={"message_type": "text", "provider_response": redact_secret_data(body)},
        )

    @staticmethod
    async def _send_text_with_sdk(bot, chat_id: str, text: str):
        try:
            return await bot.send_message(chat_id=chat_id, text=text)
        finally:
            await bot.shutdown()

    @staticmethod
    async def _send_attachment_with_sdk(bot, chat_id: str, text: str, path, media_kind: str):
        try:
            if media_kind == "image":
                return await bot.send_photo(chat_id=chat_id, photo=str(path), caption=text or None)
            return await bot.send_document(chat_id=chat_id, document=str(path), caption=text or None)
        finally:
            await bot.shutdown()
