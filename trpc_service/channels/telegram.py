"""Telegram webhook parsing and Bot API delivery."""

from __future__ import annotations

import asyncio
import hmac
import json
from collections.abc import Mapping
from typing import Any

from ..config import resolve_secret
from ..tenant.models import ChannelBinding, InboundMessage, OutboundMessage
from .base import ChannelAdapter
from .feishu import AsyncHTTPClient, FeishuError, UrllibAsyncHTTPClient


class TelegramAdapter(ChannelAdapter):
    name = "telegram"
    max_message_size = 4096

    def __init__(
        self,
        http_client: AsyncHTTPClient | None = None,
        *,
        api_base_url: str = "https://api.telegram.org",
        dry_run: bool = False,
    ) -> None:
        self.http_client = http_client or UrllibAsyncHTTPClient()
        self.api_base_url = api_base_url.rstrip("/")
        self.dry_run = dry_run

    def verify(self, binding: ChannelBinding, headers: dict[str, str], body: bytes) -> bool:
        _ = body
        expected = _secret(binding.verify_token)
        supplied = headers.get("x-telegram-bot-api-secret-token", "")
        return bool(expected and supplied and hmac.compare_digest(expected, supplied))

    def parse(self, tenant_id: str, binding: ChannelBinding, payload: Any, trace_id: str) -> InboundMessage:
        update = dict(payload)
        message = dict(update.get("message") or update.get("edited_message") or update.get("channel_post") or {})
        chat = dict(message.get("chat") or {})
        user = dict(message.get("from") or {})
        chat_id = str(chat.get("id") or user.get("id") or "")
        user_id = str(user.get("id") or chat_id)
        chat_type = "group" if chat.get("type") in {"group", "supergroup"} else "direct"
        media_type, media_id = _telegram_media(message)
        raw = dict(update)
        raw.pop("normalized_media", None)
        raw.pop("_artifact_materialized", None)
        if media_type:
            raw["normalized_media"] = {"type": media_type, "file_id": media_id}
        return InboundMessage(
            tenant_id=tenant_id,
            channel=self.name,
            account_id=binding.account_id,
            external_message_id=str(message.get("message_id") or update.get("update_id") or ""),
            external_user_id=user_id,
            chat_id=chat_id,
            chat_type=chat_type,
            text=str(message.get("text") or message.get("caption") or (f"[{media_type}]" if media_type else "")),
            trace_id=trace_id,
            raw=raw,
        )

    async def send(self, message: OutboundMessage, binding: ChannelBinding | None = None) -> dict[str, Any]:
        if binding is None or binding.channel != self.name or binding.account_id != message.account_id:
            return {"ok": False, "code": "binding_mismatch"}
        if self.dry_run:
            return {"ok": True, "provider_message_id": f"dry-run-{message.in_reply_to}-{message.part}"}
        token = _secret(binding.secret_ref)
        if not token:
            return {"ok": False, "code": "bot_token_missing"}
        method, payload = self._request(message)
        url = f"{binding.api_base_url or self.api_base_url}/bot{token}/{method}"
        for attempt in range(3):
            try:
                response = await self.http_client.post(url, headers={}, payload=payload)
                body = response.json()
            except (FeishuError, OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                if attempt == 2:
                    return {"ok": False, "code": type(exc).__name__, "retryable": True}
                await asyncio.sleep(0.1 * (2**attempt))
                continue
            ok = response.status_code < 400 and body.get("ok") is True
            if ok:
                result = body.get("result") if isinstance(body.get("result"), Mapping) else {}
                return {"ok": True, "provider_message_id": result.get("message_id")}
            parameters = body.get("parameters") if isinstance(body.get("parameters"), Mapping) else {}
            retry_after = _positive_float(parameters.get("retry_after"))
            retryable = response.status_code == 429 or response.status_code >= 500
            if not retryable or attempt == 2:
                return {
                    "ok": False,
                    "code": body.get("error_code", response.status_code),
                    "retryable": retryable,
                    "retry_after": retry_after,
                }
            await asyncio.sleep(min(retry_after or 0.1 * (2**attempt), 5.0))
        return {"ok": False, "code": "retry_exhausted", "retryable": True}

    def _request(self, message: OutboundMessage) -> tuple[str, dict[str, Any]]:
        payload: dict[str, Any] = {"chat_id": message.chat_id}
        if message.in_reply_to.isdigit():
            payload["reply_parameters"] = {"message_id": int(message.in_reply_to)}
        if message.message_type == "photo":
            payload.update(photo=message.media_id or message.media_url, caption=message.text)
            return "sendPhoto", payload
        if message.message_type in {"file", "document"}:
            payload.update(document=message.media_id or message.media_url, caption=message.text)
            return "sendDocument", payload
        payload["text"] = message.text
        return "sendMessage", payload

    async def send_proactive(
        self, binding: ChannelBinding, chat_id: str, text: str, *, message_type: str = "text", media: str | None = None
    ) -> dict[str, Any]:
        outbound = OutboundMessage(
            tenant_id="proactive",
            channel=self.name,
            account_id=binding.account_id,
            external_user_id=chat_id,
            chat_id=chat_id,
            text=text,
            in_reply_to="",
            trace_id="",
            message_type=message_type,
            media_url=media,
        )
        return await self.send(outbound, binding)

    async def download_media(
        self, binding: ChannelBinding, file_id: str, *, max_bytes: int = 30 * 1024 * 1024
    ) -> tuple[bytes, str]:
        token = _secret(binding.secret_ref)
        if not token:
            raise ValueError("Telegram bot token is missing")
        base_url = binding.api_base_url or self.api_base_url
        response = await self.http_client.post(
            f"{base_url}/bot{token}/getFile",
            headers={},
            payload={"file_id": file_id},
        )
        payload = response.json()
        result = payload.get("result") if isinstance(payload.get("result"), Mapping) else {}
        file_path = result.get("file_path")
        if response.status_code >= 400 or payload.get("ok") is not True or not isinstance(file_path, str):
            raise ValueError("Telegram getFile failed")
        get = getattr(self.http_client, "get", None)
        if not callable(get):
            raise TypeError("Telegram media download is unsupported")
        downloaded = await get(f"{base_url}/file/bot{token}/{file_path.lstrip('/')}", headers={})
        if downloaded.status_code >= 400:
            raise ValueError("Telegram media download failed")
        if not downloaded.body or len(downloaded.body) > max_bytes:
            raise ValueError("Telegram media exceeds configured size")
        content_type = _content_type(downloaded.headers, "application/octet-stream")
        return downloaded.body, content_type


def _telegram_media(message: Mapping[str, Any]) -> tuple[str | None, str | None]:
    photos = message.get("photo")
    if isinstance(photos, list) and photos and isinstance(photos[-1], Mapping):
        return "photo", str(photos[-1].get("file_id") or "")
    for key in ("document", "video", "audio", "voice"):
        item = message.get(key)
        if isinstance(item, Mapping):
            return key, str(item.get("file_id") or "")
    return None, None


def _secret(reference: str | None) -> str | None:
    if reference and reference.startswith(("env://", "file://", "literal://")):
        return resolve_secret(reference)
    return reference


def _positive_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if 0 < number <= 3600 else None


def _content_type(headers: Mapping[str, str], default: str) -> str:
    value = next((str(item) for key, item in headers.items() if str(key).lower() == "content-type"), default)
    return value.partition(";")[0].strip() or default
