"""Telegram Bot API webhook verification, normalization, and replies."""

from __future__ import annotations

import asyncio
import hmac
from datetime import UTC, datetime
from typing import Any

import httpx

from trpc_service.channels.models import ChannelMessage
from trpc_service.config.models import ChannelType


class TelegramAdapter:
    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._client = client or httpx.AsyncClient(timeout=20)
        self._owns_client = client is None

    @staticmethod
    def verify_secret(received: str | None, expected: str) -> bool:
        return received is not None and hmac.compare_digest(received, expected)

    @staticmethod
    def parse(account_id: str, payload: dict[str, Any]) -> ChannelMessage | None:
        update_id = payload.get("update_id")
        message = payload.get("message") or payload.get("edited_message")
        if not isinstance(update_id, int) or not isinstance(message, dict):
            return None
        text = message.get("text")
        chat = message.get("chat")
        sender = message.get("from")
        message_id = message.get("message_id")
        if not isinstance(text, str) or not isinstance(chat, dict) or not isinstance(sender, dict):
            return None
        if message_id is None or chat.get("id") is None or sender.get("id") is None:
            return None
        timestamp = datetime.fromtimestamp(message.get("date", 0), UTC)
        thread_id = message.get("message_thread_id")
        return ChannelMessage(
            external_message_id=f"{update_id}:{message_id}",
            channel=ChannelType.TELEGRAM,
            account_id=account_id,
            chat_type=str(chat.get("type", "unknown")),
            chat_id=str(chat["id"]),
            sender_id=str(sender["id"]),
            text=text,
            thread_id=str(thread_id) if thread_id is not None else None,
            timestamp=timestamp,
            metadata={"update_id": update_id},
        )

    async def send(self, token: str, message: ChannelMessage, text: str) -> None:
        for chunk in _split_text(text):
            body: dict[str, Any] = {"chat_id": message.chat_id, "text": chunk}
            if message.thread_id:
                body["message_thread_id"] = message.thread_id
            await self._post_with_retry(f"https://api.telegram.org/bot{token}/sendMessage", body)

    async def get_updates(
        self, token: str, offset: int | None, timeout: int = 30
    ) -> list[dict[str, Any]]:
        body: dict[str, Any] = {
            "timeout": timeout,
            "allowed_updates": ["message", "edited_message"],
        }
        if offset is not None:
            body["offset"] = offset
        response = await self._client.post(
            f"https://api.telegram.org/bot{token}/getUpdates",
            json=body,
            timeout=timeout + 10,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("ok", False) or not isinstance(data.get("result"), list):
            raise RuntimeError("Telegram Bot API rejected getUpdates")
        return data["result"]

    async def _post_with_retry(self, url: str, body: dict[str, Any]) -> None:
        for attempt in range(2):
            response = await self._client.post(url, json=body)
            if response.status_code != 429:
                response.raise_for_status()
                data = response.json()
                if not data.get("ok", False):
                    raise RuntimeError("Telegram Bot API rejected the message")
                return
            if attempt == 1:
                response.raise_for_status()
            retry_after = float(response.json().get("parameters", {}).get("retry_after", 1))
            await asyncio.sleep(min(max(retry_after, 0), 10))

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


def _split_text(text: str, limit: int = 4000) -> list[str]:
    return [text[index : index + limit] for index in range(0, len(text), limit)] or [""]


__all__ = ["TelegramAdapter"]
