"""Channel adapter contracts and shared session/message helpers."""

from __future__ import annotations

import hashlib
import hmac
from abc import ABC, abstractmethod
from typing import Any

from ..tenant.models import ChannelBinding, InboundMessage, OutboundMessage


def make_session_id(tenant_id: str, channel: str, chat_id: str, chat_type: str, secret: str) -> str:
    """Return a stable, tenant-scoped ID; external IDs never become raw keys."""
    scope = chat_id if chat_type == "group" else f"user:{chat_id}"
    value = f"{tenant_id}:{channel}:{scope}".encode()
    return hmac.new(secret.encode(), value, hashlib.sha256).hexdigest()[:32]


def split_text(text: str, limit: int, byte_limit: bool = False) -> list[str]:
    if not text:
        return [""]
    parts: list[str] = []
    current = ""
    for char in text:
        candidate = current + char
        size = len(candidate.encode("utf-8")) if byte_limit else len(candidate)
        if current and size > limit:
            parts.append(current)
            current = char
        else:
            current = candidate
    if current:
        parts.append(current)
    return parts


class ChannelAdapter(ABC):
    name: str
    max_message_size: int = 4096
    byte_limit: bool = False

    @abstractmethod
    def verify(self, binding: ChannelBinding, headers: dict[str, str], body: bytes) -> bool:
        raise NotImplementedError

    @abstractmethod
    def parse(self, tenant_id: str, binding: ChannelBinding, payload: Any, trace_id: str) -> InboundMessage:
        raise NotImplementedError

    def to_outbound(self, message: InboundMessage, text: str) -> list[OutboundMessage]:
        parts = split_text(text, self.max_message_size, self.byte_limit)
        return [
            OutboundMessage(
                tenant_id=message.tenant_id,
                channel=message.channel,
                account_id=message.account_id,
                external_user_id=message.external_user_id,
                chat_id=message.chat_id,
                text=part,
                in_reply_to=message.external_message_id,
                trace_id=message.trace_id,
                part=index,
                total_parts=len(parts),
                reply_context=dict(message.raw.get("reply_context") or {}),
            )
            for index, part in enumerate(parts, 1)
        ]

    async def send(self, message: OutboundMessage, binding: ChannelBinding | None = None) -> dict[str, Any]:
        """Return a delivery record; production replaces this with an SDK call."""
        _ = binding
        return {"ok": True, "channel": self.name, "message_id": message.in_reply_to, "part": message.part}
