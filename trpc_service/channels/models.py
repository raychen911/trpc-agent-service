"""Normalized inbound message shared by all channel gateways."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from trpc_service.config.models import ChannelType


@dataclass(frozen=True, slots=True)
class ChannelMessage:
    external_message_id: str
    channel: ChannelType
    account_id: str
    chat_type: str
    chat_id: str
    sender_id: str
    text: str
    thread_id: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def conversation_id(self) -> str:
        return f"{self.chat_id}:{self.thread_id}" if self.thread_id else self.chat_id


__all__ = ["ChannelMessage"]
