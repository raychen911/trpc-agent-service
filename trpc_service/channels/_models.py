# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Normalized message models shared by all IM channel adapters."""

from __future__ import annotations

import hashlib
import json
from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field

CHAT_PRIVATE = "private"
CHAT_GROUP = "group"


def generate_session_id(
    tenant_id: str,
    channel: str,
    chat_type: str,
    user_id: str,
    chat_id: Optional[str] = None,
) -> str:
    """Derive a stable, tenant-scoped session id.

    Single chat hashes ``[tenant, channel, "private", user_id]``.
    Group chat hashes ``[tenant, channel, "group", chat_id]``.

    JSON encoding preserves field boundaries even when platform identifiers
    contain delimiter characters. Including ``chat_type`` also prevents a
    private user id from colliding with a group chat id of the same value.
    """
    if chat_type == CHAT_GROUP:
        identity = chat_id or user_id
    else:
        identity = user_id
    raw = json.dumps(
        [tenant_id, channel, chat_type, identity],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def split_text(text: str, max_chars: int) -> list[str]:
    """Split ``text`` into chunks of at most ``max_chars`` characters."""
    if not text:
        return []
    return [text[i:i + max_chars] for i in range(0, len(text), max_chars)]


def split_text_bytes(text: str, max_bytes: int) -> list[str]:
    """Split ``text`` into chunks of at most ``max_bytes`` UTF-8 bytes.

    Used for platforms with byte-length limits (e.g. WeCom's 2048-byte cap).
    Splitting happens on character boundaries so no UTF-8 sequence is broken.
    """
    if not text:
        return []
    chunks: list[str] = []
    current = ""
    current_bytes = 0
    for ch in text:
        ch_bytes = len(ch.encode("utf-8"))
        if current and current_bytes + ch_bytes > max_bytes:
            chunks.append(current)
            current = ""
            current_bytes = 0
        current += ch
        current_bytes += ch_bytes
    if current:
        chunks.append(current)
    return chunks


class InboundMessage(BaseModel):
    """A normalized inbound message produced by a channel adapter."""

    model_config = {"extra": "allow"}

    channel: str
    """Channel identifier: ``wecom`` / ``wechat_kf`` / ``dingtalk`` / ``feishu`` / ``qq``."""
    chat_id: str
    """Platform chat id (group chat id or peer user id)."""
    chat_type: Literal["private", "group"] = CHAT_PRIVATE
    """``private`` or ``group``."""
    sender_id: str
    """Platform user id of the sender."""
    message_id: str
    """Platform message id, used for idempotent de-duplication."""
    text: str = ""
    images: list[str] = Field(default_factory=list)
    files: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    raw: Any = None
    """The raw platform payload, preserved for debugging."""


class OutboundMessage(BaseModel):
    """A normalized outbound message to be rendered by a channel adapter."""

    model_config = {"extra": "allow"}

    chat_id: str
    text: str = ""
    kind: str = "text"
    """``text`` / ``card`` / ``progress`` / ``stream``."""
    metadata: dict[str, Any] = Field(default_factory=dict)


class SendResult(BaseModel):
    """Result of sending one outbound message."""

    ok: bool = True
    message_id: Optional[str] = None
    error: Optional[str] = None
