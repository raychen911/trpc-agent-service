"""Feishu text channel adapter — decodes SDK frames, encodes safe public events."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from trpc_service.governance.approval import pending_reply_for

from trpc_service.channels.feishu.sdk import FeishuInboundFrame
from trpc_service.channels.models import PublicChannelEvent, UnboundChannelMessage

FEISHU_CHANNEL = "feishu"


def _approval_prompt(event: PublicChannelEvent) -> str:
    """Fixed pending prompt with the opaque approval id (6A2)."""
    data = event.data if isinstance(event.data, dict) else {}
    raw = data.get("approval_id")
    try:
        return pending_reply_for(uuid.UUID(str(raw)))
    except (TypeError, ValueError):
        return _SAFE_ERROR_TEXT


_SAFE_ERROR_TEXT = "An internal error occurred."


@dataclass(frozen=True, slots=True)
class FeishuReply:
    """Outgoing reply chunk for the Feishu streaming protocol."""

    text: str
    finished: bool


class FeishuChannelAdapter:
    """Adapter for authenticated Feishu input before binding resolution."""

    channel: str = FEISHU_CHANNEL

    def __init__(self) -> None:
        pass

    def decode_frame(self, frame: FeishuInboundFrame) -> UnboundChannelMessage:
        """Extract unbound input from a Feishu SDK frame.

        Raises ``ValueError`` for missing or invalid fields.
        The error message contains no external IDs or frame content.
        """
        return UnboundChannelMessage(
            channel=FEISHU_CHANNEL,
            external_account_id=frame.external_account_id,
            conversation_kind=frame.conversation_kind,  # type: ignore[arg-type]
            external_user_id=frame.external_user_id,
            external_conversation_id=frame.external_conversation_id,
            external_message_id=frame.external_message_id,
            kind=frame.kind,  # type: ignore[arg-type]
            text=frame.text,
            occurred_at_ms=frame.occurred_at_ms,
        )

    decode_text_frame = decode_frame

    def encode_event(self, event: PublicChannelEvent) -> FeishuReply | None:
        """Convert a public channel event to a :class:`FeishuReply`.

        - ``delta`` → non-finished text chunk
        - ``done`` → finished marker
        - ``error`` → safe fixed text, finished
        - ``tool`` → ``None`` (suppressed; no tool details leak)
        """
        if event.type == "tool":
            return None
        if event.type == "delta":
            text = event.data if isinstance(event.data, str) else ""
            return FeishuReply(text=text, finished=False)
        if event.type == "done":
            return FeishuReply(text="", finished=True)
        if event.type == "error":
            return FeishuReply(text=_SAFE_ERROR_TEXT, finished=True)
        if event.type == "approval":
            return FeishuReply(text=_approval_prompt(event), finished=False)
        return None


__all__ = [
    "FEISHU_CHANNEL",
    "FeishuChannelAdapter",
    "FeishuReply",
]
