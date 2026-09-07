"""Deterministic, channel-aware outbound delivery planning."""

from __future__ import annotations

from tenant_agent.channels.base import (
    TELEGRAM_TEXT_MAX_CHARS,
    WECOM_TEXT_MAX_CHARS,
    WECOM_TEXT_MAX_UTF8_BYTES,
    split_text,
)
from tenant_agent.channels.wecom_bot import WECOM_BOT_STREAM_BYTES
from tenant_agent.models import ChannelType, OutboundMessage


def plan_outbound(message: OutboundMessage) -> tuple[OutboundMessage, ...]:
    """Split one logical reply into stable single-effect delivery segments."""

    if message.channel is ChannelType.WECOM_BOT:
        if len(message.text.encode()) > WECOM_BOT_STREAM_BYTES:
            suffix = "\n[Reply truncated at WeCom's 20 KB stream limit.]"
            prefix = message.text.encode()[: WECOM_BOT_STREAM_BYTES - len(suffix.encode())]
            message = message.model_copy(update={"text": prefix.decode("utf-8", errors="ignore") + suffix})
        return (message,)
    if message.channel is ChannelType.TELEGRAM:
        text_chunks = split_text(message.text, max_chars=TELEGRAM_TEXT_MAX_CHARS) if message.text else ()
    elif message.channel is ChannelType.WECOM:
        text_chunks = (
            split_text(
                message.text,
                max_chars=WECOM_TEXT_MAX_CHARS,
                max_utf8_bytes=WECOM_TEXT_MAX_UTF8_BYTES,
            )
            if message.text
            else ()
        )
    else:
        return (message,)

    segments: list[OutboundMessage] = []
    segments.extend(
        message.model_copy(
            update={
                "text": chunk,
                "cards": (),
                "attachments": (),
                "stream_key": None,
            }
        )
        for chunk in text_chunks
    )
    segments.extend(
        message.model_copy(
            update={
                "text": "",
                "cards": (card,),
                "attachments": (),
                "stream_key": None,
            }
        )
        for card in message.cards
    )
    segments.extend(
        message.model_copy(
            update={
                "text": "",
                "cards": (),
                "attachments": (attachment,),
                "stream_key": None,
            }
        )
        for attachment in message.attachments
    )
    if not segments:
        segments.append(message.model_copy(update={"text": " ", "stream_key": None}))

    count = len(segments)
    planned: list[OutboundMessage] = []
    for index, segment in enumerate(segments):
        metadata = dict(segment.metadata)
        metadata.update(
            {
                "delivery_presegmented": True,
                "delivery_segment_index": index,
                "delivery_segment_count": count,
            }
        )
        planned.append(
            segment.model_copy(
                update={
                    "reply_to_message_id": (segment.reply_to_message_id if index == 0 else None),
                    "is_final": index == count - 1,
                    "metadata": metadata,
                }
            )
        )
    return tuple(planned)
