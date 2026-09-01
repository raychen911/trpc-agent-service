"""Map Agent events to provider-bound outbound channel messages."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from trpc_service.channels.base import Attachment, OutboundMessage
from trpc_service.channels.reliable import split_text
from trpc_service.tenant.models import AgentEvent


TEXT_EVENT_TYPES = {"message_end", "message", "answer"}
STREAM_EVENT_TYPES = {"stream_delta", "message_delta", "delta"}
CARD_EVENT_TYPES = {"card", "message_card", "approval_required"}
FILE_EVENT_TYPES = {"file", "artifact"}
IMAGE_EVENT_TYPES = {"image", "photo"}


def build_outbound_messages(
    events: list[AgentEvent],
    *,
    channel: str,
    account_id: str,
    session_id: str,
    external_user_id: str,
    group_id: str | None = None,
) -> list[OutboundMessage]:
    """Convert Agent events into normalized outbound IM messages.

    Platform adapters may have richer send APIs, but every provider gets a
    text fallback so unsupported card/file/image messages still deliver safely.
    """
    messages: list[OutboundMessage] = []
    if any(event.event_type == "message_revoked" for event in events):
        return []
    stream_parts: list[str] = []
    has_terminal_text = False

    for event in events:
        if event.event_type in STREAM_EVENT_TYPES:
            stream_parts.append(event.content)
            continue
        if event.event_type in CARD_EVENT_TYPES:
            messages.append(
                _message(
                    channel,
                    account_id,
                    session_id,
                    external_user_id,
                    group_id,
                    _render_card(event),
                    "card",
                    {"card": dict(event.metadata)},
                )
            )
            continue
        if event.event_type in FILE_EVENT_TYPES or event.event_type in IMAGE_EVENT_TYPES:
            message_type = "image" if event.event_type in IMAGE_EVENT_TYPES else "file"
            attachment = _attachment_from_event(event, message_type)
            messages.append(
                _message(
                    channel,
                    account_id,
                    session_id,
                    external_user_id,
                    group_id,
                    event.content or _attachment_fallback(attachment, message_type),
                    message_type,
                    {message_type: dict(event.metadata)},
                    [attachment],
                )
            )
            continue
        if event.event_type in TEXT_EVENT_TYPES:
            has_terminal_text = True
            messages.append(
                _message(
                    channel,
                    account_id,
                    session_id,
                    external_user_id,
                    group_id,
                    event.content,
                    "text",
                    dict(event.metadata),
                )
            )

    if stream_parts and not has_terminal_text:
        messages.append(
            _message(
                channel,
                account_id,
                session_id,
                external_user_id,
                group_id,
                "".join(stream_parts),
                "stream",
                {"stream_final": True},
            )
        )

    if messages:
        return messages
    return [
        _message(
            channel,
            account_id,
            session_id,
            external_user_id,
            group_id,
            events[-1].content if events else "",
            "text",
            {},
        )
    ]


def split_outbound_messages(messages: list[OutboundMessage], max_length: int) -> list[OutboundMessage]:
    """Split long text while preserving message type and attachment metadata."""
    split_messages: list[OutboundMessage] = []
    for message in messages:
        parts = split_text(message.text, max_length)
        part_count = len(parts)
        for index, part in enumerate(parts):
            metadata = {
                **message.metadata,
                "message_type": message.metadata.get("message_type", "text"),
                "part_index": index,
                "part_count": part_count,
            }
            split_messages.append(
                replace(
                    message,
                    text=part,
                    attachments=message.attachments if index == 0 else [],
                    metadata=metadata,
                )
            )
    return split_messages


def visible_answer(messages: list[OutboundMessage]) -> str:
    return "\n".join(message.text for message in messages if message.text).strip()


def _message(
    channel: str,
    account_id: str,
    session_id: str,
    external_user_id: str,
    group_id: str | None,
    text: str,
    message_type: str,
    metadata: dict[str, Any],
    attachments: list[Attachment] | None = None,
) -> OutboundMessage:
    return OutboundMessage(
        channel=channel,
        account_id=account_id,
        session_id=session_id,
        external_user_id=external_user_id,
        group_id=group_id,
        text=text,
        attachments=attachments or [],
        metadata={**metadata, "message_type": message_type},
    )


def _render_card(event: AgentEvent) -> str:
    title = str(event.metadata.get("title") or event.content or "通知")
    subtitle = str(event.metadata.get("subtitle") or "")
    body = str(event.metadata.get("body") or event.metadata.get("description") or "")
    buttons = event.metadata.get("buttons") or []
    lines = [title]
    if subtitle:
        lines.append(subtitle)
    if body:
        lines.append(body)
    for button in buttons:
        if isinstance(button, dict):
            label = button.get("label") or button.get("text") or button.get("title")
            url = button.get("url")
            if label and url:
                lines.append(f"{label}: {url}")
            elif label:
                lines.append(str(label))
    return "\n".join(lines)


def _attachment_from_event(event: AgentEvent, message_type: str) -> Attachment:
    metadata = dict(event.metadata)
    attachment_data = metadata.get("attachment") if isinstance(metadata.get("attachment"), dict) else metadata
    return Attachment(
        kind=str(attachment_data.get("kind") or message_type),
        url=attachment_data.get("url"),
        name=attachment_data.get("name") or attachment_data.get("filename"),
        content_type=attachment_data.get("content_type"),
        metadata=metadata,
    )


def _attachment_fallback(attachment: Attachment, message_type: str) -> str:
    label = attachment.name or attachment.url or message_type
    return f"[{message_type}] {label}"
