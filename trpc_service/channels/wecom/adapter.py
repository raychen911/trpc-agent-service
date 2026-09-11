"""WeCom text channel adapter — decodes SDK frames, encodes safe public events."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from trpc_service.governance.approval import pending_reply_for
from typing import Any, Mapping

from trpc_service.channels.models import InboundMessage, PublicChannelEvent, UnboundChannelMessage

WECOM_CHANNEL = "wecom"
_SAFE_ERROR_TEXT = "An internal error occurred."


@dataclass(frozen=True, slots=True)
class WeComReply:
    """Outgoing reply chunk for the WeCom streaming protocol."""

    text: str
    finished: bool


class WeComChannelAdapter:
    """Decode authenticated WeCom frames without tenant authority.

    ``external_account_id`` comes from the authenticated binding/service, not
    from a callback frame.  ``tenant_id`` is accepted only for the retained
    legacy ``decode_text_frame`` helper; the binding-aware service uses
    :meth:`decode_frame` exclusively.
    """

    channel: str = WECOM_CHANNEL

    def __init__(self, tenant_id: str | None = None, *, external_account_id: str | None = None) -> None:
        from trpc_service.tenant.context import validate_tenant_id

        if tenant_id is not None:
            validate_tenant_id(tenant_id)
        if external_account_id is not None:
            if not isinstance(external_account_id, str) or not external_account_id.strip():
                raise ValueError("Invalid WeCom frame.")
            external_account_id = external_account_id.strip()
        if tenant_id is None and external_account_id is None:
            raise ValueError("Invalid WeCom frame.")
        self._tenant_id = tenant_id
        self._external_account_id = external_account_id

    def decode_frame(self, frame: Mapping[str, Any]) -> UnboundChannelMessage:
        """Extract an unbound message from a WeCom SDK frame.

        Media is represented but never downloaded or forwarded.  Only a
        ChannelBinding may add a tenant/app after this point.
        The error message contains no external IDs or frame content.
        """
        if self._external_account_id is None:
            raise ValueError("Invalid WeCom frame.")
        body = frame.get("body") if isinstance(frame, Mapping) else None
        if not isinstance(body, Mapping):
            raise ValueError("Invalid WeCom frame.")

        msgtype = body.get("msgtype")
        kind = {"text": "text", "image": "image", "file": "file"}.get(msgtype, "unsupported")
        text: str | None = None
        if kind == "text":
            text_obj = body.get("text")
            if not isinstance(text_obj, Mapping):
                raise ValueError("Missing text content.")
            candidate = text_obj.get("content", "")
            if not isinstance(candidate, str) or not candidate.strip():
                raise ValueError("Missing text content.")
            text = candidate.strip()

        from_obj = body.get("from")
        if not isinstance(from_obj, Mapping):
            raise ValueError("Missing sender.")
        userid = from_obj.get("userid", "")
        if not isinstance(userid, str) or not userid.strip():
            raise ValueError("Missing sender.")

        # Session identity projection (SDK 1.0.2, client.py: "单聊填用户的
        # userid，群聊填对应群聊的 chatid"): real single-chat frames carry no
        # chatid at all.  Unknown or missing chattype is rejected fail-closed.
        chat_type = body.get("chattype")
        if chat_type == "single":
            conversation_id = userid.strip()
            conversation_kind = "direct"
        elif chat_type == "group":
            chatid = body.get("chatid", "")
            if not isinstance(chatid, str) or not chatid.strip():
                raise ValueError("Missing conversation ID.")
            conversation_id = chatid.strip()
            conversation_kind = "group"
        else:
            raise ValueError("Unsupported chat type.")

        msgid = body.get("msgid", "")
        if not isinstance(msgid, str) or not msgid.strip():
            raise ValueError("Missing message ID.")

        occurred_at_ms = body.get("create_time_ms")
        if occurred_at_ms is not None and (isinstance(occurred_at_ms, bool) or not isinstance(occurred_at_ms, int)
                                           or occurred_at_ms < 0):
            raise ValueError("Invalid WeCom frame.")
        return UnboundChannelMessage(
            channel=WECOM_CHANNEL,
            external_account_id=self._external_account_id,
            conversation_kind=conversation_kind,
            external_user_id=userid.strip(),
            external_conversation_id=conversation_id,
            external_message_id=msgid.strip(),
            kind=kind,
            text=text,
            occurred_at_ms=occurred_at_ms,
        )

    def decode_text_frame(self, frame: Mapping[str, Any]) -> InboundMessage:
        """Legacy tenant-bound helper retained for pre-R2B callers/tests.

        New services must use :meth:`decode_frame` followed by ``bind_message``.
        """
        if self._tenant_id is None:
            raise ValueError("Invalid WeCom frame.")
        legacy = WeComChannelAdapter(self._tenant_id, external_account_id="legacy").decode_frame(frame)
        if legacy.kind != "text" or legacy.text is None:
            raise ValueError("Unsupported message type.")
        return InboundMessage(
            tenant_id=self._tenant_id,
            channel=WECOM_CHANNEL,
            external_user_id=legacy.external_user_id,
            external_conversation_id=legacy.external_conversation_id,
            external_message_id=legacy.external_message_id,
            text=legacy.text,
        )

    def encode_event(self, event: PublicChannelEvent) -> WeComReply | None:
        """Convert a public channel event to a :class:`WeComReply`.

        - ``delta`` → non-finished text chunk
        - ``done`` → finished marker
        - ``error`` → safe fixed text, finished
        - ``tool`` → ``None`` (suppressed; no tool details leak)
        """
        if event.type == "tool":
            return None
        if event.type == "delta":
            text = event.data if isinstance(event.data, str) else ""
            return WeComReply(text=text, finished=False)
        if event.type == "done":
            return WeComReply(text="", finished=True)
        if event.type == "error":
            return WeComReply(text=_SAFE_ERROR_TEXT, finished=True)
        if event.type == "approval":
            data = event.data if isinstance(event.data, dict) else {}
            raw = data.get("approval_id")
            try:
                return WeComReply(text=pending_reply_for(uuid.UUID(str(raw))), finished=False)
            except (TypeError, ValueError):
                return WeComReply(text=_SAFE_ERROR_TEXT, finished=True)
        return None


__all__ = [
    "WECOM_CHANNEL",
    "WeComChannelAdapter",
    "WeComReply",
]
