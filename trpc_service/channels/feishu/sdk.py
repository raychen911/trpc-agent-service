"""Feishu SDK facade — the only module allowed to touch ``lark_channel``.

P0 architecture (verified against installed lark-channel-sdk 1.4.0):

- Inbound: ``FeishuChannel.on_raw_event("im.message.receive_v1", wrapper)``.
  The SDK dispatches a **dict** payload (``_coerce.obj_to_dict``) after
  signature verification/decryption but **outside** the safety pipeline — no
  SeenCache dedup (``DedupConfig.enabled`` cannot disable it), and redelivered
  events run the handler again.  That guarantees duplicate platform
  ``message_id`` deliveries reach the Stage 4C PostgreSQL receipt, which owns
  exactly-once semantics.
- Only ``chat_type == "p2p"`` + ``message_type == "text"`` are accepted;
  everything else is silently rejected without logging platform fields.
- Outbound: one streaming single-card reply via
  ``stream(chat_id, {"markdown": producer}, {"reply_to": message_id})``.
  The producer consumes an asyncio queue; ``append``/``finish`` on the writer
  are acknowledged per chunk, and a normal producer return completes the card.
  ``reply()`` is never used (it has no finished semantics and would fragment
  the conversation).
- ``lark_channel`` can be preloaded synchronously before an application event
  loop starts; the SDK captures an event loop at import time.  Client creation
  retains a lazy-import fallback for non-CLI callers.
  SDK controller/result types must not escape this module.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol, runtime_checkable

from .settings import FeishuSettings
from trpc_service.channels.delivery import ChannelSendError

logger = logging.getLogger(__name__)

_RAW_EVENT_TYPE = "im.message.receive_v1"


def preload_feishu_sdk() -> None:
    """Import the optional SDK before Uvicorn creates its event loop.

    ``lark-channel-sdk`` captures an event loop during module import and later
    drives it from its WebSocket worker.  Importing it for the first time from
    FastAPI lifespan instead captures Uvicorn's already-running loop.  Missing
    optional SDK installations remain a client-creation error, as before.
    """
    try:
        import lark_channel as _lark_channel  # noqa: F401
    except ImportError:
        return


def _parse_occurred_at_ms(value: object) -> int | None:
    """Return a trustworthy non-negative millisecond timestamp when supplied."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and value.isdecimal():
        parsed = int(value)
        return parsed if parsed >= 0 else None
    return None


@dataclass(frozen=True, slots=True)
class FeishuInboundFrame:
    """Standardized authenticated input without tenant authority."""

    external_user_id: str
    external_conversation_id: str
    external_message_id: str
    text: str | None
    external_account_id: str = ""
    conversation_kind: str = "direct"
    kind: str = "text"
    occurred_at_ms: int | None = None


class FeishuReplyWriter(Protocol):
    """Append chunks to a streaming reply and finish it.

    Created by :meth:`FeishuClient.open_reply_stream`; the underlying SDK
    controller type never escapes ``sdk.py``.
    """

    async def append(self, text: str) -> None:
        """Append a text chunk.  Raises if the SDK stream write failed."""
        ...

    async def finish(self) -> None:
        """Finish the stream normally.  Idempotent; never hangs after failure."""
        ...


@runtime_checkable
class FeishuClient(Protocol):
    """Protocol for the Feishu SDK facade (real SDK or test double)."""

    async def on_raw_event(self, handler: Callable[[FeishuInboundFrame], Awaitable[None]]) -> None:
        """Register an async handler receiving parsed :class:`FeishuInboundFrame`."""
        ...

    async def connect_until_ready(self, timeout_seconds: float) -> None:
        ...

    async def close(self) -> None:
        ...

    async def open_reply_stream(self, frame: FeishuInboundFrame) -> FeishuReplyWriter:
        ...


def _parse_raw_event(payload: Any, external_account_id: str) -> FeishuInboundFrame | None:
    """Parse the SDK's raw dict payload into a frame, or ``None`` to reject.

    Payload shape (``obj_to_dict(P2ImMessageReceiveV1)``)::

        {"event": {"sender": {"sender_id": {"open_id": ...}},
                   "message": {"message_id", "chat_id", "chat_type",
                               "message_type", "content": "<json str>"}}}

    Platform raw fields exist only transiently here; nothing is ever logged.
    """
    try:
        if not isinstance(payload, dict):
            return None
        event = payload.get("event")
        if not isinstance(event, dict):
            return None
        message = event.get("message")
        if not isinstance(message, dict):
            return None
        chat_type = message.get("chat_type")
        if chat_type not in {"p2p", "group"}:
            return None
        conversation_kind = "direct" if chat_type == "p2p" else "group"

        message_id = message.get("message_id") or ""
        chat_id = message.get("chat_id") or ""
        if not message_id or not chat_id:
            return None

        message_type = message.get("message_type")
        kind = {"text": "text", "image": "image", "file": "file"}.get(message_type, "unsupported")
        text: str | None = None
        if kind == "text":
            content = message.get("content")
            if not isinstance(content, str):
                return None
            parsed = json.loads(content)
            if not isinstance(parsed, dict):
                return None
            parsed_text = parsed.get("text")
            if not isinstance(parsed_text, str) or not parsed_text.strip():
                return None
            text = parsed_text.strip()

        sender = event.get("sender")
        if not isinstance(sender, dict):
            return None
        sender_id = sender.get("sender_id")
        if not isinstance(sender_id, dict):
            return None
        open_id = sender_id.get("open_id") or ""
        if not open_id:
            return None

        occurred_at_ms = _parse_occurred_at_ms(message.get("create_time"))

        return FeishuInboundFrame(
            external_account_id=external_account_id,
            external_user_id=open_id,
            external_conversation_id=chat_id,
            external_message_id=message_id,
            conversation_kind=conversation_kind,
            kind=kind,
            text=text,
            occurred_at_ms=occurred_at_ms,
        )
    except Exception:
        return None


class _QueueReplyWriter:
    """Bridges the product writer API to the SDK markdown stream producer.

    ``append``/``finish`` enqueue control items and await per-chunk
    acknowledgement from the producer coroutine, so SDK write failures
    propagate to the caller.  A normal :meth:`finish` lets the producer
    return, which completes the streaming card inside the SDK.
    """

    _SENTINEL = object()

    def __init__(self, task: "asyncio.Task[Any]", queue: "asyncio.Queue[Any]") -> None:
        self._task = task
        self._queue = queue
        self._finished = False

    async def _join_task(self) -> None:
        """Wait for the stream task and turn its outcome into a raised error.

        The task exception is always retrieved here, so asyncio never logs
        ``Task exception was never retrieved``.  A cancelled internal task
        becomes a ``ConnectionError`` so cancellation of the *SDK task* can
        never be mistaken for cancellation of the caller.
        """
        if not self._task.done():
            await asyncio.wait({self._task})
        if self._task.cancelled():
            raise ChannelSendError(sent=False)
        exc = self._task.exception()
        if exc is not None:
            if isinstance(exc, ChannelSendError):
                raise exc
            raise ChannelSendError(sent=False) from None

    async def _submit(self, item: Any) -> None:
        """Enqueue one item and wait for the producer ACK **or** task death.

        If the SDK task finishes before the ACK arrives (e.g. it failed
        before the producer consumed the queue), the task outcome is raised
        immediately instead of waiting on an ACK that can never come.
        """
        loop = asyncio.get_running_loop()
        ack: "asyncio.Future[None]" = loop.create_future()
        await self._queue.put((item, ack))
        done, _ = await asyncio.wait({ack, self._task}, return_when=asyncio.FIRST_COMPLETED)
        if ack in done:
            exc = None if ack.cancelled() else ack.exception()
            if exc is not None:
                raise exc
            return
        await self._join_task()
        # The task ended cleanly without acknowledging this chunk; the item
        # was abandoned.  Treat it as a failed write, never as success.
        raise ChannelSendError(sent=False)

    async def _cleanup_after_failure(self) -> None:
        """Ensure the task is finished and its exception retrieved."""
        if not self._task.done():
            self._task.cancel()
        try:
            await self._join_task()
        except BaseException:
            pass

    async def append(self, text: str) -> None:
        if self._finished:
            return
        if not text:
            return
        try:
            await self._submit(text)
        except BaseException:
            await self._cleanup_after_failure()
            raise

    async def finish(self) -> None:
        """Return success only after the SDK stream task truly completed.

        Sentinel-submission errors, the producer's normal return followed by
        a failing card-completion step, and any task exception all propagate
        — the service relies on this to keep ``terminal=done`` honest.
        """
        if self._finished:
            return
        self._finished = True
        try:
            await self._submit(self._SENTINEL)
        except BaseException:
            await self._cleanup_after_failure()
            raise
        try:
            await self._join_task()
        except BaseException:
            await self._cleanup_after_failure()
            raise


class FacadeClient:
    """Concrete facade delegating to the real SDK ``FeishuChannel``."""

    def __init__(self, sdk_channel: Any, external_account_id: str = "feishu_default") -> None:
        self._sdk = sdk_channel
        self._external_account_id = external_account_id
        self._closed = False
        self._unsubscribe: Callable[[], None] | None = None
        self._stream_tasks: "set[asyncio.Task[Any]]" = set()

    async def on_raw_event(self, handler: Callable[[FeishuInboundFrame], Awaitable[None]]) -> None:
        """Register the product handler on the SDK raw event path (no dedup)."""

        async def wrapper(payload: Any) -> None:
            frame = _parse_raw_event(payload, self._external_account_id)
            if frame is None:
                return
            await handler(frame)

        self._unsubscribe = self._sdk.on_raw_event(_RAW_EVENT_TYPE, wrapper)

    async def connect_until_ready(self, timeout_seconds: float) -> None:
        await self._sdk.connect_until_ready(timeout=timeout_seconds)

    async def close(self) -> None:
        """Unsubscribe, cancel pending streams and disconnect.  Idempotent."""
        if self._closed:
            return
        self._closed = True
        if self._unsubscribe is not None:
            try:
                self._unsubscribe()
            except Exception:
                pass
        for task in list(self._stream_tasks):
            if not task.done():
                task.cancel()
        pending = [t for t in self._stream_tasks if not t.done()]
        await self._sdk.disconnect()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def open_reply_stream(self, frame: FeishuInboundFrame) -> FeishuReplyWriter:
        """Start a markdown streaming reply and return its writer."""
        queue: "asyncio.Queue[Any]" = asyncio.Queue()

        async def producer(controller: Any) -> None:
            while True:
                item, ack = await queue.get()
                if item is _QueueReplyWriter._SENTINEL:
                    ack.set_result(None)
                    return
                try:
                    await controller.append(item)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if not ack.done():
                        ack.set_exception(ChannelSendError(sent=False))
                    raise ChannelSendError(sent=False) from None
                ack.set_result(None)

        task = asyncio.create_task(
            self._sdk.stream(
                frame.external_conversation_id,
                {"markdown": producer},
                {"reply_to": frame.external_message_id},
            ))
        self._stream_tasks.add(task)
        task.add_done_callback(self._stream_tasks.discard)
        return _QueueReplyWriter(task, queue)


def create_feishu_client(settings: FeishuSettings) -> FeishuClient:
    """Create a :class:`FacadeClient` backed by the real SDK.

    The only place that imports/constructs ``FeishuChannel`` — lazily, so
    unit tests never trigger SDK import-time warnings.
    """
    try:
        from lark_channel import FeishuChannel
    except ImportError as exc:
        raise RuntimeError("Feishu SDK is not installed. Install lark-channel-sdk.") from exc
    channel = FeishuChannel(app_id=settings.app_id, app_secret=settings.app_secret)
    return FacadeClient(channel, external_account_id=settings.app_id)


__all__ = [
    "FacadeClient",
    "FeishuClient",
    "FeishuInboundFrame",
    "FeishuReplyWriter",
    "create_feishu_client",
    "preload_feishu_sdk",
]
