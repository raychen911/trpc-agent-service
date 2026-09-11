"""WeCom AI Bot long-connection service — bridges SDK frames to the ChannelIngress contract."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Callable
from contextlib import aclosing
from typing import Any

from trpc_service.channels.binding import ChannelBinding
from trpc_service.channels.delivery import SdkReplySender, record_terminal_delivery
from trpc_service.channels.ingress import ChannelIngress
from trpc_service.channels.order_gate import ChannelOrderGateUnavailableError
from trpc_service.channels.policy import CHANNEL_TEXT_LIMIT, UNSUPPORTED_MESSAGE_REPLY, bind_message
from trpc_service.channels.wecom.adapter import WeComChannelAdapter, WeComReply
from trpc_service.channels.wecom.sdk import WeComClient, create_wecom_client
from trpc_service.channels.wecom.settings import WeComSettings
from trpc_service.telemetry.runtime import (
    ATTR_REPLY_COUNT,
    ATTR_RESULT,
    SPAN_CHANNEL_RECEIVE,
    SPAN_CHANNEL_REPLY,
    safe_span,
)

logger = logging.getLogger(__name__)

AUTH_TIMEOUT_SECONDS = 30.0
REPLY_FLUSH_INTERVAL_SECONDS = 0.2


class WeComAuthenticationError(Exception):
    """Raised when WeCom SDK authentication fails or times out."""


class WeComAibotService:
    """Manages the WeCom SDK connection and routes text frames to the agent pipeline.

    Each inbound text frame triggers exactly one ``ChannelIngress.stream()``
    call.  Public events are forwarded as SDK streaming replies.  Tool events are
    suppressed; error events produce a safe fixed text.
    """

    def __init__(
        self,
        settings: WeComSettings,
        client: WeComClient,
        ingress: ChannelIngress,
        auth_timeout: float = AUTH_TIMEOUT_SECONDS,
        tracer: object | None = None,
        reply_flush_interval: float = REPLY_FLUSH_INTERVAL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        binding: ChannelBinding | None = None,
        order_gate: object | None = None,
    ) -> None:
        self._settings = settings
        self._client = client
        self._ingress = ingress
        if not isinstance(binding, ChannelBinding):
            raise ValueError("invalid WeCom channel binding")
        self._binding = binding
        if self._binding.channel != "wecom" or self._binding.external_account_id != settings.bot_id:
            raise ValueError("invalid WeCom channel binding")
        self._adapter = WeComChannelAdapter(external_account_id=self._binding.external_account_id)
        self._order_gate = order_gate
        # Stage 6B1: one channel.receive root per accepted inbound frame,
        # closed by one child channel.reply span (fixed result category and
        # reply count only — no text, ids or secrets).
        self._tracer = tracer
        self._request_count = 0
        self._closed = False
        self._auth_timeout = auth_timeout
        self._reply_flush_interval = reply_flush_interval
        self._clock = clock

    @property
    def request_count(self) -> int:
        return self._request_count

    async def start(self) -> None:
        """Register callbacks, connect, and wait for authentication.

        Raises ``WeComAuthenticationError`` if authentication does not complete
        within the configured timeout.  On failure the SDK connection is closed.
        """
        auth_event = asyncio.Event()

        def _on_auth() -> None:
            auth_event.set()

        await self._client.on_text(self._on_text_frame)
        await self._client.on_authenticated(_on_auth)
        await self._client.connect()

        try:
            await asyncio.wait_for(auth_event.wait(), timeout=self._auth_timeout)
        except asyncio.TimeoutError:
            await self.close()
            raise WeComAuthenticationError("WeCom SDK authentication timed out.")

        # Acceptance-observation records use WARNING: service processes have no
        # configured root handler, so logging.lastResort only surfaces WARNING+
        # (see design "在当前日志级别看到这些固定记录").  They carry no payloads.
        logger.warning("WeCom AI Bot authenticated and started (channel=wecom)")

    async def close(self) -> None:
        """Close the SDK connection.  Idempotent."""
        if self._closed:
            return
        self._closed = True
        await self._client.close()
        logger.info(
            "WeCom AI Bot service closed (channel=wecom, requests=%d)",
            self._request_count,
        )

    async def _on_text_frame(self, frame: dict[str, Any]) -> None:
        """SDK callback for text messages — delegates to handle_text_frame."""
        await self.handle_text_frame(frame)

    async def handle_text_frame(self, frame: dict[str, Any]) -> None:
        """Process one inbound text frame through the agent pipeline.

        Calls ingress.stream() exactly once.  Forwards delta/done/error events
        as SDK streaming replies.  Tool events are suppressed.  SDK reply
        failures are logged but do not re-trigger the agent.

        Tracing: rejected frames create nothing; an accepted frame gets one
        ``channel.receive`` root span (IM callbacks carry no W3C context) and
        exactly one child ``channel.reply`` span recording only the fixed
        terminal category plus the count of successful reply writes.
        """
        try:
            unbound = self._adapter.decode_frame(frame)
        except ValueError:
            logger.warning("WeCom frame rejected (channel=wecom)")
            return

        if unbound.kind != "text":
            await self._reply_unsupported(frame)
            return
        try:
            inbound = bind_message(unbound, self._binding)
        except ValueError:
            logger.warning("WeCom frame rejected (channel=wecom)")
            return
        if self._order_gate is None and unbound.occurred_at_ms is not None:
            # Timed events require the shared Redis watermark.  Only an IM
            # platform that omits time may use the existing session lease as
            # its arrival-order serializer.
            logger.warning("WeCom frame rejected (channel=wecom)")
            return
        if self._order_gate is not None:
            try:
                accepted = await self._order_gate.accept(
                    inbound.tenant_id,
                    self._binding.binding_id,
                    unbound.external_conversation_id,
                    unbound.occurred_at_ms,
                    unbound.external_message_id,
                )
            except ChannelOrderGateUnavailableError:
                logger.warning("WeCom frame rejected (channel=wecom)")
                return
            if not accepted:
                logger.warning("WeCom frame rejected (channel=wecom)")
                return

        self._request_count += 1

        if self._tracer is None:
            execution = self._ingress.stream(inbound)
            category, event_count = await self._run_reply_chain(frame, execution)
            await self._record_terminal_delivery(execution, category)
            return

        with safe_span(self._tracer, SPAN_CHANNEL_RECEIVE):
            with safe_span(self._tracer, SPAN_CHANNEL_REPLY) as reply_span:
                execution = self._ingress.stream(inbound)
                category, event_count = await self._run_reply_chain(frame, execution)
                await self._record_terminal_delivery(execution, category)
                if reply_span is not None:
                    try:
                        reply_span.set_attribute(ATTR_RESULT, category)
                        reply_span.set_attribute(ATTR_REPLY_COUNT, event_count)
                    except Exception:
                        pass

    async def _reply_unsupported(self, frame: dict[str, Any]) -> None:
        """Reject media locally: no ingress, Worker, download or forwarding."""
        try:
            await self._client.reply_stream(
                frame,
                stream_id=f"wecom-stream-{uuid.uuid4().hex[:12]}",
                text=UNSUPPORTED_MESSAGE_REPLY,
                finished=True,
            )
        except Exception:
            logger.warning("WeCom SDK reply failed (channel=wecom, event_type=unsupported)")

    async def _record_terminal_delivery(self, execution, category: str) -> None:
        await record_terminal_delivery(
            self._ingress,
            execution,
            category,
            failure_categories=frozenset({"reply_failed"}),
        )

    async def _run_reply_chain(self, frame: dict[str, Any], execution) -> tuple[str, int]:
        """Drive one reply chain; returns (fixed terminal category, count).

        Categories: done | error | reply_failed (a mid-chain SDK write failed)
        | stream_failed (ingress raised) | missing (no terminal event).
        Behavior is identical to the pre-6B1 implementation; the former bare
        ``return``s became categorized returns.
        """
        stream_id = f"wecom-stream-{uuid.uuid4().hex[:12]}"
        logger.warning("WeCom text frame accepted (stream=%s)", stream_id)

        # WeCom stream `content` is a full snapshot of the message, not an
        # appended delta (official example sends the complete text with
        # finish=True).  Accumulate within this single inbound-frame session;
        # handler-local state keeps concurrent streams isolated.
        event_count = 0
        accumulated_text = ""
        segment_closed = False
        last_flush_at: float | None = None
        sender = SdkReplySender()

        async def send(text: str, *, finished: bool) -> bool:
            nonlocal event_count, last_flush_at
            if not await sender.write(
                    self._client.reply_stream, frame, stream_id=stream_id, text=text, finished=finished):
                return False
            last_flush_at = self._clock()
            event_count += 1
            return True

        try:
            # aclosing: every early return below closes the ingress stream
            # immediately, so downstream CLIENT/agent spans end at once.
            async with aclosing(execution) as events:
                async for event in events:
                    reply: WeComReply | None = self._adapter.encode_event(event)
                    if reply is None:
                        continue
                    if event.type in ("delta", "approval"):
                        accumulated_text += reply.text
                        sent_full_segment = False
                        while len(accumulated_text) >= CHANNEL_TEXT_LIMIT:
                            outgoing_text = accumulated_text[:CHANNEL_TEXT_LIMIT]
                            accumulated_text = accumulated_text[CHANNEL_TEXT_LIMIT:]
                            if not await send(outgoing_text, finished=True):
                                logger.warning("WeCom SDK reply failed (channel=wecom, event_type=%s)", event.type)
                                return "reply_failed", event_count
                            stream_id = f"wecom-stream-{uuid.uuid4().hex[:12]}"
                            sent_full_segment = True
                        segment_closed = sent_full_segment and not accumulated_text
                        # A full segment has already been committed.  Hold its
                        # trailing partial text for the next delta/done so a
                        # 4001-character response becomes two valid messages,
                        # never an oversized WeCom snapshot.
                        if sent_full_segment:
                            continue
                        outgoing_text = accumulated_text
                    elif event.type == "done":
                        if not accumulated_text and segment_closed:
                            logger.warning(
                                "WeCom reply chain completed (terminal=done, stream=%s, events=%d)",
                                stream_id,
                                event_count,
                            )
                            return "done", event_count
                        outgoing_text = accumulated_text
                    else:  # error terminal: fixed safe text only, never mixed body
                        outgoing_text = reply.text
                    # Send the first visible text immediately.  Thereafter,
                    # coalesce token-sized deltas so a slow SDK write cannot
                    # turn hundreds of model chunks into hundreds of platform
                    # calls.  Approval and terminal events always bypass the
                    # interval; ``done`` therefore flushes the newest snapshot.
                    if (event.type == "delta" and last_flush_at is not None
                            and self._clock() - last_flush_at < self._reply_flush_interval):
                        continue
                    if not await send(outgoing_text, finished=reply.finished):
                        logger.warning(
                            "WeCom SDK reply failed (channel=wecom, event_type=%s)",
                            event.type,
                        )
                        return "reply_failed", event_count
                    if event.type == "error":
                        logger.warning(
                            "WeCom reply chain ended (terminal=error, stream=%s, events=%d)",
                            stream_id,
                            event_count,
                        )
                        return "error", event_count
                    if event.type == "done":
                        logger.warning(
                            "WeCom reply chain completed (terminal=done, stream=%s, events=%d)",
                            stream_id,
                            event_count,
                        )
                        return "done", event_count
        except Exception:
            logger.warning("WeCom ingress stream failed (channel=wecom)")
            try:
                await self._client.reply_stream(
                    frame,
                    stream_id=stream_id,
                    text="An internal error occurred.",
                    finished=True,
                )
                event_count += 1
            except Exception:
                logger.warning("WeCom SDK error reply failed (channel=wecom)")
            return "stream_failed", event_count

        logger.warning(
            "WeCom reply chain incomplete (terminal=missing, stream=%s, events=%d)",
            stream_id,
            event_count,
        )
        try:
            await self._client.reply_stream(
                frame,
                stream_id=stream_id,
                text="An internal error occurred.",
                finished=True,
            )
            event_count += 1
        except Exception:
            logger.warning("WeCom SDK incomplete-stream reply failed (channel=wecom)")
        return "missing", event_count


def create_wecom_service(
    settings: WeComSettings | None,
    ingress: ChannelIngress,
    client: WeComClient | None = None,
    tracer: object | None = None,
    binding: ChannelBinding | None = None,
    order_gate: object | None = None,
) -> WeComAibotService | None:
    """Create the WeCom service if settings are available.

    Returns ``None`` when ``settings`` is ``None`` (feature disabled).
    When ``client`` is ``None`` and settings are present, creates one via
    :func:`create_wecom_client`.
    """
    if settings is None:
        return None
    if client is None:
        client = create_wecom_client(settings)
    return WeComAibotService(
        settings=settings,
        client=client,
        ingress=ingress,
        tracer=tracer,
        binding=binding,
        order_gate=order_gate,
    )


__all__ = [
    "AUTH_TIMEOUT_SECONDS",
    "REPLY_FLUSH_INTERVAL_SECONDS",
    "WeComAibotService",
    "WeComAuthenticationError",
    "create_wecom_service",
]
