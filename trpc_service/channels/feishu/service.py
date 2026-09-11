"""Feishu AI Bot long-connection service — bridges SDK frames to the ChannelIngress contract.

P0 fix: use streaming writer (append/finish) instead of per-delta reply().
"""

from __future__ import annotations

import logging
import uuid
from contextlib import aclosing

from trpc_service.channels.binding import ChannelBinding
from trpc_service.channels.delivery import SdkReplySender, record_terminal_delivery
from trpc_service.channels.feishu.adapter import _SAFE_ERROR_TEXT, FeishuChannelAdapter
from trpc_service.channels.feishu.sdk import FeishuClient, FeishuInboundFrame, create_feishu_client
from trpc_service.channels.feishu.settings import FeishuSettings
from trpc_service.channels.ingress import ChannelIngress
from trpc_service.channels.policy import UNSUPPORTED_MESSAGE_REPLY, bind_message, split_text
from trpc_service.telemetry.runtime import (
    ATTR_REPLY_COUNT,
    ATTR_RESULT,
    SPAN_CHANNEL_RECEIVE,
    SPAN_CHANNEL_REPLY,
    safe_span,
)

logger = logging.getLogger(__name__)


class FeishuAibotService:
    """Manages the Feishu SDK connection and routes text frames to the agent pipeline.

    Each inbound text frame triggers exactly one ``ChannelIngress.stream()``
    call.  Public events are forwarded via a streaming writer (append/finish).
    Tool events are suppressed; error events produce a safe fixed text.
    """

    def __init__(
        self,
        settings: FeishuSettings,
        client: FeishuClient,
        ingress: ChannelIngress,
        *,
        binding: ChannelBinding,
        order_gate: object | None,
        tracer: object | None = None,
    ) -> None:
        if (not isinstance(binding, ChannelBinding) or binding.channel != "feishu"
                or binding.external_account_id != settings.app_id):
            raise ValueError("invalid Feishu channel binding")
        self._settings = settings
        self._binding = binding
        self._client = client
        self._ingress = ingress
        self._order_gate = order_gate
        self._adapter = FeishuChannelAdapter()
        # Stage 6B1: channel.receive root + channel.reply child per accepted
        # inbound frame (fixed category + successful-write count only).
        self._tracer = tracer
        self._request_count = 0
        self._closed = False

    @property
    def request_count(self) -> int:
        return self._request_count

    async def start(self) -> None:
        """Register callbacks and connect until SDK is ready."""
        await self._client.on_raw_event(self._on_text_frame)
        await self._client.connect_until_ready(timeout_seconds=30.0)
        logger.info("Feishu AI Bot connected and ready (channel=feishu)")

    async def close(self) -> None:
        """Close the SDK connection.  Idempotent."""
        if self._closed:
            return
        self._closed = True
        await self._client.close()
        logger.info(
            "Feishu AI Bot service closed (channel=feishu, requests=%d)",
            self._request_count,
        )

    async def _on_text_frame(self, frame: FeishuInboundFrame) -> None:
        """SDK callback for all supported frame kinds."""
        await self.handle_frame(frame)

    async def handle_frame(self, frame: FeishuInboundFrame) -> None:
        """Process one inbound text frame through the agent pipeline.

        Calls ingress.stream() exactly once.  Forwards delta/done/error events
        via a streaming writer (append/finish).  Tool events are suppressed.
        SDK stream failures are logged but do not re-trigger the agent.
        Only terminal=done logs "completed"; all other terminals log without
        recording completion.

        Tracing mirrors the WeCom service: rejected frames create nothing;
        accepted frames get one ``channel.receive`` root plus a child
        ``channel.reply`` span with fixed category and write count.
        """
        try:
            message = self._adapter.decode_frame(frame)
        except ValueError:
            logger.warning("Feishu frame rejected (channel=feishu)")
            return

        if message.kind != "text":
            await self._reply_unsupported(frame)
            return
        try:
            inbound = bind_message(message, self._binding)
        except ValueError:
            logger.warning("Feishu binding rejected frame (channel=feishu)")
            return
        if message.occurred_at_ms is not None:
            if self._order_gate is None:
                logger.warning("Feishu order gate rejected frame (channel=feishu)")
                return
            try:
                accepted = await self._order_gate.accept(
                    self._binding.tenant_id,
                    self._binding.binding_id,
                    message.external_conversation_id,
                    message.occurred_at_ms,
                    message.external_message_id,
                )
            except Exception:
                logger.warning("Feishu order gate rejected frame (channel=feishu)")
                return
            if not accepted:
                logger.warning("Feishu out-of-order frame rejected (channel=feishu)")
                return

        self._request_count += 1

        if self._tracer is None:
            execution = self._ingress.stream(inbound)
            category, write_count = await self._run_reply_chain(frame, execution)
            await self._record_terminal_delivery(execution, category)
            return

        with safe_span(self._tracer, SPAN_CHANNEL_RECEIVE):
            with safe_span(self._tracer, SPAN_CHANNEL_REPLY) as reply_span:
                execution = self._ingress.stream(inbound)
                category, write_count = await self._run_reply_chain(frame, execution)
                await self._record_terminal_delivery(execution, category)
                if reply_span is not None:
                    try:
                        reply_span.set_attribute(ATTR_RESULT, category)
                        reply_span.set_attribute(ATTR_REPLY_COUNT, write_count)
                    except Exception:
                        pass

    async def handle_text_frame(self, frame: FeishuInboundFrame) -> None:
        """Compatibility name for SDK callers; all frame kinds share one path."""
        await self.handle_frame(frame)

    async def _reply_unsupported(self, frame: FeishuInboundFrame) -> None:
        """Reject media locally without constructing an ingress/Worker request."""
        try:
            writer = await self._client.open_reply_stream(frame)
            await writer.append(UNSUPPORTED_MESSAGE_REPLY)
            await writer.finish()
        except Exception:
            logger.warning("Feishu unsupported-message reply failed (channel=feishu)")

    async def _record_terminal_delivery(self, execution, category: str) -> None:
        await record_terminal_delivery(
            self._ingress,
            execution,
            category,
            failure_categories=frozenset({"append_failed", "finish_failed"}),
        )

    async def _run_reply_chain(self, frame: FeishuInboundFrame, execution) -> tuple[str, int]:
        """Drive one reply chain; returns (fixed terminal category, writes).

        Categories: done | error | append_failed | finish_failed |
        stream_failed | missing.  ``writes`` counts SUCCESSFUL SDK write
        operations (appends plus finishes).  Behavior is otherwise identical
        to the pre-6B1 implementation.
        """
        stream_id = f"feishu-stream-{uuid.uuid4().hex[:12]}"
        logger.info("Feishu text frame accepted (stream=%s)", stream_id)

        event_count = 0
        writer = None
        sender = SdkReplySender()

        async def _write(method, *args) -> bool:
            return await sender.write(method, *args)

        try:
            writer = await self._client.open_reply_stream(frame)
            # aclosing: every early return below closes the ingress stream
            # immediately, so downstream CLIENT/agent spans end at once.
            async with aclosing(execution) as events:
                async for event in events:
                    # Event→reply conversion is defined once, in the Adapter.
                    reply = self._adapter.encode_event(event)
                    if reply is None:
                        continue  # tool / unknown: never sent
                    if not reply.finished:
                        if reply.text:
                            for chunk in split_text(reply.text):
                                if not await _write(writer.append, chunk):
                                    logger.warning("Feishu SDK stream append failed (channel=feishu)")
                                    return "append_failed", event_count
                                event_count += 1
                        continue
                    if event.type == "done":
                        if not await _write(writer.finish):
                            logger.warning(
                                "Feishu SDK stream finish failed "
                                "(terminal=stream-failed, stream=%s, events=%d)",
                                stream_id,
                                event_count,
                            )
                            return "finish_failed", event_count
                        logger.info(
                            "Feishu reply chain completed (terminal=done, stream=%s, events=%d)",
                            stream_id,
                            event_count,
                        )
                        return "done", event_count + 1
                    # finished terminal carrying the Adapter's safe fixed text
                    terminal_writes = 0
                    if reply.text:
                        if not await _write(writer.append, reply.text):
                            logger.warning("Feishu SDK error stream failed (channel=feishu)")
                            return "append_failed", event_count
                        terminal_writes += 1
                    if not await _write(writer.finish):
                        logger.warning("Feishu SDK error stream failed (channel=feishu)")
                        return "finish_failed", event_count + terminal_writes
                    terminal_writes += 1
                    logger.warning(
                        "Feishu reply chain ended (terminal=error, stream=%s, events=%d)",
                        stream_id,
                        event_count,
                    )
                    return "error", event_count + terminal_writes
        except Exception:
            logger.warning("Feishu ingress stream failed (channel=feishu)")
            if writer is not None:
                return "stream_failed", event_count + await self._safe_finish(writer)
            return "stream_failed", event_count

        # The event stream ended without a terminal event (no done/error).
        logger.warning(
            "Feishu reply chain incomplete (terminal=missing, stream=%s, events=%d)",
            stream_id,
            event_count,
        )
        if writer is not None:
            writes = await self._safe_finish(writer, "Feishu SDK incomplete-stream failed (channel=feishu)")
            return "missing", event_count + writes
        return "missing", event_count

    @staticmethod
    async def _safe_finish(writer, log_msg: str = "Feishu SDK error stream failed (channel=feishu)") -> int:
        """Best-effort safe-text finish; returns successful write count."""
        writes = 0
        try:
            await writer.append(_SAFE_ERROR_TEXT)
            writes += 1
            await writer.finish()
            writes += 1
        except Exception:
            logger.warning(log_msg)
        return writes


def create_feishu_service(
    settings: FeishuSettings | None,
    ingress: ChannelIngress,
    *,
    binding: ChannelBinding | None = None,
    order_gate: object | None = None,
    client: FeishuClient | None = None,
    tracer: object | None = None,
) -> FeishuAibotService | None:
    """Create the Feishu service if settings are available.

    Returns ``None`` when ``settings`` is ``None`` (feature disabled).
    When ``client`` is ``None`` and settings are present, creates one via
    :func:`create_feishu_client`.
    """
    if settings is None:
        return None
    if binding is None:
        raise ValueError("Feishu binding configuration is required")
    if client is None:
        client = create_feishu_client(settings)
    return FeishuAibotService(
        settings=settings,
        client=client,
        ingress=ingress,
        binding=binding,
        order_gate=order_gate,
        tracer=tracer,
    )


__all__ = [
    "FeishuAibotService",
    "create_feishu_service",
]
