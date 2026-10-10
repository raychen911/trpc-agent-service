"""Durable outbound delivery with explicit ambiguous-result handling."""

from __future__ import annotations

import asyncio
import logging
import math
import random
from collections.abc import Callable
from datetime import timedelta

from pydantic import ValidationError

from trpc_service.channels.base import ChannelAdapter
from trpc_service.channels.envelopes import DeliveryReceipt, DeliveryStatus, OutboundEnvelope
from trpc_service.metrics.privacy import extract_trace_context
from trpc_service.metrics.prometheus import DELIVERIES, QUEUE_DEPTH
from trpc_service.metrics.telemetry import get_tracer, mark_span_error
from trpc_service.storage.models import DeliveryAttempt, OutboxRecord
from trpc_service.storage.protocols import (
    DeliveryInProgress,
    FencingConflict,
    RuntimeRepository,
)
from trpc_service.tenant.models import Channel, ChannelBinding

logger = logging.getLogger(__name__)


def _default_retry_jitter(base_seconds: float) -> float:
    """Apply equal jitter to locally generated backoff delays."""

    return random.uniform(base_seconds / 2, base_seconds)  # noqa: S311


class ChannelDispatcher:
    def __init__(
        self,
        repository: RuntimeRepository,
        adapters: dict[Channel, ChannelAdapter],
        *,
        owner_id: str,
        event_type: str,
        max_attempts: int = 5,
        batch_limit: int = 25,
        lease_seconds: float = 30.0,
        retry_jitter: Callable[[float], float] | None = None,
        binding_ready: Callable[[ChannelBinding], bool] | None = None,
    ) -> None:
        self._repository = repository
        self._adapters = dict(adapters)
        self._owner_id = owner_id
        self._event_type = event_type
        self._max_attempts = max_attempts
        if batch_limit < 1:
            raise ValueError("batch_limit must be positive")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self._batch_limit = batch_limit
        self._lease_seconds = lease_seconds
        self._retry_jitter = retry_jitter or _default_retry_jitter
        self._binding_ready = binding_ready

    async def dispatch_once(self, stop_event: asyncio.Event | None = None) -> int:
        records = await self._repository.claim_outbox(
            event_type=self._event_type,
            owner_id=self._owner_id,
            limit=self._batch_limit,
            lease_for=timedelta(seconds=self._lease_seconds),
        )
        completed = 0
        QUEUE_DEPTH.labels(queue="outbound").set(len(records))
        try:
            for index, record in enumerate(records):
                if stop_event is not None and stop_event.is_set():
                    for pending in records[index:]:
                        try:
                            await self._repository.release_outbox(
                                pending.tenant_id,
                                pending.outbox_id,
                                owner_id=self._owner_id,
                                delay=timedelta(0),
                                error_type="dispatcher_draining",
                            )
                        except Exception as exc:
                            logger.warning(
                                "channel dispatcher could not release pending item",
                                extra={
                                    "error_type": type(exc).__name__,
                                    "safe_code": "channel_dispatcher_release_failed",
                                },
                            )
                    break
                try:
                    processed = await self._dispatch_record(record)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "channel delivery item isolated",
                        extra={
                            "error_type": type(exc).__name__,
                            "safe_code": "channel_delivery_item_isolated",
                        },
                    )
                    processed = await self._dead_letter_record(
                        record, reason="dispatcher_item_failed"
                    )
                if processed:
                    completed += 1
        finally:
            QUEUE_DEPTH.labels(queue="outbound").set(0)
        return completed

    async def _dispatch_record(self, record: OutboxRecord) -> bool:
        try:
            envelope = OutboundEnvelope.model_validate(record.payload)
        except (ValidationError, TypeError, ValueError) as exc:
            logger.warning(
                "invalid outbound payload quarantined",
                extra={
                    "error_type": type(exc).__name__,
                    "safe_code": "invalid_outbound_payload",
                },
            )
            DELIVERIES.labels(channel="unknown", outcome="poison").inc()
            return await self._dead_letter_record(record, reason="invalid_outbound_payload")
        channel = envelope.channel.value
        carrier = record.trace_headers or envelope.trace_headers
        outcome = "failed"
        delivery_recorded = False
        with get_tracer().start_as_current_span(
            "im.send",
            context=extract_trace_context(carrier),
            attributes={"channel": channel},
        ) as span:
            try:
                route = await self._repository.resolve_binding(envelope.binding_id)
                if (
                    route is None
                    or route.binding.binding_id != envelope.binding_id
                    or route.binding.tenant_id != record.tenant_id
                    or route.binding.tenant_id != envelope.tenant_id
                    or route.binding.channel != envelope.channel
                ):
                    await self._retry_or_dead_letter(
                        record,
                        reason="binding_unavailable",
                        delay=timedelta(minutes=5),
                    )
                    DELIVERIES.labels(channel=channel, outcome="failed").inc()
                    delivery_recorded = True
                    return False
                adapter = self._adapters.get(envelope.channel)
                if adapter is None:
                    await self._retry_or_dead_letter(
                        record,
                        reason="adapter_unavailable",
                        delay=timedelta(minutes=5),
                    )
                    DELIVERIES.labels(channel=channel, outcome="failed").inc()
                    delivery_recorded = True
                    return False
                if self._binding_ready is not None and not self._binding_ready(route.binding):
                    # A standby WeCom replica must not consume provider-attempt
                    # budget while another replica owns the binding's WSS.
                    # Release the durable record before begin_delivery/send;
                    # the active owner (or this replica after takeover) will
                    # claim it on a later dispatcher cycle.
                    await self._repository.release_outbox(
                        record.tenant_id,
                        record.outbox_id,
                        owner_id=self._owner_id,
                        delay=timedelta(milliseconds=100),
                        error_type="adapter_standby",
                    )
                    outcome = "standby"
                    delivery_recorded = True
                    return False
                if self._supports_atomic_delivery():
                    try:
                        receipt, retrying, persisted = await self._dispatch_atomic(
                            record,
                            envelope,
                            adapter,
                            route.binding,
                        )
                    except DeliveryInProgress as progress:
                        receipt, persisted = await self._resolve_in_progress(
                            record, envelope, progress
                        )
                        retrying = False
                    except FencingConflict as exc:
                        outcome = "stale"
                        logger.info(
                            "channel delivery claim is no longer current",
                            extra={
                                "error_type": type(exc).__name__,
                                "safe_code": "channel_delivery_stale_claim",
                            },
                        )
                        return False
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        mark_span_error(span, type(exc).__name__)
                        outcome = "degraded"
                        logger.warning(
                            "channel delivery attempt could not start",
                            extra={
                                "error_type": type(exc).__name__,
                                "safe_code": "channel_delivery_attempt_failed",
                            },
                        )
                        return False
                    outcome = receipt.status.value
                    DELIVERIES.labels(channel=channel, outcome=outcome).inc()
                    # A provider result is known even if the durable finish
                    # failed.  Do not let the outer exception path perform a
                    # legacy blind retry; the outbox lease will be recovered
                    # by the next claim after the repository is available.
                    delivery_recorded = True
                    return bool(persisted and not retrying)
                receipt = await adapter.send(envelope, route.binding)
                if receipt.outbound_id != envelope.outbound_id:
                    receipt = receipt.model_copy(
                        update={
                            "outbound_id": envelope.outbound_id,
                            "status": DeliveryStatus.AMBIGUOUS,
                            "provider_code": "receipt_mismatch",
                            "retryable": False,
                        }
                    )
                outcome = receipt.status.value
                DELIVERIES.labels(channel=channel, outcome=outcome).inc()
                delivery_recorded = True
                retrying = (
                    receipt.status == DeliveryStatus.FAILED
                    and receipt.retryable
                    and record.attempts < self._max_attempts
                )
                await self._repository.record_delivery(record.tenant_id, receipt, retrying=retrying)
                if retrying:
                    await self._repository.release_outbox(
                        record.tenant_id,
                        record.outbox_id,
                        owner_id=self._owner_id,
                        delay=self._retry_delay(
                            receipt,
                            record,
                            jitter=self._retry_jitter,
                        ),
                        error_type=receipt.provider_code or "delivery_failed",
                    )
                elif receipt.status in {DeliveryStatus.FAILED, DeliveryStatus.AMBIGUOUS}:
                    await self._repository.dead_letter_outbox(
                        record,
                        owner_id=self._owner_id,
                        reason=receipt.provider_code or receipt.status.value,
                    )
                else:
                    await self._repository.mark_outbox_published(
                        record.tenant_id, record.outbox_id, owner_id=self._owner_id
                    )
                return not retrying
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                mark_span_error(span, type(exc).__name__)
                outcome = "degraded"
                logger.warning(
                    "channel delivery item degraded",
                    extra={
                        "error_type": type(exc).__name__,
                        "safe_code": "channel_delivery_item_failed",
                    },
                )
                # An exception after the provider call may mean that the
                # provider accepted the message.  Record an ambiguous result
                # when the repository is still reachable; never blind-retry.
                try:
                    receipt = DeliveryReceipt(
                        outbound_id=envelope.outbound_id,
                        status=DeliveryStatus.AMBIGUOUS,
                        provider_code="dispatcher_unknown",
                    )
                    await self._repository.record_delivery(
                        record.tenant_id, receipt, retrying=False
                    )
                    await self._repository.dead_letter_outbox(
                        record, owner_id=self._owner_id, reason="dispatcher_unknown"
                    )
                except Exception as persist_error:
                    logger.warning(
                        "channel delivery ambiguous result could not be persisted",
                        extra={
                            "error_type": type(persist_error).__name__,
                            "safe_code": "channel_delivery_persist_failed",
                        },
                    )
                return False
            finally:
                if not delivery_recorded:
                    DELIVERIES.labels(channel=channel, outcome="failed").inc()
                span.set_attribute("outcome", outcome)

    def _supports_atomic_delivery(self) -> bool:
        return callable(getattr(self._repository, "begin_delivery", None)) and callable(
            getattr(self._repository, "finish_delivery", None)
        )

    async def _dispatch_atomic(
        self,
        record: OutboxRecord,
        envelope: OutboundEnvelope,
        adapter: ChannelAdapter,
        binding: ChannelBinding,
    ) -> tuple[DeliveryReceipt, bool, bool]:
        attempt = await self._repository.begin_delivery(record, owner_id=self._owner_id)
        if not isinstance(attempt, DeliveryAttempt):
            raise TypeError("repository returned an invalid delivery attempt")
        if (
            attempt.tenant_id != record.tenant_id
            or attempt.owner_id != self._owner_id
            or attempt.outbound_id != envelope.outbound_id
        ):
            raise ValueError("repository returned a mismatched delivery attempt")
        try:
            raw_receipt = await adapter.send(envelope, binding)
            if not isinstance(raw_receipt, DeliveryReceipt):
                raise TypeError("adapter returned an invalid delivery receipt")
            receipt = self._normalize_receipt(raw_receipt, envelope)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Once the provider call has started, a transport exception does
            # not prove that the provider rejected the message.
            receipt = DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.AMBIGUOUS,
                provider_code="transport_unknown",
            )
        retrying = (
            receipt.status == DeliveryStatus.FAILED
            and receipt.retryable
            and attempt.attempt_number < self._max_attempts
        )
        if receipt.status == DeliveryStatus.FAILED and receipt.retryable and not retrying:
            receipt = receipt.model_copy(update={"retryable": False})
        attempt_record = record.model_copy(update={"attempts": attempt.attempt_number})
        retry_delay = self._retry_delay(receipt, attempt_record, jitter=self._retry_jitter)
        try:
            await self._repository.finish_delivery(
                record,
                owner_id=self._owner_id,
                attempt_number=attempt.attempt_number,
                receipt=receipt,
                retry_delay=retry_delay,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "channel delivery result could not be committed",
                extra={
                    "error_type": type(exc).__name__,
                    "safe_code": "channel_delivery_finish_failed",
                },
            )
            return receipt, retrying, False
        return receipt, retrying, True

    async def _resolve_in_progress(
        self,
        record: OutboxRecord,
        envelope: OutboundEnvelope,
        progress: DeliveryInProgress,
    ) -> tuple[DeliveryReceipt, bool]:
        receipt = DeliveryReceipt(
            outbound_id=envelope.outbound_id,
            status=DeliveryStatus.AMBIGUOUS,
            provider_code="delivery_in_progress",
        )
        if progress.attempt_number is not None and progress.attempt_number > 0:
            await self._repository.finish_delivery(
                record,
                owner_id=self._owner_id,
                attempt_number=progress.attempt_number,
                receipt=receipt,
                retry_delay=timedelta(0),
            )
            return receipt, True
        # Older attempt ledgers may not expose the attempt number.  Preserve
        # the compatibility manual-review path without starting a duplicate
        # provider request.
        try:
            await self._repository.record_delivery(record.tenant_id, receipt, retrying=False)
            await self._repository.dead_letter_outbox(
                record,
                owner_id=self._owner_id,
                reason=receipt.provider_code or "delivery_in_progress",
            )
        except Exception as exc:
            logger.warning(
                "in-progress delivery could not be marked ambiguous",
                extra={
                    "error_type": type(exc).__name__,
                    "safe_code": "channel_delivery_ambiguous_persist_failed",
                },
            )
            return receipt, False
        return receipt, True

    @staticmethod
    def _normalize_receipt(receipt: DeliveryReceipt, envelope: OutboundEnvelope) -> DeliveryReceipt:
        if receipt.outbound_id == envelope.outbound_id:
            return receipt
        return receipt.model_copy(
            update={
                "outbound_id": envelope.outbound_id,
                "status": DeliveryStatus.AMBIGUOUS,
                "provider_code": "receipt_mismatch",
                "retryable": False,
            }
        )

    @staticmethod
    def _retry_delay(
        receipt: DeliveryReceipt,
        record: OutboxRecord,
        *,
        jitter: Callable[[float], float] | None = None,
    ) -> timedelta:
        retry_after = receipt.retry_after_seconds
        if retry_after is None or not math.isfinite(retry_after):
            base_seconds = min(2**record.attempts, 60)
            retry_after = jitter(base_seconds) if jitter is not None else base_seconds
            if not math.isfinite(retry_after) or retry_after < 0:
                raise ValueError("retry jitter must return a finite non-negative delay")
            retry_after = min(retry_after, 60)
        return timedelta(seconds=retry_after)

    async def _retry_or_dead_letter(
        self,
        record: OutboxRecord,
        *,
        reason: str,
        delay: timedelta,
    ) -> None:
        if record.attempts >= self._max_attempts:
            await self._repository.dead_letter_outbox(
                record,
                owner_id=self._owner_id,
                reason=reason,
            )
            return
        await self._repository.release_outbox(
            record.tenant_id,
            record.outbox_id,
            owner_id=self._owner_id,
            delay=delay,
            error_type=reason,
        )

    async def _dead_letter_record(self, record: OutboxRecord, *, reason: str) -> bool:
        """Quarantine one record without logging its untrusted payload."""

        try:
            await self._repository.dead_letter_outbox(
                record,
                owner_id=self._owner_id,
                reason=reason,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "channel delivery item could not be quarantined",
                extra={
                    "error_type": type(exc).__name__,
                    "safe_code": "channel_delivery_quarantine_failed",
                },
            )
            return False
        return True

    async def run(
        self,
        *,
        poll_seconds: float = 0.5,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        if poll_seconds < 0 or not math.isfinite(poll_seconds):
            raise ValueError("poll_seconds must be non-negative and finite")
        recovery_seconds = min(max(poll_seconds, 0.1), 30.0)
        retry_seconds = recovery_seconds
        while stop_event is None or not stop_event.is_set():
            try:
                completed = await self.dispatch_once(stop_event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "channel dispatcher cycle degraded",
                    extra={
                        "error_type": type(exc).__name__,
                        "safe_code": "channel_dispatcher_cycle_failed",
                    },
                )
                await self._wait_or_stop(stop_event, retry_seconds)
                retry_seconds = min(max(retry_seconds * 2, 0.1), 30.0)
                continue
            retry_seconds = recovery_seconds
            if completed == 0:
                await self._wait_or_stop(stop_event, poll_seconds)

    @staticmethod
    async def _wait_or_stop(stop_event: asyncio.Event | None, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        if stop_event is None:
            await asyncio.sleep(seconds)
            return
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=seconds)
        except TimeoutError:
            pass


__all__ = ["ChannelDispatcher"]
