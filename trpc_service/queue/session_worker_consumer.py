"""Bounded SessionReady v2 consumer orchestration.

The Redis notice is only a wake-up.  A consumer claims the authoritative
session state first, acknowledges the short Redis delivery window, and only
then executes the turn.  This module intentionally has no model lease,
BUSY-retry, or Redis PEL heartbeat behavior.
"""

from __future__ import annotations

import asyncio
import logging
import math
from typing import Protocol

from trpc_service.faults import FaultStage, FaultStageController, FaultStageEvent
from trpc_service.queue.session_ready import SessionReady, SessionReadyDelivery
from trpc_service.storage.models import MailboxClaimStatus, SessionClaim

logger = logging.getLogger(__name__)

_DEFAULT_RECEIVE_BLOCK_MS = 5_000
_DEFAULT_RECLAIMER_POLL_SECONDS = 5.0
_DEFAULT_ERROR_RETRY_SECONDS = 0.1
_DEFAULT_ACK_TIMEOUT_SECONDS = 3.0


class SessionReadyReceiver(Protocol):
    async def receive_new(
        self,
        *,
        consumer: str,
        count: int,
        block_ms: int,
    ) -> tuple[SessionReadyDelivery, ...]: ...

    async def ack(self, delivery: SessionReadyDelivery) -> bool: ...


class SessionReadyReclaimSource(Protocol):
    async def reclaim(self) -> tuple[SessionReadyDelivery, ...]: ...


class SessionReadyClaimer(Protocol):
    async def claim(self, message: SessionReady) -> SessionClaim: ...


class SessionReadyExecutor(Protocol):
    async def execute(self, claim: SessionClaim) -> None: ...


class _PermitLease:
    """An idempotent semaphore permit transferred to one delivery task."""

    def __init__(self, semaphore: asyncio.Semaphore) -> None:
        self._semaphore = semaphore
        self._released = False

    def release(self) -> None:
        if not self._released:
            self._released = True
            self._semaphore.release()


class SessionWorkerConsumer:
    """Run new-message and stale-PEL loops under one bounded semaphore."""

    def __init__(
        self,
        queue: SessionReadyReceiver,
        reclaimer: SessionReadyReclaimSource,
        claimer: SessionReadyClaimer,
        executor: SessionReadyExecutor,
        *,
        consumer_id: str,
        concurrency: int = 10,
        receive_block_ms: int = _DEFAULT_RECEIVE_BLOCK_MS,
        reclaimer_poll_seconds: float = _DEFAULT_RECLAIMER_POLL_SECONDS,
        ack_timeout_seconds: float = _DEFAULT_ACK_TIMEOUT_SECONDS,
        shutdown_grace_seconds: float = 30.0,
        fault_stages: FaultStageController | None = None,
        fault_injection_enabled: bool = False,
        test_environment: bool = False,
    ) -> None:
        if not consumer_id:
            raise ValueError("consumer_id must not be empty")
        if concurrency < 1:
            raise ValueError("concurrency must be positive")
        if receive_block_ms < 1:
            raise ValueError("receive_block_ms must be positive and finite")
        if reclaimer_poll_seconds < 0:
            raise ValueError("reclaimer_poll_seconds must be non-negative")
        if ack_timeout_seconds <= 0 or not math.isfinite(ack_timeout_seconds):
            raise ValueError("ack_timeout_seconds must be positive and finite")
        if shutdown_grace_seconds <= 0 or not math.isfinite(shutdown_grace_seconds):
            raise ValueError("shutdown_grace_seconds must be positive and finite")
        if fault_stages is not None and not (fault_injection_enabled and test_environment):
            raise ValueError("fault stages require fault_injection_enabled in the test environment")
        self._queue = queue
        self._reclaimer = reclaimer
        self._claimer = claimer
        self._executor = executor
        self._consumer_id = consumer_id
        self._receive_block_ms = receive_block_ms
        self._reclaimer_poll_seconds = reclaimer_poll_seconds
        self._ack_timeout_seconds = ack_timeout_seconds
        self._shutdown_grace_seconds = shutdown_grace_seconds
        # The normal path keeps this as None: no controller call or event
        # allocation occurs unless an explicitly enabled test worker supplies
        # one.  Production settings reject fault injection before construction.
        self._fault_stages = fault_stages
        self._permits = asyncio.Semaphore(concurrency)
        self._in_flight: dict[asyncio.Task[None], _PermitLease] = {}

    @property
    def concurrency_available(self) -> int:
        """Expose a diagnostic snapshot without exposing the semaphore itself."""

        return self._permits._value

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Run both sources until stopped or cancelled, fully draining tasks."""

        event = stop_event or asyncio.Event()
        source_tasks = (
            asyncio.create_task(self._receive_loop(event), name="session-ready-receive"),
            asyncio.create_task(self._reclaim_loop(event), name="session-ready-reclaim"),
        )
        abrupt_exit = False
        try:
            await asyncio.gather(*source_tasks)
        except asyncio.CancelledError:
            abrupt_exit = True
            event.set()
            raise
        except BaseException:
            abrupt_exit = True
            event.set()
            raise
        finally:
            event.set()
            for task in source_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*source_tasks, return_exceptions=True)
            if abrupt_exit:
                await self._cancel_in_flight()
            else:
                await self._drain_in_flight()

    async def _receive_loop(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            permit = await self._take_permit(stop_event)
            if permit is None:
                return
            try:
                deliveries = await self._queue.receive_new(
                    consumer=self._consumer_id,
                    count=1,
                    block_ms=self._receive_block_ms,
                )
            except asyncio.CancelledError:
                permit.release()
                raise
            except Exception as exc:
                permit.release()
                logger.warning(
                    "session-ready receive failed",
                    extra={"error_type": type(exc).__name__},
                    exc_info=True,
                )
                await _wait_or_yield(stop_event, _DEFAULT_ERROR_RETRY_SECONDS)
                continue
            if not deliveries:
                permit.release()
                # The finite Redis block is deliberate: it gives the
                # reclaimer a scheduling opportunity even at concurrency=1.
                await asyncio.sleep(0)
                continue
            self._start_delivery(deliveries[0], permit)

    async def _reclaim_loop(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            # A Redis PEL ownership transfer is itself work.  Reserve capacity
            # before XAUTOCLAIM so a saturated worker cannot steal a delivery
            # and then leave it idle while another worker had capacity.
            permit = await self._take_permit(stop_event)
            if permit is None:
                return
            try:
                deliveries = await self._reclaimer.reclaim()
            except asyncio.CancelledError:
                permit.release()
                raise
            except Exception as exc:
                permit.release()
                logger.warning(
                    "session-ready reclaim failed",
                    extra={"error_type": type(exc).__name__},
                    exc_info=True,
                )
                await _wait_or_yield(stop_event, _DEFAULT_ERROR_RETRY_SECONDS)
                continue
            if not deliveries:
                permit.release()
                await _wait_or_yield(stop_event, self._reclaimer_poll_seconds)
                continue
            self._start_delivery(deliveries[0], permit)

    async def _take_permit(self, stop_event: asyncio.Event) -> _PermitLease | None:
        if stop_event.is_set():
            return None
        acquire_task = asyncio.create_task(self._permits.acquire())
        stop_task = asyncio.create_task(stop_event.wait())
        try:
            done, _ = await asyncio.wait(
                {acquire_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )
        except BaseException:
            acquire_task.cancel()
            stop_task.cancel()
            await asyncio.gather(acquire_task, stop_task, return_exceptions=True)
            raise
        if stop_task in done:
            if acquire_task in done and not acquire_task.cancelled():
                self._permits.release()
            else:
                acquire_task.cancel()
            await asyncio.gather(acquire_task, return_exceptions=True)
            return None
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        return _PermitLease(self._permits)

    def _start_delivery(self, delivery: SessionReadyDelivery, permit: _PermitLease) -> None:
        coroutine = self._process_delivery(delivery, permit)
        try:
            task = asyncio.create_task(coroutine, name=f"session-ready:{delivery.stream_id}")
        except BaseException:
            # If task creation itself fails, the coroutine has never had a
            # chance to run its ``finally`` block.  Close it explicitly to
            # avoid an unawaited-coroutine warning and release the permit.
            coroutine.close()
            permit.release()
            raise
        self._in_flight[task] = permit
        task.add_done_callback(self._finish_delivery_task)

    def _finish_delivery_task(self, task: asyncio.Task[None]) -> None:
        permit = self._in_flight.pop(task, None)
        if permit is not None:
            # A task cancelled before its coroutine gets a scheduling turn
            # never reaches ``_process_delivery``'s finally block.  Releasing
            # here as well is safe because the permit lease is idempotent.
            permit.release()
        if task.cancelled():
            return
        exception = task.exception()
        if exception is not None:
            logger.error(
                "session-ready delivery task failed",
                extra={"error_type": type(exception).__name__},
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    async def _process_delivery(
        self,
        delivery: SessionReadyDelivery,
        permit: _PermitLease,
    ) -> None:
        try:
            await self._claim_ack_execute(delivery)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The task boundary is intentionally isolated: one bad session
            # must not stop either source loop.
            logger.error(
                "session-ready delivery failed",
                extra={
                    "event_id": delivery.message.event_id,
                    "tenant_id": delivery.message.tenant_id,
                    "session_id": delivery.message.session_id,
                    "error_type": type(exc).__name__,
                },
                exc_info=True,
            )
        finally:
            permit.release()

    async def _claim_ack_execute(self, delivery: SessionReadyDelivery) -> None:
        # This is the v2 claim-before fault checkpoint.  It is deliberately
        # after permit acquisition/Redis receive and immediately before the
        # authoritative PostgreSQL claim.  If the test controller blocks,
        # raises, or the process is terminated here, the Redis delivery stays
        # in the PEL and no PG claim/ACK/execution has occurred.
        if self._fault_stages is not None:
            await self._fault_stages.checkpoint(
                FaultStageEvent(
                    stage=FaultStage.ENQUEUE,
                    tenant_id=delivery.message.tenant_id,
                    worker_id=self._consumer_id,
                    # v2 SessionReady.event_id is the outbox event id used by
                    # the PG claim; the actual inbound id is not on the wire.
                    inbound_id=delivery.message.event_id,
                    stream_id=delivery.stream_id,
                )
            )
        try:
            claim = await self._claimer.claim(delivery.message)
        except asyncio.CancelledError:
            raise
        except Exception:
            # A failed PG claim leaves Redis unacknowledged for the independent
            # reclaimer; do not invoke ACK or execute.
            logger.exception(
                "session-ready claim failed",
                extra={"event_id": delivery.message.event_id},
            )
            return

        if claim.status != MailboxClaimStatus.CLAIMED:
            await self._ack_safely(delivery)
            return

        # ACK is intentionally before execution.  A successful PG claim is
        # authoritative and cannot be undone by an ACK failure.
        await self._ack_safely(delivery)
        try:
            await self._executor.execute(claim)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "session-ready execution failed",
                extra={"event_id": delivery.message.event_id},
            )

    async def _ack_safely(self, delivery: SessionReadyDelivery) -> None:
        try:
            # ACK is after the authoritative PG claim and therefore is best
            # effort.  Bound the Redis call so a half-open connection cannot
            # hold an execution permit forever; never retry or undo the claim.
            acknowledged = await asyncio.wait_for(
                self._queue.ack(delivery), timeout=self._ack_timeout_seconds
            )
            if not acknowledged:
                logger.error(
                    "session-ready ACK returned zero",
                    extra={"event_id": delivery.message.event_id},
                )
        except TimeoutError:
            logger.warning(
                "session-ready ACK deadline exceeded",
                extra={
                    "event_id": delivery.message.event_id,
                    "timeout_seconds": self._ack_timeout_seconds,
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "session-ready ACK failed",
                extra={"event_id": delivery.message.event_id},
            )

    async def _cancel_in_flight(self) -> None:
        """Cancel and await every delivery after an abrupt consumer exit."""

        tasks = tuple(self._in_flight)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        # A task canceled before its first scheduling turn cannot execute its
        # coroutine finally block.  The lease is idempotent, so release here
        # as the final cancellation safeguard.
        for permit in tuple(self._in_flight.values()):
            permit.release()
        self._in_flight.clear()

    async def _drain_in_flight(self) -> None:
        """Give in-flight work the configured grace period on normal stop."""

        tasks = tuple(self._in_flight)
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=self._shutdown_grace_seconds)
            if pending:
                await self._cancel_in_flight()
        # A task canceled before its first scheduling turn cannot execute its
        # coroutine finally block.  The lease is idempotent, so release here
        # as the final cancellation safeguard.
        for permit in tuple(self._in_flight.values()):
            permit.release()
        self._in_flight.clear()


async def _wait_or_yield(stop_event: asyncio.Event, seconds: float) -> None:
    if seconds <= 0:
        await asyncio.sleep(0)
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        pass


__all__ = [
    "SessionReadyClaimer",
    "SessionReadyExecutor",
    "SessionReadyReceiver",
    "SessionReadyReclaimSource",
    "SessionWorkerConsumer",
]
