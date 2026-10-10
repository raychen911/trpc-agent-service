"""Redis consumer that delegates correctness to the PostgreSQL repository."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any

from trpc_service.agent.worker import AgentWorker, ProcessStatus
from trpc_service.faults import FaultStage, FaultStageController, FaultStageEvent
from trpc_service.metrics.privacy import extract_trace_context
from trpc_service.metrics.prometheus import QUEUE_DEPTH
from trpc_service.metrics.telemetry import get_tracer, mark_span_error
from trpc_service.queue.redis_streams import QueueMessage, RedisStreamQueue
from trpc_service.storage.protocols import RuntimeRepository

_HEARTBEAT_SHUTDOWN_TIMEOUT_SECONDS = 0.25
_OPERATION_CANCEL_TIMEOUT_SECONDS = 0.25
_BUSY_RETRY_DELAYS_SECONDS = (0.1, 0.2, 0.25)


class _OwnershipLost(Exception):
    """The delivery is no longer owned by this worker."""


class _HeartbeatFailed(Exception):
    """Wrap a heartbeat failure so it cannot be mistaken for a body failure."""

    def __init__(self, cause: BaseException) -> None:
        super().__init__(str(cause))
        self.cause = cause


class WorkerConsumer:
    def __init__(
        self,
        repository: RuntimeRepository,
        queue: RedisStreamQueue,
        worker: AgentWorker,
        *,
        consumer_id: str,
        concurrency: int = 1,
        shutdown_grace_seconds: float = 30.0,
        fault_stages: FaultStageController | None = None,
    ) -> None:
        if not 1 <= concurrency <= 256:
            raise ValueError("concurrency must be between 1 and 256")
        if shutdown_grace_seconds <= 0:
            raise ValueError("shutdown grace must be positive")
        self._repository = repository
        self._queue = queue
        self._worker = worker
        self._consumer_id = consumer_id
        self._concurrency = concurrency
        self._shutdown_grace_seconds = shutdown_grace_seconds
        self._active_stream_ids: set[str] = set()
        # The controller is intentionally optional: the normal worker path must
        # not perform any fault-injection I/O or allocate a marker client.
        self._fault_stages = fault_stages

    async def process_message(self, message: QueueMessage) -> None:
        self._active_stream_ids.add(message.stream_id)
        try:
            await self._process_message(message)
        finally:
            self._active_stream_ids.discard(message.stream_id)

    async def _process_message(self, message: QueueMessage) -> None:
        parent_context = extract_trace_context(message.trace_headers)
        outcome = "error"
        tracer = get_tracer()
        with tracer.start_as_current_span(
            "queue.consume",
            context=parent_context,
            attributes={"queue": "agent"},
        ) as span:
            try:
                if self._fault_stages is not None:
                    event = FaultStageEvent(
                        stage=FaultStage.ENQUEUE,
                        tenant_id=message.tenant_id,
                        inbound_id=message.aggregate_id,
                        stream_id=message.stream_id,
                        worker_id=self._consumer_id,
                    )
                    await self._fault_stages.checkpoint(event)
                acceptance = await self._repository.get_acceptance(
                    message.tenant_id, message.aggregate_id
                )
                if acceptance is None:
                    await self._queue.ack(message)
                    outcome = "missing"
                    return

                heartbeat_stop = asyncio.Event()
                heartbeat_task = asyncio.create_task(
                    self._queue.heartbeat(
                        message,
                        consumer_id=self._consumer_id,
                        stop_event=heartbeat_stop,
                    )
                )
                body_exception: BaseException | None = None
                heartbeat_exception: BaseException | None = None
                heartbeat_ok = True
                ownership_lost = False
                defer_lost = False
                try:
                    busy_attempt = 0
                    result = await self._run_owned_operation(
                        self._worker.process(acceptance), heartbeat_task
                    )
                    while result.status == ProcessStatus.BUSY:
                        if heartbeat_task.done():
                            self._raise_if_heartbeat_unhealthy(heartbeat_task)
                        retained = await self._run_owned_operation(
                            self._queue.defer(
                                message,
                                consumer_id=self._consumer_id,
                                retry_delay_seconds=_BUSY_RETRY_DELAYS_SECONDS[
                                    min(busy_attempt, len(_BUSY_RETRY_DELAYS_SECONDS) - 1)
                                ],
                            ),
                            heartbeat_task,
                        )
                        if not retained:
                            defer_lost = True
                            break
                        busy_attempt += 1
                        result = await self._run_owned_operation(
                            self._worker.process(acceptance), heartbeat_task
                        )
                except _OwnershipLost:
                    ownership_lost = True
                except _HeartbeatFailed as exc:
                    heartbeat_exception = exc.cause
                except BaseException as exc:
                    body_exception = exc
                finally:
                    heartbeat_ok, cleanup_error = await self._stop_heartbeat(
                        heartbeat_stop, heartbeat_task
                    )
                    if heartbeat_exception is None and cleanup_error is not None:
                        heartbeat_exception = cleanup_error

                if body_exception is not None:
                    raise body_exception
                if heartbeat_exception is not None:
                    raise heartbeat_exception
                if not heartbeat_ok or ownership_lost:
                    outcome = "ownership_lost"
                    return
                if defer_lost:
                    outcome = "ownership_lost"
                    return
                await self._queue.ack(message)
                outcome = "acked"
            except Exception as exc:
                mark_span_error(span, type(exc).__name__)
                raise
            finally:
                span.set_attribute("outcome", outcome)

    async def _run_owned_operation(
        self,
        operation: Coroutine[Any, Any, Any],
        heartbeat_task: asyncio.Task[bool],
    ) -> Any:
        if heartbeat_task.done():
            operation.close()
            self._raise_if_heartbeat_unhealthy(heartbeat_task)
            raise _OwnershipLost
        operation_task = asyncio.create_task(operation)
        try:
            done, _ = await asyncio.wait(
                {operation_task, heartbeat_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        except BaseException:
            await self._cancel_task_bounded(operation_task)
            raise

        if operation_task in done:
            result = operation_task.result()
            if heartbeat_task in done:
                self._raise_if_heartbeat_unhealthy(heartbeat_task)
            return result
        if operation_task.done():
            return operation_task.result()

        try:
            self._raise_if_heartbeat_unhealthy(heartbeat_task)
        except (_OwnershipLost, _HeartbeatFailed):
            await self._cancel_task_bounded(operation_task)
            if operation_task.done():
                try:
                    return operation_task.result()
                except asyncio.CancelledError:
                    pass
                except BaseException as exc:
                    raise exc
            raise
        return await operation_task

    @staticmethod
    def _raise_if_heartbeat_unhealthy(heartbeat_task: asyncio.Task[bool]) -> None:
        try:
            if not heartbeat_task.result():
                raise _OwnershipLost
        except (_OwnershipLost, _HeartbeatFailed):
            raise
        except BaseException as exc:
            raise _HeartbeatFailed(exc) from exc

    @staticmethod
    async def _cancel_task_bounded(task: asyncio.Task[Any]) -> None:
        if not task.done():
            task.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=_OPERATION_CANCEL_TIMEOUT_SECONDS)
        except TimeoutError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        except BaseException:
            return

    @classmethod
    async def _stop_heartbeat(
        cls,
        stop_event: asyncio.Event,
        heartbeat_task: asyncio.Task[bool],
    ) -> tuple[bool, BaseException | None]:
        stop_event.set()
        try:
            result = await asyncio.wait_for(
                asyncio.shield(heartbeat_task),
                timeout=_HEARTBEAT_SHUTDOWN_TIMEOUT_SECONDS,
            )
        except TimeoutError as exc:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            return False, exc
        except BaseException as exc:
            return False, exc
        return result, None

    @staticmethod
    async def _cancel_in_flight(tasks: set[asyncio.Task[None]]) -> None:
        """Cancel and await every task after an abrupt consumer exit."""

        pending = tuple(task for task in tasks if not task.done())
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _drain_in_flight(self, tasks: set[asyncio.Task[None]]) -> None:
        """Give in-flight work the configured grace period on normal stop."""

        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=self._shutdown_grace_seconds)
        if pending:
            await self._cancel_in_flight(pending)

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        await self._queue.ensure_group()
        event = stop_event or asyncio.Event()
        if self._concurrency == 1:
            await self._run_serial(event)
            return
        await self._run_concurrent(event)

    async def _run_serial(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            messages = await self._queue.consume(
                consumer=self._consumer_id,
                count=1,
                active_stream_ids=self._active_stream_ids,
            )
            QUEUE_DEPTH.labels(queue="agent").set(len(messages))
            try:
                for message in messages:
                    await self.process_message(message)
            finally:
                QUEUE_DEPTH.labels(queue="agent").set(0)

    async def _run_concurrent(self, stop_event: asyncio.Event) -> None:
        in_flight: set[asyncio.Task[None]] = set()
        consume_task: asyncio.Task[tuple[QueueMessage, ...]] | None = None
        stop_task = asyncio.create_task(stop_event.wait(), name="worker-consumer-drain")
        abrupt_exit = False
        try:
            while not stop_event.is_set():
                available = self._concurrency - len(in_flight)
                if available and consume_task is None:
                    consume_task = asyncio.create_task(
                        self._queue.consume(
                            consumer=self._consumer_id,
                            count=available,
                            active_stream_ids=self._active_stream_ids,
                        )
                    )

                waiters: set[asyncio.Task[Any]] = set()
                waiters.update(in_flight)
                if consume_task is not None:
                    waiters.add(consume_task)
                waiters.add(stop_task)
                done, _ = await asyncio.wait(
                    waiters,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done:
                    break

                completed = in_flight.intersection(done)
                for task in completed:
                    task.result()
                in_flight.difference_update(completed)

                if consume_task in done:
                    messages = consume_task.result()
                    consume_task = None
                    for message in messages:
                        self._active_stream_ids.add(message.stream_id)
                        in_flight.add(asyncio.create_task(self.process_message(message)))
                QUEUE_DEPTH.labels(queue="agent").set(len(in_flight))
        except asyncio.CancelledError:
            abrupt_exit = True
            raise
        except BaseException:
            abrupt_exit = True
            raise
        finally:
            stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)
            if consume_task is not None:
                consume_task.cancel()
                await asyncio.gather(consume_task, return_exceptions=True)
            if abrupt_exit:
                await self._cancel_in_flight(in_flight)
            else:
                await self._drain_in_flight(in_flight)
            QUEUE_DEPTH.labels(queue="agent").set(0)


__all__ = ["WorkerConsumer"]
