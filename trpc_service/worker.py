"""Bounded consumers with lease renewal, deadlines and signal-driven drain."""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass

from trpc_service.reliability.execution import SessionBusyError
from trpc_service.telemetry.runtime import count


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkerSettings:
    concurrency: int = 4
    poll_seconds: float = 0.25
    lease_seconds: float = 30
    run_timeout_seconds: float = 120
    drain_seconds: float = 20
    max_attempts: int = 5

    def __post_init__(self):
        if min(self.concurrency, self.poll_seconds, self.lease_seconds, self.run_timeout_seconds,
               self.drain_seconds, self.max_attempts) <= 0:
            raise ValueError("worker settings must be positive")


async def wait_or_stop(stop, seconds):
    try:
        await asyncio.wait_for(stop.wait(), seconds)
    except asyncio.TimeoutError:
        pass


async def consume(run_once, stop, *, poll_seconds=0.25):
    """Database outages back off without terminating the consumer."""
    failures = 0
    while not stop.is_set():
        try:
            busy = await run_once()
            failures = 0
        except Exception as error:
            failures += 1
            count("consumer.unavailable")
            logger.warning("consumer unavailable: %s", type(error).__name__)
            await wait_or_stop(stop, min(2 ** min(failures, 5), 30) * random.uniform(0.8, 1.2))
        else:
            if not busy:
                await wait_or_stop(stop, poll_seconds)


class AgentWorker:
    def __init__(self, inbox, pipeline, registry, *, worker_id=None, settings=None, include_im=False):
        self.inbox, self.pipeline, self.registry = inbox, pipeline, registry
        self.worker_id = worker_id or uuid.uuid4().hex
        self.settings = settings or WorkerSettings()
        self.include_im = include_im

    async def run_once(self):
        # Every claim has a unique owner, including concurrent runs in one process.
        owner = f"{self.worker_id}:{uuid.uuid4().hex}"
        options = {"include_im": True} if self.include_im else {}
        work = self.inbox.claim(worker_id=owner, lease_seconds=self.settings.lease_seconds, **options)
        if work is None:
            return False
        execution = asyncio.create_task(self.pipeline.execute(
            work, self.registry, worker_id=owner, lease_seconds=self.settings.lease_seconds))

        async def renew():
            while True:
                await asyncio.sleep(self.settings.lease_seconds / 3)
                if not self.inbox.renew(work, owner, self.settings.lease_seconds):
                    # Completion can commit while the pipeline is still releasing
                    # its session lease. That terminal write is not a lease loss.
                    await execution
                    return

        heartbeat = asyncio.create_task(renew())
        try:
            done, _ = await asyncio.wait({execution, heartbeat}, timeout=self.settings.run_timeout_seconds,
                                         return_when=asyncio.FIRST_COMPLETED)
            if execution.cancelled() or heartbeat.cancelled():
                raise RuntimeError("execution lost its lease")
            if execution.done():
                await execution
            elif heartbeat in done:
                await heartbeat
            if not execution.done():
                raise asyncio.TimeoutError()
            await execution
        except SessionBusyError:
            count("worker.session_busy")
            # Contention does not consume the model failure budget.
            self.inbox.retry(work.inbound_message_id, owner, "session_busy", random.uniform(0.2, 0.8),
                             count_attempt=False)
        except (ValueError, LookupError) as error:
            self.inbox.mark_terminal(work.inbound_message_id, owner, "failed_final", type(error).__name__)
        except Exception as error:
            if self.inbox.is_recalled(work.inbound_message_id):
                count("worker.cancelled", reason="message_recalled")
                return True
            count("worker.failure", reason=type(error).__name__)
            execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
            # An exception can occur after a native write or tool side effect. Never
            # automatically replay an unproven execution. Durable finals are recovered
            # by the next claim before this limit applies.
            if work.attempt < self.settings.max_attempts:
                self.inbox.retry(work.inbound_message_id, owner, type(error).__name__,
                                 min(2 ** work.attempt, 30))
            else:
                self.inbox.mark_terminal(work.inbound_message_id, owner, "unknown_outcome", type(error).__name__)
            logger.warning("execution interrupted: %s", type(error).__name__)
        finally:
            execution.cancel()
            heartbeat.cancel()
            await asyncio.gather(execution, heartbeat, return_exceptions=True)
        return True

    async def run(self, stop):
        tasks = [asyncio.create_task(consume(self.run_once, stop, poll_seconds=self.settings.poll_seconds))
                 for _ in range(self.settings.concurrency)]
        try:
            await stop.wait()
            _, pending = await asyncio.wait(tasks, timeout=self.settings.drain_seconds)
            for task in pending:
                task.cancel()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
