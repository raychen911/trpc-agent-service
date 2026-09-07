# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Redis Streams queue for decoupling the gateway from the worker.

The gateway enqueues a serialized :class:`TaskMessage`; worker processes consume
it via a consumer group (at-least-once delivery). Combined with the gateway's
existing idempotency key, redelivery never causes duplicate processing.

This module lives at the package top level (not under ``gateway`` or ``worker``)
so both sides can import it without a circular dependency.
"""

from __future__ import annotations

import time
from typing import Any
from typing import Optional

import redis.asyncio as aioredis
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from ..channels import InboundMessage
from ..metrics import EnterpriseMetrics
from ..metrics import get_enterprise_metrics
from ..metrics import operation_span


class TaskMessage(BaseModel):
    """A unit of work routed from gateway to worker."""

    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    channel: str
    inbound: dict[str, Any]
    """Serialized :class:`InboundMessage`."""
    trace_headers: dict[str, str] = Field(default_factory=dict)
    """Optional OpenTelemetry propagation headers (reserved for trace continuity)."""

    @classmethod
    def from_inbound(
        cls,
        tenant_id: str,
        channel: str,
        inbound: InboundMessage,
        trace_headers: Optional[dict[str, str]] = None,
    ) -> "TaskMessage":
        return cls(
            tenant_id=tenant_id,
            channel=channel,
            inbound=inbound.model_dump(mode="json"),
            trace_headers=trace_headers or {},
        )

    def to_inbound(self) -> InboundMessage:
        return InboundMessage.model_validate(self.inbound)

    @property
    def idempotency_key(self) -> str:
        message_id = self.inbound.get("message_id", "")
        return f"{self.tenant_id}:{self.channel}:{message_id}"


class StreamQueue:
    """A thin Redis Streams wrapper for the gateway→worker task queue."""

    def __init__(
        self,
        *,
        redis_url: Optional[str] = None,
        client: Any = None,
        stream: str = "agent:tasks",
        group: str = "agent-workers",
        consumer: str = "worker",
        maxlen: int = 10000,
        metrics: Optional[EnterpriseMetrics] = None,
    ) -> None:
        if client is not None:
            self._client = client
        elif redis_url:
            self._client = aioredis.from_url(redis_url, decode_responses=True)
        else:
            raise ValueError("StreamQueue requires redis_url or client")
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._maxlen = maxlen
        self._metrics = metrics or get_enterprise_metrics()

    async def _execute(self, operation: str, awaitable: Any) -> Any:
        started = time.perf_counter()
        outcome = "error"
        error_type = None
        try:
            with operation_span(f"queue.{operation}", **{"messaging.system": "redis"}):
                result = await awaitable
            outcome = "success"
            return result
        except Exception as exc:
            error_type = type(exc).__name__
            raise
        finally:
            attributes = {"operation": operation, "outcome": outcome, "error_type": error_type}
            self._metrics.increment("agent_queue_operation_total", **attributes)
            self._metrics.observe(
                "agent_queue_operation_duration_ms",
                (time.perf_counter() - started) * 1000,
                **attributes,
            )

    async def enqueue(self, task: TaskMessage) -> str:
        """Append a task and return its stream message id."""
        return await self._execute(
            "enqueue",
            self._client.xadd(
                self._stream,
                {"payload": task.model_dump_json()},
                maxlen=self._maxlen,
                approximate=True,
            ),
        )

    async def ensure_group(self) -> None:
        """Create the consumer group if it does not already exist."""
        started = time.perf_counter()
        outcome = "success"
        error_type = None
        try:
            with operation_span("queue.ensure_group", **{"messaging.system": "redis"}):
                await self._client.xgroup_create(self._stream, self._group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                outcome = "error"
                error_type = type(exc).__name__
                raise
        finally:
            attributes = {"operation": "ensure_group", "outcome": outcome, "error_type": error_type}
            self._metrics.increment("agent_queue_operation_total", **attributes)
            self._metrics.observe(
                "agent_queue_operation_duration_ms",
                (time.perf_counter() - started) * 1000,
                **attributes,
            )

    async def read(self, count: int = 1, block: int = 0) -> list:
        """Read new (``>``) messages for the consumer group."""
        try:
            return await self._execute(
                "read",
                self._client.xreadgroup(
                    self._group,
                    self._consumer,
                    {self._stream: ">"},
                    count=count,
                    block=block,
                ),
            )
        except RedisTimeoutError:
            # Some redis-py configurations use the socket timeout for a
            # blocking XREADGROUP call.  An empty poll is normal in that
            # case; let the worker continue polling instead of exiting.
            if block > 0:
                return []
            raise

    async def claim_stale(self, min_idle_ms: int = 60000, count: int = 10) -> list:
        """Claim abandoned pending messages for this consumer.

        Redis Streams does not redeliver pending entries through ``>`` reads.
        ``XAUTOCLAIM`` is therefore required after a worker disappears.
        """
        result = await self._execute(
            "claim_stale",
            self._client.xautoclaim(
                self._stream,
                self._group,
                self._consumer,
                min_idle_time=min_idle_ms,
                start_id="0-0",
                count=count,
            ),
        )
        entries = result[1] if result and len(result) > 1 else []
        return [(self._stream, entries)] if entries else []

    async def delivery_count(self, message_id: str) -> int:
        """Return the consumer-group delivery count for one pending entry."""
        entries = await self._execute(
            "delivery_count",
            self._client.xpending_range(
                self._stream,
                self._group,
                min=message_id,
                max=message_id,
                count=1,
            ),
        )
        if not entries:
            return 0
        entry = entries[0]
        return int(entry.get("times_delivered", entry.get(b"times_delivered", 1)))

    async def dead_letter(self, message_id: str, payload: str, error: str) -> str:
        """Move a poison task to the dead-letter stream and acknowledge it."""
        dead_id = await self._execute(
            "dead_letter",
            self._client.xadd(
                f"{self._stream}:dead",
                {
                    "source_id": message_id,
                    "payload": payload,
                    "error": error[:1000]
                },
            ),
        )
        await self.ack(message_id)
        return dead_id

    async def ack(self, message_id: str) -> int:
        return await self._execute("ack", self._client.xack(self._stream, self._group, message_id))

    async def close(self) -> None:
        await self._client.aclose()
