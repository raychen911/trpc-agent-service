"""Inline and Redis Streams message brokers."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Protocol, cast

from redis.asyncio import Redis, RedisCluster
from redis.exceptions import ResponseError

from tenant_agent.models import RoutedEnvelope
from tenant_agent.observability import ERRORS, QUEUE_DEPTH

logger = logging.getLogger(__name__)

PUBLISH_SCRIPT = """
local global_count = redis.call('XLEN', KEYS[1]) + redis.call('ZCARD', KEYS[3])
if global_count >= tonumber(ARGV[1]) then
  return {'global', ''}
end
local tenant_count = tonumber(redis.call('HGET', KEYS[2], ARGV[3]) or '0')
if tenant_count >= tonumber(ARGV[2]) then
  return {'tenant', ''}
end
local message_id = redis.call(
  'XADD', KEYS[1], '*', 'envelope', ARGV[4], 'attempts', '0'
)
redis.call('HINCRBY', KEYS[2], ARGV[3], 1)
return {'ok', message_id}
"""

ACK_SCRIPT = """
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if acknowledged > 0 then
  redis.call('XDEL', KEYS[1], ARGV[2])
  local remaining = redis.call('HINCRBY', KEYS[2], ARGV[3], -1)
  if remaining <= 0 then
    redis.call('HDEL', KEYS[2], ARGV[3])
  end
end
return acknowledged
"""

RETRY_SCRIPT = """
local message_id = redis.call(
  'XADD', KEYS[1], '*', 'envelope', ARGV[3], 'attempts', ARGV[4],
  'source_id', ARGV[2]
)
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if acknowledged > 0 then
  redis.call('XDEL', KEYS[1], ARGV[2])
end
return message_id
"""

DEFER_SCRIPT = """
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
if acknowledged > 0 then
  redis.call('XDEL', KEYS[1], ARGV[2])
  redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
end
return acknowledged
"""

PROMOTE_DUE_SCRIPT = """
local entries = redis.call(
  'ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2]
)
for _, member in ipairs(entries) do
  local item = cjson.decode(member)
  redis.call(
    'XADD', KEYS[1], '*', 'envelope', item.envelope,
    'attempts', tostring(item.attempts), 'source_id', item.source_id
  )
  redis.call('ZREM', KEYS[2], member)
end
return #entries
"""


class BrokerCapacityError(RuntimeError):
    """The durable broker rejected admission before accepting the callback."""


@dataclass(frozen=True, slots=True)
class BrokerMessage:
    broker_id: str
    routed: RoutedEnvelope
    attempts: int = 0


class JobBroker(Protocol):
    async def initialize(self) -> None: ...

    async def close(self) -> None: ...

    async def healthcheck(self) -> bool: ...

    async def publish(self, routed: RoutedEnvelope) -> str: ...

    async def receive(self, *, timeout_ms: int) -> BrokerMessage | None: ...

    async def ack(self, message: BrokerMessage) -> None: ...

    async def defer(
        self,
        message: BrokerMessage,
        *,
        delay_seconds: float,
        count_attempt: bool = True,
    ) -> None: ...

    async def fail(self, message: BrokerMessage, *, terminal: bool = False) -> None: ...

    async def list_dead(self, tenant_id: str, *, limit: int) -> tuple[BrokerMessage, ...]: ...

    async def requeue_dead(self, tenant_id: str, broker_id: str) -> str: ...


class InlineBroker:
    def __init__(self, *, max_queue_size: int = 100_000) -> None:
        self.queue: asyncio.Queue[BrokerMessage] = asyncio.Queue(maxsize=max_queue_size)
        self.dead_letters: deque[BrokerMessage] = deque(maxlen=1_000)
        self._sequence = 0

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def healthcheck(self) -> bool:
        return True

    async def publish(self, routed: RoutedEnvelope) -> str:
        self._sequence += 1
        broker_id = f"inline-{self._sequence}"
        try:
            self.queue.put_nowait(BrokerMessage(broker_id, routed))
        except asyncio.QueueFull:
            tenant_id = routed.inbound.tenant_id
            ERRORS.labels(tenant_id, "broker_publish", "CapacityExceeded").inc()
            raise BrokerCapacityError("global broker queue capacity reached") from None
        QUEUE_DEPTH.labels("inline").set(self.queue.qsize())
        return broker_id

    async def receive(self, *, timeout_ms: int) -> BrokerMessage | None:
        try:
            message = await asyncio.wait_for(self.queue.get(), timeout=timeout_ms / 1_000)
            QUEUE_DEPTH.labels("inline").set(self.queue.qsize())
            return message
        except TimeoutError:
            return None

    async def ack(self, message: BrokerMessage) -> None:
        del message
        self.queue.task_done()
        QUEUE_DEPTH.labels("inline").set(self.queue.qsize())

    async def defer(
        self,
        message: BrokerMessage,
        *,
        delay_seconds: float,
        count_attempt: bool = True,
    ) -> None:
        self.queue.task_done()
        if delay_seconds:
            await asyncio.sleep(delay_seconds)
        await self.queue.put(
            BrokerMessage(
                message.broker_id,
                message.routed,
                attempts=message.attempts + (1 if count_attempt else 0),
            )
        )
        QUEUE_DEPTH.labels("inline").set(self.queue.qsize())

    async def fail(self, message: BrokerMessage, *, terminal: bool = False) -> None:
        self.queue.task_done()
        failed = BrokerMessage(
            message.broker_id,
            message.routed,
            attempts=message.attempts + 1,
        )
        if terminal:
            self.dead_letters.append(failed)
            tenant_id = message.routed.inbound.tenant_id
            ERRORS.labels(tenant_id, "inline_broker", "TerminalJob").inc()
            QUEUE_DEPTH.labels("inline:dead").set(len(self.dead_letters))
            logger.error(
                "inline broker dead-lettered terminal job id=%s tenant=%s",
                message.broker_id,
                tenant_id,
            )
        else:
            await self.queue.put(failed)
        QUEUE_DEPTH.labels("inline").set(self.queue.qsize())

    async def list_dead(self, tenant_id: str, *, limit: int) -> tuple[BrokerMessage, ...]:
        rows = [item for item in reversed(self.dead_letters) if item.routed.inbound.tenant_id == tenant_id]
        return tuple(rows[:limit])

    async def requeue_dead(self, tenant_id: str, broker_id: str) -> str:
        item = next(
            (
                row
                for row in self.dead_letters
                if row.broker_id == broker_id and row.routed.inbound.tenant_id == tenant_id
            ),
            None,
        )
        if item is None:
            raise KeyError("unknown broker dead letter")
        new_id = await self.publish(item.routed)
        self.dead_letters.remove(item)
        QUEUE_DEPTH.labels("inline:dead").set(len(self.dead_letters))
        return new_id


class RedisStreamsBroker:
    """At-least-once broker; idempotency receipts provide exactly-once agent effects."""

    def __init__(
        self,
        *,
        url: str,
        stream: str,
        group: str,
        consumer: str,
        claim_idle_ms: int,
        max_attempts: int = 8,
        global_queue_limit: int = 100_000,
        tenant_queue_limit: int = 10_000,
        cluster: bool = False,
    ) -> None:
        if cluster:
            opening = stream.find("{")
            closing = stream.find("}", opening + 1)
            if opening < 0 or closing <= opening + 1:
                raise ValueError("Redis Cluster broker stream must contain a non-empty hash tag")
        client_type = RedisCluster if cluster else Redis
        self.redis = client_type.from_url(
            url,
            decode_responses=True,
            health_check_interval=30,
        )
        self.stream = stream
        self.tenant_counts_key = f"{stream}:tenant-pending"
        self.delayed_key = f"{stream}:delayed"
        self.dead_letter_stream = f"{stream}:dead"
        self.group = group
        self.consumer = consumer
        self.claim_idle_ms = claim_idle_ms
        self.max_attempts = max_attempts
        self.global_queue_limit = global_queue_limit
        self.tenant_queue_limit = tenant_queue_limit
        self._claim_cursor = "0-0"

    async def initialize(self) -> None:
        try:
            await self.redis.xgroup_create(self.stream, self.group, id="0-0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def close(self) -> None:
        await self.redis.aclose()

    async def healthcheck(self) -> bool:
        return bool(await self.redis.ping())

    async def publish(self, routed: RoutedEnvelope) -> str:
        tenant_id = routed.inbound.tenant_id
        result = cast(
            list[str | bytes],
            await cast(Any, self.redis).eval(
                PUBLISH_SCRIPT,
                3,
                self.stream,
                self.tenant_counts_key,
                self.delayed_key,
                str(self.global_queue_limit),
                str(self.tenant_queue_limit),
                tenant_id,
                routed.model_dump_json(),
            ),
        )
        status_value, message_value = result
        status = status_value.decode() if isinstance(status_value, bytes) else status_value
        if status != "ok":
            ERRORS.labels(tenant_id, "broker_publish", "CapacityExceeded").inc()
            logger.warning("broker admission rejected tenant=%s scope=%s", tenant_id, status)
            raise BrokerCapacityError(f"{status} broker queue capacity reached")
        message_id = message_value.decode() if isinstance(message_value, bytes) else message_value
        QUEUE_DEPTH.labels(self.stream).set(await self._queue_depth())
        return message_id

    async def receive(self, *, timeout_ms: int) -> BrokerMessage | None:
        await cast(Any, self.redis).eval(
            PROMOTE_DUE_SCRIPT,
            2,
            self.stream,
            self.delayed_key,
            str(int(time.time() * 1_000)),
            "32",
        )
        claimed = await self.redis.xautoclaim(
            self.stream,
            self.group,
            self.consumer,
            min_idle_time=self.claim_idle_ms,
            start_id=self._claim_cursor,
            count=1,
        )
        if claimed and len(claimed) >= 2:
            self._claim_cursor = claimed[0] or "0-0"
            if claimed[1]:
                broker_id, fields = claimed[1][0]
                return self._decode(broker_id, fields)
        rows = await self.redis.xreadgroup(
            self.group,
            self.consumer,
            {self.stream: ">"},
            count=1,
            block=timeout_ms,
        )
        if not rows:
            return None
        _, messages = rows[0]
        broker_id, fields = messages[0]
        return self._decode(broker_id, fields)

    @staticmethod
    def _decode(broker_id: str, fields: dict[str, str]) -> BrokerMessage:
        return BrokerMessage(
            broker_id=broker_id,
            routed=RoutedEnvelope.model_validate_json(fields["envelope"]),
            attempts=int(fields.get("attempts", "0")),
        )

    def _tenant_dead_letter_stream(self, tenant_id: str) -> str:
        return f"{self.dead_letter_stream}:{tenant_id}"

    async def ack(self, message: BrokerMessage) -> None:
        await cast(Any, self.redis).eval(
            ACK_SCRIPT,
            2,
            self.stream,
            self.tenant_counts_key,
            self.group,
            message.broker_id,
            message.routed.inbound.tenant_id,
        )
        QUEUE_DEPTH.labels(self.stream).set(await self._queue_depth())

    async def defer(
        self,
        message: BrokerMessage,
        *,
        delay_seconds: float,
        count_attempt: bool = True,
    ) -> None:
        attempts = message.attempts + (1 if count_attempt else 0)
        if count_attempt and attempts >= self.max_attempts:
            await self.fail(message, terminal=True)
            return
        delayed = json.dumps(
            {
                "envelope": message.routed.model_dump_json(),
                "attempts": attempts,
                "source_id": message.broker_id,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        acknowledged = await cast(Any, self.redis).eval(
            DEFER_SCRIPT,
            2,
            self.stream,
            self.delayed_key,
            self.group,
            message.broker_id,
            str(int((time.time() + max(0.0, delay_seconds)) * 1_000)),
            delayed,
        )
        if int(acknowledged) != 1:
            raise RuntimeError("cannot defer a broker message that is not pending")
        QUEUE_DEPTH.labels(self.stream).set(await self._queue_depth())

    async def fail(self, message: BrokerMessage, *, terminal: bool = False) -> None:
        attempts = message.attempts + 1
        is_terminal = terminal or attempts >= self.max_attempts
        target = (
            self._tenant_dead_letter_stream(message.routed.inbound.tenant_id) if is_terminal else self.stream
        )
        fields: dict[Any, Any] = {
            "envelope": message.routed.model_dump_json(),
            "attempts": str(attempts),
            "source_id": message.broker_id,
        }
        if is_terminal:
            await self.redis.xadd(
                target,
                fields,
                maxlen=100_000,
                approximate=True,
            )
            await self.ack(message)
        else:
            await cast(Any, self.redis).eval(
                RETRY_SCRIPT,
                1,
                self.stream,
                self.group,
                message.broker_id,
                fields["envelope"],
                fields["attempts"],
            )
            QUEUE_DEPTH.labels(self.stream).set(await self._queue_depth())

    async def _queue_depth(self) -> int:
        live, delayed = await asyncio.gather(
            self.redis.xlen(self.stream),
            self.redis.zcard(self.delayed_key),
        )
        return int(live) + int(delayed)

    async def list_dead(self, tenant_id: str, *, limit: int) -> tuple[BrokerMessage, ...]:
        rows = await self.redis.xrevrange(
            self._tenant_dead_letter_stream(tenant_id),
            count=limit,
        )
        return tuple(self._decode(broker_id, fields) for broker_id, fields in rows)

    async def requeue_dead(self, tenant_id: str, broker_id: str) -> str:
        dead_stream = self._tenant_dead_letter_stream(tenant_id)
        rows = await self.redis.xrange(
            dead_stream,
            min=broker_id,
            max=broker_id,
            count=1,
        )
        if not rows or rows[0][0] != broker_id:
            raise KeyError("unknown broker dead letter")
        message = self._decode(rows[0][0], rows[0][1])
        if message.routed.inbound.tenant_id != tenant_id:
            raise KeyError("unknown broker dead letter")
        new_id = await self.publish(message.routed)
        await self.redis.xdel(dead_stream, broker_id)
        QUEUE_DEPTH.labels(self.dead_letter_stream).set(await self.redis.xlen(dead_stream))
        return new_id
