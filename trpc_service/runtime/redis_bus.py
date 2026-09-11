"""Redis Streams transport adapter with consumer-group semantics.

The SQL Inbox/Outbox remains the correctness source.  This adapter intentionally
allows duplicate stream events; consumers must claim an Inbox before executing.
"""

from __future__ import annotations

import json
from typing import Any

from .models import OutboxRecord, to_primitive


class RedisStreamMessageBus:
    def __init__(self, redis_url: str, *, stream: str = "trpc-agent:runtime") -> None:
        from redis import Redis

        self._redis = Redis.from_url(redis_url, decode_responses=True)
        self._stream = stream

    def publish(self, event: OutboxRecord, partition_key: str) -> None:
        payload = to_primitive(event)
        self._redis.xadd(
            self._stream,
            {
                "outbox_id": event.outbox_id,
                "tenant_id": event.tenant_id,
                "event_type": event.event_type,
                "partition_key": partition_key,
                "payload": json.dumps(payload, separators=(",", ":"), sort_keys=True),
            },
        )

    def check(self) -> None:
        """Verify the live Redis dependency without creating a Stream entry."""

        if not self._redis.ping():
            raise RuntimeError("Redis health check returned false")

    def ensure_group(self, group: str) -> None:
        try:
            self._redis.xgroup_create(self._stream, group, id="0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def consume(
        self, group: str, consumer: str, *, count: int = 10, block_ms: int = 5000
    ) -> list[tuple[str, dict[str, Any]]]:
        self.ensure_group(group)
        messages = self._redis.xreadgroup(
            group, consumer, {self._stream: ">"}, count=count, block=block_ms
        )
        result: list[tuple[str, dict[str, Any]]] = []
        for _, rows in messages:
            for message_id, fields in rows:
                result.append((str(message_id), json.loads(str(fields["payload"]))))
        return result

    def acknowledge(self, group: str, message_id: str) -> None:
        self._redis.xack(self._stream, group, message_id)

    def reclaim_idle(
        self, group: str, consumer: str, *, min_idle_ms: int = 60_000, count: int = 10
    ) -> list[tuple[str, dict[str, Any]]]:
        """Claim abandoned pending messages; Inbox idempotency makes this safe."""

        self.ensure_group(group)
        next_id, rows, _ = self._redis.xautoclaim(
            self._stream, group, consumer, min_idle_ms, "0-0", count=count
        )
        del next_id
        return [
            (str(message_id), json.loads(str(fields["payload"]))) for message_id, fields in rows
        ]
