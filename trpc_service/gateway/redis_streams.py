"""Optional Redis Streams transport for reconstructible worker delivery.

The durable Inbox/Outbox remains the source of truth. This transport only
moves serialized work between Gateway and Worker processes and can be rebuilt
from the database after a Redis loss.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any
from uuid import uuid4

from trpc_service.security.secrets import redact_secret_text


class RedisStreamsTransport:
    def __init__(
        self,
        url: str | None = None,
        prefix: str = "trpc-agent",
        stream: str = "worker",
        group: str = "workers",
        consumer: str | None = None,
        max_attempts: int = 3,
        stream_key: str | None = None,
    ) -> None:
        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("Redis Streams transport requires the 'redis' package") from exc
        self.client = redis.Redis.from_url(
            url or os.getenv("WORKER_QUEUE_URL", "redis://localhost:6379/0"),
            decode_responses=True,
        )
        self.stream_key = stream_key or f"{prefix}:{stream}:stream"
        self.dead_letter_key = f"{prefix}:{stream}:dead-letter"
        self.group = group
        self.consumer = consumer or f"{stream}-{uuid4()}"
        self.max_attempts = max(1, int(max_attempts))
        default_reclaim_interval = max(1.0, float(os.getenv("WORKER_VISIBILITY_TIMEOUT_SECONDS", "180")) / 3.0)
        self.reclaim_interval_seconds = max(
            0.1,
            float(os.getenv("WORKER_STREAM_RECLAIM_INTERVAL_SECONDS", str(default_reclaim_interval))),
        )
        self._last_reclaim_at = 0.0
        self._ensure_group()

    def _ensure_group(self) -> None:
        try:
            self.client.xgroup_create(self.stream_key, self.group, id="0-0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def submit(self, payload: dict[str, Any]) -> str:
        return str(
            self.client.xadd(
                self.stream_key,
                {"payload": json.dumps(payload, ensure_ascii=False, default=str)},
            )
        )

    def consume_once(self, handler, block_ms: int = 1000) -> bool:
        reclaimed = self._reclaim_if_due(handler)
        items = self.client.xreadgroup(
            self.group,
            self.consumer,
            {self.stream_key: ">"},
            count=1,
            block=max(0, int(block_ms)),
        )
        if not items:
            if not reclaimed:
                reclaimed = self.requeue_stale(handler)
                self._last_reclaim_at = time.monotonic()
            return bool(reclaimed)
        _, messages = items[0]
        message_id, fields = messages[0]
        self._process(message_id, fields, handler)
        return True

    def _reclaim_if_due(self, handler) -> int:
        now = time.monotonic()
        if now - self._last_reclaim_at < self.reclaim_interval_seconds:
            return 0
        self._last_reclaim_at = now
        return self.requeue_stale(handler)

    def requeue_stale(self, handler, min_idle_ms: int | None = None) -> int:
        idle = int(
            min_idle_ms
            if min_idle_ms is not None
            else float(os.getenv("WORKER_VISIBILITY_TIMEOUT_SECONDS", "180")) * 1000
        )
        try:
            result = self.client.xautoclaim(
                self.stream_key,
                self.group,
                self.consumer,
                min_idle_time=idle,
                start_id="0-0",
                count=10,
            )
        except (AttributeError, TypeError):
            return 0
        messages = result[1] if isinstance(result, (list, tuple)) and len(result) > 1 else []
        for message_id, fields in messages:
            self._process(message_id, fields, handler)
        return len(messages)

    def _process(self, message_id: str, fields: dict[str, str], handler) -> None:
        raw = fields.get("payload") if isinstance(fields, dict) else None
        if not raw:
            self._dead_letter(message_id, raw, "stream message has no payload")
            self._ack(message_id)
            return

        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("payload must be a JSON object")
        except (UnicodeDecodeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            self._dead_letter(message_id, raw, f"malformed stream payload: {exc}")
            self._ack(message_id)
            return

        acknowledge = False
        try:
            handler(payload)
            acknowledge = True
        except Exception as exc:
            try:
                attempt = int(payload.get("attempt", 0)) + 1
            except (TypeError, ValueError):
                attempt = 1
            payload["attempt"] = attempt
            if attempt >= self.max_attempts:
                self._dead_letter(message_id, payload, redact_secret_text(str(exc))[:1000])
            else:
                # A new stream entry gives the retry a fresh delivery id while
                # the old pending entry is acknowledged and removed.
                self.submit(payload)
            acknowledge = True
        if acknowledge:
            self._ack(message_id)

    def _ack(self, message_id: str) -> None:
        self.client.xack(self.stream_key, self.group, message_id)
        self.client.xdel(self.stream_key, message_id)

    def _dead_letter(self, message_id: str, payload: Any, error: str) -> None:
        encoded = payload
        if not isinstance(payload, str):
            encoded = json.dumps(payload, ensure_ascii=False, default=str)
        self.client.xadd(
            self.dead_letter_key,
            {
                "message_id": str(message_id),
                "payload": redact_secret_text(str(encoded))[:10000],
                "error": redact_secret_text(str(error))[:1000],
            },
        )

    def close(self) -> None:
        close = getattr(self.client, "close", None)
        if close:
            close()
