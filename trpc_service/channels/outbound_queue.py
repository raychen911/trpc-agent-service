"""Redis-backed durable outbound delivery queue."""

from __future__ import annotations

import json
import os
import time
from uuid import uuid4

from trpc_service.security.secrets import redact_secret_text


def _is_transient_redis_error(exc: Exception) -> bool:
    return exc.__class__.__name__ in {"TimeoutError", "ConnectionError", "BusyLoadingError"}


class OutboundDeliveryQueue:
    """At-least-once outbound queue with visibility timeout and dead letters."""

    def __init__(
        self,
        url: str | None = None,
        prefix: str = "trpc-agent",
        max_attempts: int = 5,
        visibility_timeout: int = 180,
    ) -> None:
        import redis

        self.client = redis.Redis.from_url(
            url or os.getenv("OUTBOUND_QUEUE_URL", "redis://localhost:6379/0"),
            decode_responses=True,
        )
        self.prefix = prefix
        self.queue_key = f"{prefix}:outbound:requests"
        self.processing_key = f"{prefix}:outbound:processing"
        self.processing_meta_key = f"{prefix}:outbound:processing-meta"
        self.data_key = f"{prefix}:outbound:data"
        self.dead_letter_key = f"{prefix}:outbound:dead-letter"
        self.max_attempts = max_attempts
        self.visibility_timeout = visibility_timeout
        self.orphan_grace_seconds = float(os.getenv("OUTBOUND_ORPHAN_GRACE_SECONDS", "5"))
        self._orphan_seen_at: dict[str, float] = {}

    def enqueue(self, item: dict, task_id: str | None = None) -> str:
        task_id = task_id or str(uuid4())
        encoded = json.dumps(
            {**item, "task_id": task_id, "attempt": int(item.get("attempt", 0))},
            ensure_ascii=False,
            default=str,
        )
        if self.client.hsetnx(self.data_key, task_id, encoded):
            self.client.rpush(self.queue_key, task_id)
        return task_id

    def update(self, item: dict) -> None:
        """Persist a progress update for an item already in flight."""

        task_id = item.get("task_id")
        if not task_id:
            raise KeyError("outbound task missing task_id")
        existing_raw = self.client.hget(self.data_key, str(task_id))
        existing = json.loads(existing_raw) if existing_raw else {}
        existing.update(item)
        self.client.hset(
            self.data_key,
            str(task_id),
            json.dumps(existing, ensure_ascii=False, default=str),
        )

    def consume(self, handler, on_dead_letter=None, timeout: int = 5) -> None:
        while True:
            self.consume_once(handler, on_dead_letter=on_dead_letter, timeout=timeout)

    def consume_once(self, handler, on_dead_letter=None, timeout: int = 1) -> bool:
        try:
            self.requeue_stale()
            task_id = self._move_to_processing(timeout)
        except Exception as exc:
            if _is_transient_redis_error(exc):
                if exc.__class__.__name__ != "TimeoutError":
                    time.sleep(1)
                return False
            raise
        if not task_id:
            return False
        raw = self.client.hget(self.data_key, task_id)
        if not raw:
            self._remove_processing(task_id)
            return True
        try:
            item = json.loads(raw)
            if not isinstance(item, dict) or not item.get("tenant_id") or not item.get("messages"):
                raise ValueError("outbound payload is missing tenant_id or messages")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            self._dead_letter_raw(task_id, raw, f"malformed outbound payload: {exc}")
            return True
        try:
            handler(item)
        except Exception as exc:
            item["attempt"] = int(item.get("attempt", 0)) + 1
            error = redact_secret_text(f"{type(exc).__name__}: {exc}")
            self._remove_processing(task_id)
            if item["attempt"] < self.max_attempts:
                self.client.hset(
                    self.data_key,
                    task_id,
                    json.dumps(item, ensure_ascii=False, default=str),
                )
                self.client.rpush(self.queue_key, task_id)
            else:
                self.client.rpush(
                    self.dead_letter_key,
                    json.dumps({"item": item, "error": error}, ensure_ascii=False),
                )
                if on_dead_letter:
                    on_dead_letter(item, error)
        else:
            self._remove_processing(task_id)
            self.client.hdel(self.data_key, task_id)
        return True

    def requeue_stale(self) -> int:
        recovered = 0
        now = time.time()
        for raw_id in self.client.lrange(self.processing_key, 0, -1):
            task_id = str(raw_id)
            metadata = self.client.hget(self.processing_meta_key, task_id)
            if metadata:
                try:
                    claimed_at = float(json.loads(metadata).get("claimed_at", now))
                except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
                    self.client.hdel(self.processing_meta_key, task_id)
                    claimed_at = 0.0
                self._orphan_seen_at.pop(task_id, None)
            else:
                first_seen = self._orphan_seen_at.setdefault(task_id, now)
                if now - first_seen < self.orphan_grace_seconds:
                    continue
                claimed_at = 0.0
            if claimed_at and now - claimed_at < self.visibility_timeout:
                continue
            raw = self.client.hget(self.data_key, task_id)
            self._remove_processing(task_id)
            if not raw:
                continue
            try:
                item = json.loads(raw)
                if not isinstance(item, dict) or not item.get("tenant_id") or not item.get("messages"):
                    raise ValueError("outbound payload is missing tenant_id or messages")
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                self._dead_letter_raw(task_id, raw, f"malformed outbound payload: {exc}")
                recovered += 1
                continue
            item["attempt"] = int(item.get("attempt", 0)) + 1
            if item["attempt"] < self.max_attempts:
                self.client.hset(
                    self.data_key,
                    task_id,
                    json.dumps(item, ensure_ascii=False, default=str),
                )
                self.client.rpush(self.queue_key, task_id)
            else:
                self.client.rpush(
                    self.dead_letter_key,
                    json.dumps(
                        {"item": item, "error": "outbound visibility timeout exceeded"},
                        ensure_ascii=False,
                    ),
                )
            recovered += 1
            self._orphan_seen_at.pop(task_id, None)
        return recovered

    def _move_to_processing(self, timeout: int):
        script = """
        local task_id = redis.call('RPOP', KEYS[1])
        if not task_id then
          return nil
        end
        redis.call('LPUSH', KEYS[2], task_id)
        redis.call('HSET', KEYS[3], task_id, cjson.encode({claimed_at=tonumber(ARGV[1])}))
        return task_id
        """
        deadline = time.monotonic() + max(0, timeout)
        while True:
            try:
                task_id = self.client.eval(
                    script,
                    3,
                    self.queue_key,
                    self.processing_key,
                    self.processing_meta_key,
                    time.time(),
                )
            except AttributeError:
                break
            except Exception as exc:
                # redis-py can raise its own TimeoutError when the socket
                # timeout matches the blocking pop timeout. An empty queue
                # is a normal polling result and must not kill the consumer.
                if _is_transient_redis_error(exc):
                    return None
                raise
            if task_id or not timeout or time.monotonic() >= deadline:
                return task_id
            time.sleep(0.05)
        if timeout:
            try:
                task_id = self.client.brpoplpush(self.queue_key, self.processing_key, timeout=timeout)
                if task_id:
                    self.client.hset(
                        self.processing_meta_key,
                        task_id,
                        json.dumps({"claimed_at": time.time()}),
                    )
                return task_id
            except Exception as exc:
                if _is_transient_redis_error(exc):
                    return None
                raise
        try:
            task_id = self.client.lmove(self.queue_key, self.processing_key, "RIGHT", "RIGHT")
        except (AttributeError, TypeError):
            task_id = self.client.rpoplpush(self.queue_key, self.processing_key)
        if task_id:
            self.client.hset(
                self.processing_meta_key,
                task_id,
                json.dumps({"claimed_at": time.time()}),
            )
        return task_id

    def _remove_processing(self, task_id: str) -> None:
        self.client.lrem(self.processing_key, 1, task_id)
        self.client.hdel(self.processing_meta_key, task_id)

    def _dead_letter_raw(self, task_id: str, raw, error: str) -> None:
        self.client.rpush(
            self.dead_letter_key,
            json.dumps(
                {
                    "task_id": task_id,
                    "payload": str(raw)[:10000],
                    "error": redact_secret_text(str(error))[:1000],
                },
                ensure_ascii=False,
            ),
        )
        self._remove_processing(task_id)

    def close(self) -> None:
        close = getattr(self.client, "close", None)
        if close:
            close()
