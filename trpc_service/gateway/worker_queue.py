"""Redis queue transport used by Gateway and independent Worker processes."""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict
from threading import Event, Thread
from uuid import uuid4

from trpc_service.channels.base import sanitize_event_payload
from trpc_service.gateway.redis_streams import RedisStreamsTransport
from trpc_service.security.secrets import redact_secret_text
from trpc_service.tenant.models import RunRequest, TenantConfig, TenantContext, UserInput


def _is_transient_redis_error(exc: Exception) -> bool:
    return exc.__class__.__name__ in {"TimeoutError", "ConnectionError", "BusyLoadingError"}


def _bounded_webhook_result(value) -> dict:
    """Keep task status useful without turning Redis into an answer store."""

    if not isinstance(value, dict):
        return {}
    result = {}
    for key in ("ok", "accepted", "durable", "duplicate", "revoked", "session_id", "response_ref", "task_id"):
        if key in value:
            result[key] = value[key]
    if "answer" in value:
        answer = str(value["answer"] or "")
        result["answer_length"] = len(answer)
    return result


class WorkerQueue:
    def __init__(
        self,
        url: str | None = None,
        prefix: str = "trpc-agent",
        max_attempts: int = 3,
        visibility_timeout: int | None = None,
    ) -> None:
        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("Worker queue requires the 'redis' package") from exc
        self.client = redis.Redis.from_url(
            url or os.getenv("WORKER_QUEUE_URL", "redis://localhost:6379/0"), decode_responses=True
        )
        self.prefix = prefix
        self.queue_key = f"{prefix}:worker:requests"
        self.processing_key = f"{prefix}:worker:processing"
        self.processing_meta_key = f"{prefix}:worker:processing-meta"
        self.dead_letter_key = f"{prefix}:worker:dead-letter"
        self.max_attempts = max_attempts
        self.visibility_timeout = visibility_timeout or int(os.getenv("WORKER_VISIBILITY_TIMEOUT_SECONDS", "180"))
        self.orphan_grace_seconds = float(os.getenv("WORKER_ORPHAN_GRACE_SECONDS", "5"))
        self._orphan_seen_at: dict[str, float] = {}
        self.streams = None
        if os.getenv("WORKER_QUEUE_TRANSPORT", "list").strip().lower() in {"stream", "streams"}:
            self.streams = RedisStreamsTransport(
                url or os.getenv("WORKER_QUEUE_URL"),
                prefix=prefix,
                stream="worker",
                group=os.getenv("WORKER_STREAM_GROUP", "workers"),
                consumer=os.getenv("WORKER_STREAM_CONSUMER") or None,
                max_attempts=max_attempts,
            )

    def submit(self, request: RunRequest, config: TenantConfig, timeout: float | None = None) -> list[dict]:
        timeout = timeout if timeout is not None else float(os.getenv("WORKER_RESULT_TIMEOUT_SECONDS", "60"))
        request_id = str(uuid4())
        result_key = f"{self.prefix}:worker:result:{request_id}"
        payload = {
            "request_id": request_id,
            "attempt": 0,
            "request": asdict(request),
            "config": config.to_dict(),
            "traceparent": request.tenant_context.traceparent,
        }
        encoded = json.dumps(payload, ensure_ascii=False, default=str)
        if self.streams is not None:
            self.streams.submit(payload)
        else:
            self.client.rpush(self.queue_key, encoded)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = self.client.get(result_key)
            if result:
                self.client.delete(result_key)
                data = json.loads(result)
                if data.get("error"):
                    raise RuntimeError(data["error"])
                return data.get("events", [])
            time.sleep(0.05)
        raise TimeoutError("worker did not complete request before timeout")

    def consume(self, handler, timeout: int = 5) -> None:
        if self.streams is not None:
            stream_handler = self._stream_handler(handler)
            while True:
                try:
                    self.streams.consume_once(
                        stream_handler,
                        block_ms=max(0, int(timeout * 1000)),
                    )
                except Exception as exc:
                    if _is_transient_redis_error(exc):
                        if exc.__class__.__name__ != "TimeoutError":
                            time.sleep(1)
                        continue
                    raise
            return
        while True:
            try:
                self.requeue_stale()
                item = self._claim(timeout)
            except Exception as exc:
                if _is_transient_redis_error(exc):
                    if exc.__class__.__name__ != "TimeoutError":
                        time.sleep(1)
                    continue
                raise
            if not item:
                continue
            raw = item
            try:
                payload = json.loads(raw)
                if not isinstance(payload, dict) or not payload.get("request_id"):
                    raise ValueError("worker payload must be an object with request_id")
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                self._dead_letter_raw(raw, f"malformed worker payload: {exc}")
                continue
            request_id = payload["request_id"]
            heartbeat_stop = self._start_heartbeat(request_id)
            try:
                request_data = payload["request"]
                context = TenantContext(**request_data["tenant_context"])
                request = RunRequest(
                    tenant_context=context,
                    user_input=UserInput(**request_data["user_input"]),
                    idempotency_key=request_data["idempotency_key"],
                )
                config = TenantConfig.from_dict(payload["config"])
                events = handler(request, config)
                result = {"events": [asdict(event) for event in events]}
            except Exception as exc:
                payload["attempt"] = int(payload.get("attempt", 0)) + 1
                if payload["attempt"] < self.max_attempts:
                    self.client.rpush(self.queue_key, json.dumps(payload, ensure_ascii=False, default=str))
                    result = None
                else:
                    error = redact_secret_text(f"{type(exc).__name__}: {exc}")
                    self.client.rpush(
                        self.dead_letter_key,
                        json.dumps(
                            {"payload": payload, "error": error},
                            ensure_ascii=False,
                            default=str,
                        ),
                    )
                    result = {"error": error}
            finally:
                heartbeat_stop.set()
                self.client.lrem(self.processing_key, 1, raw)
                self.client.hdel(self.processing_meta_key, request_id)
            if result is not None:
                self.client.setex(
                    f"{self.prefix}:worker:result:{request_id}",
                    300,
                    json.dumps(result, ensure_ascii=False, default=str),
                )

    def _stream_handler(self, handler):
        def process(payload: dict) -> None:
            request_id = str(payload["request_id"])
            try:
                request_data = payload["request"]
                context = TenantContext(**request_data["tenant_context"])
                request = RunRequest(
                    tenant_context=context,
                    user_input=UserInput(**request_data["user_input"]),
                    idempotency_key=request_data["idempotency_key"],
                )
                config = TenantConfig.from_dict(payload["config"])
                events = handler(request, config)
                result = {"events": [asdict(event) for event in events]}
            except Exception as exc:
                # Redis Streams owns retry scheduling. Do not publish a
                # terminal result for a transient delivery attempt: Gateway
                # would consume it and fail idempotency before a later retry
                # can publish the successful result. Only the final attempt
                # publishes an error for the waiting Gateway.
                attempt = int(payload.get("attempt", 0)) + 1
                if attempt >= self.max_attempts:
                    error = redact_secret_text(f"{type(exc).__name__}: {exc}")
                    self.client.setex(
                        f"{self.prefix}:worker:result:{request_id}",
                        300,
                        json.dumps({"error": error}, ensure_ascii=False),
                    )
                raise
            self.client.setex(
                f"{self.prefix}:worker:result:{request_id}",
                300,
                json.dumps(result, ensure_ascii=False, default=str),
            )

        return process

    def requeue_stale(self) -> int:
        """Return requests abandoned by a crashed Worker to the pending queue.

        The queue is at-least-once by design. Gateway idempotency makes a
        recovered delivery safe even if a Worker dies after its external call.
        """
        recovered = self._recover_orphaned()
        now = time.time()
        for request_id, encoded in self.client.hscan_iter(self.processing_meta_key):
            try:
                metadata = json.loads(encoded)
            except (TypeError, ValueError, json.JSONDecodeError):
                self.client.hdel(self.processing_meta_key, request_id)
                continue
            try:
                claimed_at = float(metadata.get("claimed_at", now))
            except (AttributeError, TypeError, ValueError):
                self.client.hdel(self.processing_meta_key, request_id)
                continue
            if now - claimed_at < self.visibility_timeout:
                continue
            raw = str(metadata.get("raw", ""))
            if not raw or not self.client.lrem(self.processing_key, 1, raw):
                self.client.hdel(self.processing_meta_key, request_id)
                continue
            try:
                payload = json.loads(raw)
                if not isinstance(payload, dict) or not payload.get("request_id"):
                    raise ValueError("worker payload must be an object with request_id")
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                self._dead_letter_raw(raw, f"malformed worker payload: {exc}")
                self.client.hdel(self.processing_meta_key, request_id)
                recovered += 1
                continue
            payload["attempt"] = int(payload.get("attempt", 0)) + 1
            if payload["attempt"] < self.max_attempts:
                self.client.rpush(
                    self.queue_key,
                    json.dumps(payload, ensure_ascii=False, default=str),
                )
            else:
                self.client.rpush(
                    self.dead_letter_key,
                    json.dumps(
                        {
                            "payload": payload,
                            "error": "worker visibility timeout exceeded",
                        },
                        ensure_ascii=False,
                        default=str,
                    ),
                )
                self.client.setex(
                    f"{self.prefix}:worker:result:{request_id}",
                    300,
                    json.dumps({"error": "worker visibility timeout exceeded"}),
                )
            self.client.hdel(self.processing_meta_key, request_id)
            recovered += 1
        return recovered

    def _recover_orphaned(self) -> int:
        """Recover entries moved to processing before metadata was written."""
        known = {str(request_id) for request_id, _ in self.client.hscan_iter(self.processing_meta_key)}
        recovered = 0
        now = time.time()
        for raw in self.client.lrange(self.processing_key, 0, -1):
            try:
                payload = json.loads(raw)
                if not isinstance(payload, dict) or not payload.get("request_id"):
                    raise ValueError("worker payload must be an object with request_id")
                request_id = str(payload["request_id"])
            except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
                self._dead_letter_raw(raw, f"malformed worker payload: {exc}")
                recovered += 1
                continue
            if request_id in known:
                self._orphan_seen_at.pop(request_id, None)
                continue
            first_seen = self._orphan_seen_at.setdefault(request_id, now)
            if now - first_seen < self.orphan_grace_seconds:
                continue
            if not self.client.lrem(self.processing_key, 1, raw):
                continue
            payload["attempt"] = int(payload.get("attempt", 0)) + 1
            if payload["attempt"] < self.max_attempts:
                self.client.rpush(self.queue_key, json.dumps(payload, ensure_ascii=False, default=str))
            else:
                self.client.rpush(
                    self.dead_letter_key,
                    json.dumps(
                        {"payload": payload, "error": "worker claim metadata missing"},
                        ensure_ascii=False,
                        default=str,
                    ),
                )
                self.client.setex(
                    f"{self.prefix}:worker:result:{request_id}",
                    300,
                    json.dumps({"error": "worker claim metadata missing"}),
                )
            recovered += 1
            self._orphan_seen_at.pop(request_id, None)
        return recovered

    def _start_heartbeat(self, request_id: str) -> Event:
        stop = Event()
        interval = max(1.0, min(float(self.visibility_timeout) / 3.0, 10.0))

        def beat() -> None:
            while not stop.wait(interval):
                encoded = self.client.hget(self.processing_meta_key, request_id)
                if not encoded:
                    return
                metadata = json.loads(encoded)
                metadata["claimed_at"] = time.time()
                self.client.hset(self.processing_meta_key, request_id, json.dumps(metadata))

        Thread(target=beat, name=f"worker-heartbeat-{request_id}", daemon=True).start()
        return stop

    def close(self) -> None:
        if self.streams is not None:
            self.streams.close()
        close = getattr(self.client, "close", None)
        if close:
            close()

    def _claim(self, timeout: int = 5):
        """Atomically move a request to processing and create claim metadata."""

        deadline = time.monotonic() + max(0, timeout)
        script = """
        local item = redis.call('RPOP', KEYS[1])
        if not item then
          return nil
        end
        redis.call('LPUSH', KEYS[2], item)
        local ok, payload = pcall(cjson.decode, item)
        if ok and payload['request_id'] then
          redis.call('HSET', KEYS[3], payload['request_id'], cjson.encode({raw=item, claimed_at=tonumber(ARGV[1])}))
        end
        return item
        """
        while True:
            try:
                item = self.client.eval(
                    script,
                    3,
                    self.queue_key,
                    self.processing_key,
                    self.processing_meta_key,
                    time.time(),
                )
            except (AttributeError, TypeError):
                item = self.client.brpoplpush(self.queue_key, self.processing_key, timeout=timeout)
                if item:
                    try:
                        payload = json.loads(item)
                        if isinstance(payload, dict) and payload.get("request_id"):
                            self.client.hset(
                                self.processing_meta_key,
                                payload["request_id"],
                                json.dumps({"raw": item, "claimed_at": time.time()}),
                            )
                    except (TypeError, ValueError, json.JSONDecodeError):
                        pass
                return item
            if item or not timeout or time.monotonic() >= deadline:
                return item
            time.sleep(0.05)

    def _dead_letter_raw(self, raw, error: str) -> None:
        self.client.rpush(
            self.dead_letter_key,
            json.dumps(
                {
                    "payload": str(raw)[:10000],
                    "error": redact_secret_text(str(error))[:1000],
                },
                ensure_ascii=False,
            ),
        )
        self.client.lrem(self.processing_key, 1, raw)


class DurableWebhookQueue:
    """Redis-backed at-least-once queue for webhook callbacks.

    The callback process acknowledges the HTTP request only after the payload
    is stored. A crashed consumer is recovered after the visibility timeout.
    """

    def __init__(
        self,
        url: str | None = None,
        prefix: str = "trpc-agent",
        max_attempts: int = 5,
        visibility_timeout: int = 180,
    ) -> None:
        import redis

        self.client = redis.Redis.from_url(
            url or os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
        )
        self.prefix = prefix
        self.queue_key = f"{prefix}:webhook:requests"
        self.processing_key = f"{prefix}:webhook:processing"
        self.processing_meta_key = f"{prefix}:webhook:processing-meta"
        self.dead_letter_key = f"{prefix}:webhook:dead-letter"
        self.status_prefix = f"{prefix}:webhook:status:"
        self.max_attempts = max_attempts
        self.visibility_timeout = visibility_timeout
        self.orphan_grace_seconds = float(os.getenv("WEBHOOK_ORPHAN_GRACE_SECONDS", "5"))
        self._orphan_seen_at: dict[str, float] = {}

    def submit(
        self,
        channel: str,
        account_id: str,
        payload: dict,
        traceparent: str | None,
        tenant_id: str | None = None,
    ) -> str:
        task_id = str(uuid4())
        item = {
            "task_id": task_id,
            "attempt": 0,
            "channel": channel,
            "account_id": account_id,
            "tenant_id": tenant_id,
            "payload": sanitize_event_payload(payload),
            "traceparent": traceparent,
            "created_at": time.time(),
        }
        self.client.rpush(self.queue_key, json.dumps(item, ensure_ascii=False, default=str))
        self._set_status(
            task_id,
            {
                "task_id": task_id,
                "tenant_id": tenant_id,
                "channel": channel,
                "account_id": account_id,
                "status": "accepted",
                "attempt": 0,
                "created_at": item["created_at"],
            },
        )
        return task_id

    def status(self, task_id: str) -> dict | None:
        """Return a bounded, non-secret lifecycle record for a webhook task."""

        encoded = self.client.get(f"{self.status_prefix}{task_id}")
        if not encoded:
            return None
        try:
            value = json.loads(encoded)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {"task_id": task_id, "status": "unknown"}
        return value if isinstance(value, dict) else {"task_id": task_id, "status": "unknown"}

    def _set_status(self, task_id: str, value: dict, ttl: int = 86_400) -> None:
        self.client.setex(
            f"{self.status_prefix}{task_id}",
            ttl,
            json.dumps(value, ensure_ascii=False, default=str),
        )

    def consume(self, handler, timeout: int = 5) -> None:
        while True:
            self.consume_once(handler, timeout)

    def consume_once(self, handler, timeout: int = 1) -> bool:
        try:
            self.requeue_stale()
            raw = self._claim(timeout)
        except Exception as exc:
            # redis-py may surface a socket timeout instead of returning
            # None when the blocking-pop timeout expires.
            if _is_transient_redis_error(exc):
                if exc.__class__.__name__ != "TimeoutError":
                    time.sleep(1)
                return False
            raise
        if not raw:
            return False
        try:
            item = json.loads(raw)
            if not isinstance(item, dict) or not item.get("task_id"):
                raise ValueError("webhook payload must be an object with task_id")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            self._dead_letter_raw(raw, f"malformed webhook payload: {exc}")
            return True
        task_id = item["task_id"]
        heartbeat_stop = self._start_heartbeat(task_id)
        self._set_status(
            task_id,
            {
                "task_id": task_id,
                "tenant_id": item.get("tenant_id"),
                "channel": item.get("channel"),
                "account_id": item.get("account_id"),
                "status": "processing",
                "attempt": int(item.get("attempt", 0)) + 1,
                "created_at": item.get("created_at"),
            },
        )
        try:
            result = handler(item)
            status = {
                "task_id": task_id,
                "tenant_id": item.get("tenant_id"),
                "channel": item.get("channel"),
                "account_id": item.get("account_id"),
                "status": "completed",
                "attempt": int(item.get("attempt", 0)) + 1,
                "created_at": item.get("created_at"),
                "result": _bounded_webhook_result(result),
            }
            self._set_status(task_id, status)
        except Exception as exc:
            item["attempt"] = int(item.get("attempt", 0)) + 1
            encoded = json.dumps(item, ensure_ascii=False, default=str)
            if item["attempt"] < self.max_attempts:
                self.client.rpush(self.queue_key, encoded)
                self._set_status(
                    task_id,
                    {
                        "task_id": task_id,
                        "tenant_id": item.get("tenant_id"),
                        "channel": item.get("channel"),
                        "account_id": item.get("account_id"),
                        "status": "retrying",
                        "attempt": item["attempt"],
                        "created_at": item.get("created_at"),
                        "error": redact_secret_text(f"{type(exc).__name__}: {exc}")[:500],
                    },
                )
            else:
                self.client.rpush(
                    self.dead_letter_key,
                    json.dumps(
                        {
                            "item": item,
                            "error": redact_secret_text(f"{type(exc).__name__}: {exc}"),
                        },
                        ensure_ascii=False,
                        default=str,
                    ),
                )
                self._set_status(
                    task_id,
                    {
                        "task_id": task_id,
                        "tenant_id": item.get("tenant_id"),
                        "channel": item.get("channel"),
                        "account_id": item.get("account_id"),
                        "status": "dead",
                        "attempt": item["attempt"],
                        "created_at": item.get("created_at"),
                        "error": redact_secret_text(f"{type(exc).__name__}: {exc}")[:500],
                    },
                )
        finally:
            heartbeat_stop.set()
            self.client.lrem(self.processing_key, 1, raw)
            self.client.hdel(self.processing_meta_key, task_id)
        return True

    def requeue_stale(self) -> int:
        recovered = self._recover_orphaned()
        now = time.time()
        for task_id, encoded in self.client.hscan_iter(self.processing_meta_key):
            try:
                metadata = json.loads(encoded)
                claimed_at = float(metadata.get("claimed_at", now))
            except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
                self.client.hdel(self.processing_meta_key, task_id)
                continue
            if now - claimed_at < self.visibility_timeout:
                continue
            raw = str(metadata.get("raw", ""))
            if not raw or not self.client.lrem(self.processing_key, 1, raw):
                self.client.hdel(self.processing_meta_key, task_id)
                continue
            try:
                item = json.loads(raw)
                if not isinstance(item, dict) or not item.get("task_id"):
                    raise ValueError("webhook payload must be an object with task_id")
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                self._dead_letter_raw(raw, f"malformed webhook payload: {exc}")
                self.client.hdel(self.processing_meta_key, task_id)
                recovered += 1
                continue
            item["attempt"] = int(item.get("attempt", 0)) + 1
            if item["attempt"] < self.max_attempts:
                self.client.rpush(
                    self.queue_key,
                    json.dumps(item, ensure_ascii=False, default=str),
                )
                self._set_status(
                    task_id,
                    {
                        "task_id": task_id,
                        "tenant_id": item.get("tenant_id"),
                        "channel": item.get("channel"),
                        "account_id": item.get("account_id"),
                        "status": "retrying",
                        "attempt": item["attempt"],
                        "created_at": item.get("created_at"),
                        "error": "webhook visibility timeout exceeded",
                    },
                )
            else:
                self.client.rpush(
                    self.dead_letter_key,
                    json.dumps(
                        {"item": item, "error": "webhook visibility timeout exceeded"},
                        ensure_ascii=False,
                        default=str,
                    ),
                )
                self._set_status(
                    task_id,
                    {
                        "task_id": task_id,
                        "tenant_id": item.get("tenant_id"),
                        "channel": item.get("channel"),
                        "account_id": item.get("account_id"),
                        "status": "dead",
                        "attempt": item["attempt"],
                        "created_at": item.get("created_at"),
                        "error": "webhook visibility timeout exceeded",
                    },
                )
            recovered += 1
            self.client.hdel(self.processing_meta_key, task_id)
        return recovered

    def _recover_orphaned(self) -> int:
        """Recover webhook entries moved before processing metadata was written."""
        known = {str(task_id) for task_id, _ in self.client.hscan_iter(self.processing_meta_key)}
        recovered = 0
        now = time.time()
        for raw in self.client.lrange(self.processing_key, 0, -1):
            try:
                item = json.loads(raw)
                if not isinstance(item, dict) or not item.get("task_id"):
                    raise ValueError("webhook payload must be an object with task_id")
                task_id = str(item["task_id"])
            except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
                self._dead_letter_raw(raw, f"malformed webhook payload: {exc}")
                recovered += 1
                continue
            if task_id in known:
                self._orphan_seen_at.pop(task_id, None)
                continue
            first_seen = self._orphan_seen_at.setdefault(task_id, now)
            if now - first_seen < self.orphan_grace_seconds:
                continue
            if not self.client.lrem(self.processing_key, 1, raw):
                continue
            item["attempt"] = int(item.get("attempt", 0)) + 1
            if item["attempt"] < self.max_attempts:
                self.client.rpush(self.queue_key, json.dumps(item, ensure_ascii=False, default=str))
            else:
                self.client.rpush(
                    self.dead_letter_key,
                    json.dumps(
                        {"item": item, "error": "webhook claim metadata missing"},
                        ensure_ascii=False,
                        default=str,
                    ),
                )
            recovered += 1
            self._orphan_seen_at.pop(task_id, None)
        return recovered

    def _start_heartbeat(self, task_id: str) -> Event:
        stop = Event()
        interval = max(1.0, min(float(self.visibility_timeout) / 3.0, 10.0))

        def beat() -> None:
            while not stop.wait(interval):
                encoded = self.client.hget(self.processing_meta_key, task_id)
                if not encoded:
                    return
                metadata = json.loads(encoded)
                metadata["claimed_at"] = time.time()
                self.client.hset(self.processing_meta_key, task_id, json.dumps(metadata))

        Thread(target=beat, name=f"webhook-heartbeat-{task_id}", daemon=True).start()
        return stop

    def close(self) -> None:
        close = getattr(self.client, "close", None)
        if close:
            close()

    def _claim(self, timeout: int = 5):
        """Atomically move a webhook item to processing and create metadata."""

        deadline = time.monotonic() + max(0, timeout)
        script = """
        local item = redis.call('RPOP', KEYS[1])
        if not item then
          return nil
        end
        redis.call('LPUSH', KEYS[2], item)
        local ok, payload = pcall(cjson.decode, item)
        if ok and payload['task_id'] then
          redis.call('HSET', KEYS[3], payload['task_id'], cjson.encode({raw=item, claimed_at=tonumber(ARGV[1])}))
        end
        return item
        """
        while True:
            try:
                item = self.client.eval(
                    script,
                    3,
                    self.queue_key,
                    self.processing_key,
                    self.processing_meta_key,
                    time.time(),
                )
            except (AttributeError, TypeError):
                item = self.client.brpoplpush(self.queue_key, self.processing_key, timeout=timeout)
                if item:
                    try:
                        payload = json.loads(item)
                        if isinstance(payload, dict) and payload.get("task_id"):
                            self.client.hset(
                                self.processing_meta_key,
                                payload["task_id"],
                                json.dumps({"raw": item, "claimed_at": time.time()}),
                            )
                    except (TypeError, ValueError, json.JSONDecodeError):
                        pass
                return item
            if item or not timeout or time.monotonic() >= deadline:
                return item
            time.sleep(0.05)

    def _dead_letter_raw(self, raw, error: str) -> None:
        self.client.rpush(
            self.dead_letter_key,
            json.dumps(
                {
                    "payload": str(raw)[:10000],
                    "error": redact_secret_text(str(error))[:1000],
                },
                ensure_ascii=False,
            ),
        )
        self.client.lrem(self.processing_key, 1, raw)
