"""Durable compensation queue for post-event derived-state writes."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
import json
import os
from threading import RLock
from uuid import uuid4

from trpc_service.security.secrets import redact_secret_text
from trpc_service.storage.base import CompensationTask, now_utc


def _task_from_dict(value: dict) -> CompensationTask:
    item = dict(value)
    for field in ("available_at", "created_at", "updated_at"):
        raw = item.get(field)
        if isinstance(raw, str):
            item[field] = datetime.fromisoformat(raw)
    return CompensationTask(**item)


def _task_dict(task: CompensationTask) -> dict:
    return {
        "task_id": task.task_id,
        "tenant_id": task.tenant_id,
        "operation": task.operation,
        "payload": task.payload,
        "status": task.status,
        "attempt": task.attempt,
        "available_at": task.available_at.isoformat(),
        "last_error": task.last_error,
        "created_at": task.created_at.isoformat(),
        "updated_at": task.updated_at.isoformat(),
    }


def _decode_redis_text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _decode_task(value) -> CompensationTask:
    return _task_from_dict(json.loads(_decode_redis_text(value)))


class InMemoryCompensationStore:
    def __init__(self, max_attempts: int = 10) -> None:
        self._tasks: dict[str, CompensationTask] = {}
        self._lock = RLock()
        self.max_attempts = int(max_attempts)

    def enqueue(self, tenant_id, operation, payload, task_id=None):
        with self._lock:
            task_id = task_id or str(uuid4())
            existing = self._tasks.get(task_id)
            if existing:
                return deepcopy(existing)
            task = CompensationTask(
                task_id=task_id,
                tenant_id=tenant_id,
                operation=operation,
                payload=deepcopy(payload),
            )
            self._tasks[task_id] = task
            return deepcopy(task)

    def claim(self, limit=10, tenant_id=None):
        now = now_utc()
        result = []
        with self._lock:
            for task in self._tasks.values():
                if tenant_id is not None and task.tenant_id != tenant_id:
                    continue
                if task.status != "pending" or task.available_at > now:
                    continue
                task.status = "processing"
                task.attempt += 1
                task.updated_at = now
                result.append(deepcopy(task))
                if len(result) >= limit:
                    break
        return result

    def complete(self, task_id, tenant_id=None):
        with self._lock:
            task = self._tasks.get(task_id)
            if task and (tenant_id is None or task.tenant_id == tenant_id):
                task.status = "completed"
                task.updated_at = now_utc()

    def fail(self, task_id, error, retry_after_seconds=30, tenant_id=None):
        with self._lock:
            task = self._tasks.get(task_id)
            if task and (tenant_id is None or task.tenant_id == tenant_id):
                task.status = "dead" if task.attempt >= self.max_attempts else "pending"
                task.last_error = redact_secret_text(error)
                task.available_at = now_utc() + timedelta(seconds=retry_after_seconds)
                task.updated_at = now_utc()


def apply_compensation_task(storage, task: CompensationTask) -> None:
    """Apply one task using the same tenant-scoped StorageBundle interfaces."""
    if task.operation.startswith("mirror."):
        replay = getattr(storage, "replay_mirror_task", None)
        if replay is None:
            raise RuntimeError("mirror compensation requires mirrored storage")
        replay(task)
        return
    from trpc_service.storage.base import AuditRecord, MemoryItem, Summary

    if task.operation == "memory.put":
        storage.memory.put(MemoryItem.from_dict(task.payload))
    elif task.operation == "summary.put":
        storage.summary.put(Summary.from_dict(task.payload))
    elif task.operation == "audit.append":
        storage.audit.append(AuditRecord.from_dict(task.payload))
    else:
        raise ValueError(f"unsupported compensation operation: {task.operation}")


def replay_compensations(storage, limit=20, tenant_id=None) -> int:
    processed = 0
    for task in storage.compensation.claim(limit, tenant_id=tenant_id):
        try:
            apply_compensation_task(storage, task)
        except Exception as exc:
            storage.compensation.fail(
                task.task_id,
                f"{type(exc).__name__}: {exc}",
                tenant_id=task.tenant_id,
            )
        else:
            storage.compensation.complete(task.task_id, tenant_id=task.tenant_id)
            processed += 1
    return processed


class RedisCompensationStore:
    def __init__(
        self,
        client,
        prefix: str = "trpc-agent",
        visibility_timeout: int = 180,
        max_attempts: int = 10,
    ) -> None:
        self.client = client
        self.prefix = prefix
        self.queue_key = f"{prefix}:compensation:queue"
        self.processing_key = f"{prefix}:compensation:processing"
        self.processing_meta_key = f"{prefix}:compensation:processing-meta"
        self.data_key = f"{prefix}:compensation:data"
        self.dead_letter_key = f"{prefix}:compensation:dead-letter"
        self.visibility_timeout = int(visibility_timeout)
        self.max_attempts = int(max_attempts)
        self.orphan_grace_seconds = float(os.getenv("COMPENSATION_ORPHAN_GRACE_SECONDS", "5"))
        self._orphan_seen_at: dict[str, float] = {}

    def enqueue(self, tenant_id, operation, payload, task_id=None):
        task = CompensationTask(
            task_id=task_id or str(uuid4()),
            tenant_id=tenant_id,
            operation=operation,
            payload=dict(payload),
        )
        existing = self.client.hget(self.data_key, task.task_id)
        if existing:
            return _decode_task(existing)
        self.client.hset(
            self.data_key,
            task.task_id,
            json.dumps(_task_dict(task), ensure_ascii=False),
        )
        self.client.rpush(self.queue_key, task.task_id)
        return task

    def claim(self, limit=10, tenant_id=None):
        self.requeue_stale()
        result = []
        scan_limit = max(int(limit), int(self.client.llen(self.queue_key) or 0))
        for _ in range(scan_limit):
            if len(result) >= limit:
                break
            task_id = self._move_to_processing()
            if not task_id:
                break
            task_id_text = _decode_redis_text(task_id)
            raw = self.client.hget(self.data_key, task_id_text)
            if not raw:
                self.client.lrem(self.processing_key, 1, task_id_text)
                self.client.hdel(self.processing_meta_key, task_id_text)
                continue
            task = _decode_task(raw)
            if tenant_id is not None and task.tenant_id != tenant_id:
                self.client.lrem(self.processing_key, 1, task.task_id)
                self.client.hdel(self.processing_meta_key, task.task_id)
                self.client.rpush(self.queue_key, task.task_id)
                continue
            if task.status != "pending" or task.available_at > now_utc():
                self.client.lrem(self.processing_key, 1, task.task_id)
                self.client.hdel(self.processing_meta_key, task.task_id)
                if task.status == "pending":
                    self.client.rpush(self.queue_key, task.task_id)
                continue
            task.status = "processing"
            task.attempt += 1
            task.updated_at = now_utc()
            self.client.hset(self.data_key, task.task_id, json.dumps(_task_dict(task)))
            self.client.hset(
                self.processing_meta_key,
                task.task_id,
                json.dumps({"claimed_at": now_utc().timestamp()}),
            )
            result.append(task)
        return result

    def complete(self, task_id, tenant_id=None):
        raw = self.client.hget(self.data_key, task_id)
        if raw:
            task = _decode_task(raw)
            if tenant_id is not None and task.tenant_id != tenant_id:
                return
            task.status = "completed"
            task.updated_at = now_utc()
            self.client.hset(self.data_key, task_id, json.dumps(_task_dict(task)))
        self.client.lrem(self.processing_key, 1, task_id)
        self.client.hdel(self.processing_meta_key, task_id)

    def fail(self, task_id, error, retry_after_seconds=30, tenant_id=None):
        raw = self.client.hget(self.data_key, task_id)
        if not raw:
            return
        task = _decode_task(raw)
        if tenant_id is not None and task.tenant_id != tenant_id:
            return
        task.status = "dead" if task.attempt >= self.max_attempts else "pending"
        task.last_error = redact_secret_text(error)
        task.available_at = now_utc() + timedelta(seconds=retry_after_seconds)
        task.updated_at = now_utc()
        self.client.hset(self.data_key, task_id, json.dumps(_task_dict(task)))
        self.client.lrem(self.processing_key, 1, task_id)
        self.client.hdel(self.processing_meta_key, task_id)
        if task.status == "pending":
            self.client.rpush(self.queue_key, task_id)
        else:
            self.client.rpush(
                self.dead_letter_key,
                json.dumps(
                    {"task_id": task_id, "error": task.last_error},
                    ensure_ascii=False,
                ),
            )

    def requeue_stale(self) -> int:
        """Recover compensation tasks abandoned by a crashed process."""

        recovered = 0
        now = now_utc().timestamp()
        seen: set[str] = set()
        for raw_id in self.client.lrange(self.processing_key, 0, -1):
            task_id = str(_decode_redis_text(raw_id))
            if task_id in seen:
                continue
            seen.add(task_id)
            raw_task = self.client.hget(self.data_key, task_id)
            if not raw_task:
                self.client.lrem(self.processing_key, 1, task_id)
                self.client.hdel(self.processing_meta_key, task_id)
                continue
            task = _decode_task(raw_task)
            metadata = self.client.hget(self.processing_meta_key, task_id)
            claimed_at = 0.0
            if metadata:
                decoded_metadata = json.loads(_decode_redis_text(metadata))
                claimed_at = float(decoded_metadata.get("claimed_at", 0.0))
                self._orphan_seen_at.pop(task_id, None)
            else:
                first_seen = self._orphan_seen_at.setdefault(task_id, now)
                if now - first_seen < self.orphan_grace_seconds:
                    continue
            stale = (
                now - claimed_at >= self.visibility_timeout
                if claimed_at
                else (task.status != "processing" or now - task.updated_at.timestamp() >= self.visibility_timeout)
            )
            if not stale:
                continue
            self.client.lrem(self.processing_key, 1, task_id)
            self.client.hdel(self.processing_meta_key, task_id)
            if task.status == "processing":
                task.attempt += 1
                task.status = "dead" if task.attempt >= self.max_attempts else "pending"
                task.available_at = now_utc()
                task.updated_at = now_utc()
                self.client.hset(self.data_key, task_id, json.dumps(_task_dict(task)))
                if task.status == "pending":
                    self.client.rpush(self.queue_key, task_id)
                else:
                    self.client.rpush(
                        self.dead_letter_key,
                        json.dumps(
                            {"task_id": task_id, "error": "visibility timeout"},
                            ensure_ascii=False,
                        ),
                    )
                recovered += 1
            self._orphan_seen_at.pop(task_id, None)
        return recovered

    def _move_to_processing(self):
        """Atomically move a task out of the pending queue when possible."""

        try:
            return self.client.lmove(
                self.queue_key,
                self.processing_key,
                "LEFT",
                "RIGHT",
            )
        except (AttributeError, TypeError):
            # Redis < 6.2 fallback. It is still recoverable through the
            # processing list and visibility timeout.
            return self.client.rpoplpush(self.queue_key, self.processing_key)
