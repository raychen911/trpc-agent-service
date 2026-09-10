"""Boundary coverage for durable compensation queues."""

from __future__ import annotations

import json
from datetime import timedelta
from types import SimpleNamespace

import pytest
from test_backend_adapters import FakeRedis

from trpc_service.storage.base import CompensationTask, now_utc
from trpc_service.storage.compensation import (
    InMemoryCompensationStore,
    RedisCompensationStore,
    apply_compensation_task,
    replay_compensations,
)


def test_inmemory_compensation_edges_are_tenant_scoped_and_redacted():
    store = InMemoryCompensationStore(max_attempts=2)
    generated = store.enqueue("tenant-a", "memory.put", {})
    assert generated.task_id
    duplicate = store.enqueue("tenant-a", "memory.put", {"ignored": True}, generated.task_id)
    assert duplicate.payload == generated.payload

    future = store.enqueue("tenant-a", "memory.put", {}, "future")
    store._tasks[future.task_id].available_at = now_utc() + timedelta(hours=1)
    processing = store.enqueue("tenant-a", "memory.put", {}, "processing")
    store._tasks[processing.task_id].status = "processing"
    assert store.claim(10, tenant_id="other") == []
    claimed = store.claim(10, tenant_id="tenant-a")
    assert [item.task_id for item in claimed] == [generated.task_id]

    store.complete(generated.task_id, tenant_id="other")
    assert store._tasks[generated.task_id].status == "processing"
    store.fail(generated.task_id, "api_key=secret", retry_after_seconds=0, tenant_id="other")
    assert store._tasks[generated.task_id].status == "processing"
    store.fail(generated.task_id, "api_key=secret", retry_after_seconds=0, tenant_id="tenant-a")
    assert store._tasks[generated.task_id].status == "pending"
    assert "api_key=secret" not in store._tasks[generated.task_id].last_error

    with pytest.raises(KeyError):
        store.replay("missing")
    with pytest.raises(ValueError, match="only dead"):
        store.replay(generated.task_id)
    store._tasks[generated.task_id].attempt = 2
    store._tasks[generated.task_id].status = "processing"
    store.fail(generated.task_id, "permanent", retry_after_seconds=0)
    assert store._tasks[generated.task_id].status == "dead"
    with pytest.raises(KeyError):
        store.replay(generated.task_id, tenant_id="other")
    assert store.replay(generated.task_id, tenant_id="tenant-a").attempt == 0


def test_apply_compensation_task_dispatches_mirror_and_rejects_missing_handlers():
    task = CompensationTask("task", "tenant", "mirror.memory.put", {"x": 1})
    calls = []
    apply_compensation_task(SimpleNamespace(replay_mirror_task=lambda value: calls.append(value)), task)
    assert calls == [task]
    with pytest.raises(RuntimeError, match="mirrored storage"):
        apply_compensation_task(SimpleNamespace(), task)
    with pytest.raises(ValueError, match="unsupported"):
        apply_compensation_task(SimpleNamespace(), CompensationTask("task", "tenant", "unknown", {}))


def test_replay_compensations_counts_success_and_handles_task_failure(monkeypatch):
    good = CompensationTask(
        "good", "tenant", "memory.put",
        {"tenant_id": "tenant", "memory_id": "memory", "scope_key": "scope", "content": "content"},
    )
    bad = CompensationTask("bad", "tenant", "unsupported", {})
    good.attempt = 1
    bad.attempt = 1

    class Queue:
        def claim(self, limit, tenant_id=None):
            del limit, tenant_id
            return [good, bad]

        def complete(self, task_id, tenant_id=None):
            assert task_id == "good"
            assert tenant_id == "tenant"

        def fail(self, task_id, error, retry_after_seconds, tenant_id=None):
            assert task_id == "bad"
            assert "ValueError" in error
            assert retry_after_seconds >= 0
            assert tenant_id == "tenant"

    storage = SimpleNamespace(
        compensation=Queue(),
        memory=SimpleNamespace(put=lambda value: None),
    )
    monkeypatch.setenv("COMPENSATION_RETRY_BASE_SECONDS", "1")
    monkeypatch.setenv("COMPENSATION_RETRY_MAX_SECONDS", "1")
    assert replay_compensations(storage, limit=2, tenant_id="tenant") == 1


def test_redis_compensation_handles_missing_rows_statuses_and_legacy_move(monkeypatch):
    client = FakeRedis()
    store = RedisCompensationStore(client, prefix="coverage", visibility_timeout=0, max_attempts=2)

    client.rpush(store.queue_key, "missing")
    assert store.claim(1) == []
    task = store.enqueue("tenant-a", "memory.put", {}, "task")
    raw = json.loads(client.hget(store.data_key, "task"))
    raw["status"] = "completed"
    client.hset(store.data_key, "task", json.dumps(raw))
    assert store.claim(1) == []

    future = store.enqueue("tenant-a", "memory.put", {}, "future")
    future_data = json.loads(client.hget(store.data_key, future.task_id))
    future_data["available_at"] = (now_utc() + timedelta(hours=1)).isoformat()
    client.hset(store.data_key, future.task_id, json.dumps(future_data))
    assert store.claim(1, tenant_id="tenant-a") == []
    store.complete("missing")
    store.fail("missing", "error")
    store.complete("task", tenant_id="other")
    store.fail("task", "error", tenant_id="other")

    retry = store.enqueue("tenant-a", "memory.put", {}, "retry")
    claimed = store.claim(1)[0]
    assert claimed.task_id == "retry"
    store.fail(retry.task_id, "token=secret", retry_after_seconds=0, tenant_id="tenant-a")
    claimed = store.claim(1)[0]
    assert claimed.task_id == "retry"
    store.fail(claimed.task_id, "permanent", retry_after_seconds=0)
    assert store.claim(1) == []
    with pytest.raises(ValueError, match="only dead"):
        store.replay("task")
    with pytest.raises(KeyError):
        store.replay("missing")

    class LegacyClient(FakeRedis):
        def lmove(self, *_args):
            raise AttributeError("lmove unavailable")

    legacy_client = LegacyClient()
    legacy = RedisCompensationStore(legacy_client, prefix="legacy")
    legacy.enqueue("tenant", "memory.put", {}, "legacy-task")
    assert legacy.claim(1)[0].task_id == "legacy-task"
    monkeypatch.setattr(legacy, "requeue_stale", lambda: 0)
    assert legacy._move_to_processing() is None


def test_redis_requeue_stale_covers_missing_orphan_active_and_dead_paths(monkeypatch):
    client = FakeRedis()
    store = RedisCompensationStore(client, prefix="stale", visibility_timeout=1, max_attempts=1)
    monkeypatch.setattr(store, "orphan_grace_seconds", 0)

    client.rpush(store.processing_key, "missing")
    assert store.requeue_stale() == 0

    active = store.enqueue("tenant", "memory.put", {}, "active")
    active_data = json.loads(client.hget(store.data_key, active.task_id))
    active_data["status"] = "processing"
    active_data["updated_at"] = (now_utc() - timedelta(seconds=10)).isoformat()
    client.hset(store.data_key, active.task_id, json.dumps(active_data))
    client.rpush(store.processing_key, active.task_id)
    client.hset(store.processing_meta_key, active.task_id, json.dumps({"claimed_at": now_utc().timestamp()}))
    assert store.requeue_stale() == 0

    orphan = store.enqueue("tenant", "memory.put", {}, "orphan")
    orphan_data = json.loads(client.hget(store.data_key, orphan.task_id))
    orphan_data["status"] = "pending"
    orphan_data["updated_at"] = (now_utc() - timedelta(seconds=10)).isoformat()
    client.hset(store.data_key, orphan.task_id, json.dumps(orphan_data))
    client.rpush(store.processing_key, orphan.task_id)
    assert store.requeue_stale() == 0
    assert orphan.task_id not in client.lists[store.processing_key]

    dead = store.enqueue("tenant", "memory.put", {}, "dead")
    dead_data = json.loads(client.hget(store.data_key, dead.task_id))
    dead_data["status"] = "processing"
    dead_data["attempt"] = 0
    dead_data["updated_at"] = (now_utc() - timedelta(seconds=10)).isoformat()
    client.hset(store.data_key, dead.task_id, json.dumps(dead_data))
    client.rpush(store.processing_key, dead.task_id)
    assert store.requeue_stale() == 1
    assert client.lists[store.dead_letter_key]
