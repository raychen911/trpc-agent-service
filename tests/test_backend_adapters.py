"""Offline contract tests for Redis, queue, and object/vector adapters.

These tests model the Redis/S3/Qdrant protocols locally.  They validate the
adapter's serialization and recovery behavior without claiming that an actual
external service was available during the test run.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from threading import RLock
from types import SimpleNamespace

import pytest

from trpc_service.gateway.redis_streams import RedisStreamsTransport
from trpc_service.gateway.worker_queue import DurableWebhookQueue, WorkerQueue
from trpc_service.policy.quota import QuotaEnforcer, QuotaExceeded
from trpc_service.security.secrets import SecretManager, SecretResolutionError, redact_secret_data, redact_secret_text
from trpc_service.storage.base import AuditRecord, MemoryItem, SessionEvent, Summary, now_utc
from trpc_service.storage.compensation import RedisCompensationStore
from trpc_service.storage.durable import (
    InboxRecord,
    InboxStatus,
    OutboxRecord,
    OutboxStatus,
    PostgresInboxOutbox,
    SQLiteInboxOutbox,
)
from trpc_service.storage.external_memory import ExternalMemoryStore
from trpc_service.storage.factory import StorageBundle
from trpc_service.storage.in_memory import InMemoryStorage
from trpc_service.storage.locking import (
    RedisSessionLockMixin,
    SessionLease,
    SessionLeaseLost,
    renew_session_lease,
    session_lease,
    session_lock,
    validate_session_lease,
)
from trpc_service.storage.mirror import (
    MirroredStorageBundle,
    _MirrorArtifact,
    _MirrorAudit,
    _MirrorIdempotency,
    _MirrorInboxOutbox,
    _MirrorKnowledge,
    _MirrorMemory,
    _MirrorSession,
    _MirrorSessionMailboxV2,
    _MirrorSummary,
    _MirrorToolGovernance,
)
from trpc_service.storage.object_store import FileObjectStore, RedisObjectStore, S3ObjectStore
from trpc_service.storage.postgres_knowledge import PostgresKnowledgeStore
from trpc_service.storage.postgres_session_mailbox import (
    PostgresSessionMailboxStore,
    _item_export_row,
    _mailbox_export_row,
    _mailbox_from_pg,
)
from trpc_service.storage.postgres_store import PostgresStorage
from trpc_service.storage.redis_knowledge import RedisKnowledgeStore
from trpc_service.storage.redis_store import RedisStorage
from trpc_service.storage.remote_vector import RemoteVectorStore
from trpc_service.storage.session_mailbox import (
    SessionMailboxClaimStatus,
    SessionMailboxLease,
    SQLiteSessionMailboxStore,
)
from trpc_service.storage.tool_governance import SQLiteToolGovernanceStore
from trpc_service.storage.vector_store import KnowledgeChunk
from trpc_service.telemetry import metrics
from trpc_service.telemetry.context import with_traceparent
from trpc_service.tenant.models import QuotaPolicy, TenantContext


class FakePipeline:
    def __init__(self, client):
        self.client = client

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def watch(self, *_):
        return None

    def unwatch(self):
        return None

    def multi(self):
        return None

    def set(self, key, value, ex=None):
        self.client.set(key, value, ex=ex)

    def execute(self):
        return [True]


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.lists = {}
        self.hashes = {}
        self.sets = {}
        self.streams = {}
        self.eval_results = []
        self.closed = False
        self.xacks = []
        self.xdels = []

    def ping(self):
        return True

    def close(self):
        self.closed = True

    def pipeline(self):
        return FakePipeline(self)

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ex=None, nx=False, **_kwargs):
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    def setex(self, key, ttl, value):
        self.values[key] = value
        return True

    def delete(self, key):
        self.values.pop(key, None)

    def exists(self, key):
        return int(key in self.values)

    def incr(self, key):
        value = int(self.values.get(key, 0)) + 1
        self.values[key] = str(value)
        return value

    def expire(self, *_args):
        return True

    def hincrby(self, key, field, value):
        current = int(self.hashes.setdefault(key, {}).get(field, 0)) + int(value)
        self.hashes[key][field] = current
        return current

    def hincrbyfloat(self, key, field, value):
        current = float(self.hashes.setdefault(key, {}).get(field, 0.0)) + float(value)
        self.hashes[key][field] = current
        return current

    def rpush(self, key, *values):
        self.lists.setdefault(key, []).extend(values)
        return len(self.lists[key])

    def lpush(self, key, *values):
        for value in values:
            self.lists.setdefault(key, []).insert(0, value)
        return len(self.lists[key])

    def lrange(self, key, start, end):
        values = self.lists.get(key, [])
        stop = None if end == -1 else end + 1
        return values[start:stop]

    def llen(self, key):
        return len(self.lists.get(key, []))

    def lrem(self, key, count, value):
        values = self.lists.setdefault(key, [])
        removed = 0
        indexes = range(len(values)) if count >= 0 else range(len(values) - 1, -1, -1)
        for index in list(indexes):
            if values[index] == value and (count == 0 or removed < abs(count)):
                values[index] = None
                removed += 1
        self.lists[key] = [value for value in values if value is not None]
        return removed

    def rpoplpush(self, source, destination):
        if not self.lists.get(source):
            return None
        value = self.lists[source].pop()
        self.lists.setdefault(destination, []).insert(0, value)
        return value

    def brpoplpush(self, source, destination, timeout=0):
        return self.rpoplpush(source, destination)

    def lmove(self, source, destination, *_):
        return self.rpoplpush(source, destination)

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(str(field))

    def hset(self, key, field=None, value=None, mapping=None):
        target = self.hashes.setdefault(key, {})
        if mapping is not None:
            target.update({str(k): v for k, v in mapping.items()})
        else:
            target[str(field)] = value
        return 1

    def hdel(self, key, field):
        self.hashes.get(key, {}).pop(str(field), None)

    def hscan_iter(self, key):
        return iter(list(self.hashes.get(key, {}).items()))

    def sadd(self, key, value):
        self.sets.setdefault(key, set()).add(value)

    def smembers(self, key):
        return self.sets.get(key, set())

    def scan_iter(self, pattern):
        prefix = pattern.removesuffix("*")
        keys = set(self.hashes) | set(self.values) | set(self.sets)
        return (key for key in keys if key.startswith(prefix))

    def eval(self, *_args):
        if not self.eval_results:
            return 1
        result = self.eval_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def xgroup_create(self, *_args, **_kwargs):
        return True

    def xadd(self, key, fields):
        message_id = f"{len(self.streams.get(key, [])) + 1}-0"
        self.streams.setdefault(key, []).append((message_id, fields))
        return message_id

    def xreadgroup(self, *_args, **_kwargs):
        return []

    def xack(self, _stream, _group, message_id):
        self.xacks.append(message_id)

    def xdel(self, _stream, message_id):
        self.xdels.append(message_id)

    def xautoclaim(self, *_args, **_kwargs):
        return ["0-0", []]


def redis_storage(client=None):
    storage = RedisStorage.__new__(RedisStorage)
    storage.url = "redis://fake"
    storage.prefix = "test"
    storage.client = client or FakeRedis()
    storage.idempotency_ttl = 3600
    storage.compensation = RedisCompensationStore(storage.client, "test")
    return storage


def test_redis_storage_session_projection_and_idempotency():
    client = FakeRedis()
    storage = redis_storage(client)
    event = SessionEvent("t1", "s1", "e1", "message", {"text": "hello"}, "trace", "dedupe")
    client.eval_results = [1]
    assert storage.append_event(event) == 1
    event_payload = {**event.__dict__} if hasattr(event, "__dict__") else {
        "tenant_id": "t1", "session_id": "s1", "event_id": "e1", "event_type": "message",
        "payload": {"text": "hello"}, "trace_id": "trace", "idempotency_key": "dedupe",
        "seq": 1, "created_at": event.created_at.isoformat(),
    }
    client.lists[storage._key("session-events", "t1", "s1")] = [json.dumps(event_payload)]
    assert storage.load_events("t1", "s1")[0].event_id == "e1"
    assert storage.load_state("t1", "s1").latest_event_seq == 0
    client.values[storage._key("session-state", "t1", "s1")] = json.dumps({"state_version": 1, "state": {"x": 1}})
    client.values[storage._key("session-seq", "t1", "s1")] = "1"
    assert storage.load_state("t1", "s1").state == {"x": 1}
    client.eval_results = [1, 0]
    assert storage.compare_and_set_state("t1", "s1", 1, {"x": 2})
    assert not storage.compare_and_set_state("t1", "s1", 1, {"x": 3})
    storage.restore_state("t1", "s1", {"restored": True}, 4)
    item = MemoryItem("t1", "m1", "s1", "hello world")
    storage.put(item)
    assert storage.search("t1", "hello")[0].memory_id == "m1"
    summary = Summary("t1", "s1", "summary", 1)
    client.eval_results = [2]
    storage.put(summary)
    client.values[storage._key("summary", "t1", "s1")] = json.dumps({**asdict(summary), "created_at": summary.created_at.isoformat()})
    assert storage.latest("t1", "s1").summary_version == 2
    record = AuditRecord("a1", "t1", "allow", "trace")
    storage.append(record)
    client.lists[storage._key("audit", "t1")] = [json.dumps(asdict(record), default=str)]
    assert storage.list_by_tenant("t1")[0].audit_id == "a1"
    started = storage.start("t1", "request-1", "trace")
    assert started.status.value == "processing"
    assert storage.complete("t1", "request-1", "response", {"ok": True}).response_ref == "response"
    assert storage.claim_delivery("t1", "request-1")
    storage.release_delivery("t1", "request-1")
    assert storage.fail("t1", "request-1", "provider").status.value == "failed"
    storage.close()
    assert client.closed


def test_redis_compensation_retries_dead_letters_and_tenant_filtering(monkeypatch):
    client = FakeRedis()
    store = RedisCompensationStore(client, prefix="test", visibility_timeout=0, max_attempts=1)
    task = store.enqueue("tenant-a", "memory.put", {"tenant_id": "tenant-a", "memory_id": "m", "scope_key": "s", "content": "x"}, "task-1")
    assert store.enqueue("tenant-a", "memory.put", {}, "task-1").payload == task.payload
    assert store.claim(10, tenant_id="other") == []
    claimed = store.claim(10, tenant_id="tenant-a")
    assert claimed[0].attempt == 1
    store.fail("task-1", "token=secret", retry_after_seconds=0, tenant_id="tenant-a")
    assert json.loads(client.lists[store.dead_letter_key][0])["task_id"] == "task-1"
    assert store.replay("task-1", tenant_id="tenant-a").status == "pending"
    with pytest.raises(KeyError):
        store.replay("task-1", tenant_id="other")
    task = store.claim(1)[0]
    client.lists[store.processing_key].append(task.task_id)
    client.hashes[store.processing_meta_key].pop(task.task_id, None)
    monkeypatch.setattr(store, "orphan_grace_seconds", 0)
    assert store.requeue_stale() == 1
    assert client.lists[store.dead_letter_key]


def test_redis_stream_transport_success_retry_dead_letter_and_reclaim():
    client = FakeRedis()
    transport = RedisStreamsTransport.__new__(RedisStreamsTransport)
    transport.client = client
    transport.stream_key = "test:stream"
    transport.dead_letter_key = "test:dead"
    transport.group = "workers"
    transport.consumer = "consumer-1"
    transport.max_attempts = 2
    transport.reclaim_interval_seconds = 0.1
    transport._last_reclaim_at = 0
    seen = []
    transport._process("1-0", {"payload": json.dumps({"id": 1})}, lambda item: seen.append(item))
    assert seen == [{"id": 1}]
    transport._process("2-0", {"payload": "not-json"}, lambda _: None)
    assert client.xacks[-1] == "2-0"
    transport._process("3-0", {"payload": json.dumps({"attempt": 0})}, lambda _: (_ for _ in ()).throw(RuntimeError("token=bad")))
    assert client.streams[transport.stream_key]
    transport._process("4-0", {"payload": json.dumps({"attempt": 1})}, lambda _: (_ for _ in ()).throw(RuntimeError("permanent")))
    assert client.streams[transport.dead_letter_key]
    transport.client.xautoclaim = lambda *a, **k: ["0-0", [("5-0", {"payload": json.dumps({"id": 5})})]]
    assert transport.requeue_stale(lambda item: seen.append(item)) == 1
    transport.close()
    assert client.closed


def queue_instance(cls, client):
    queue = cls.__new__(cls)
    queue.client = client
    queue.prefix = "test"
    queue.queue_key = "test:queue"
    queue.processing_key = "test:processing"
    queue.processing_meta_key = "test:meta"
    queue.dead_letter_key = "test:dead"
    queue.status_prefix = "test:status:"
    queue.max_attempts = 2
    queue.visibility_timeout = 0
    queue.orphan_grace_seconds = 0
    queue._orphan_seen_at = {}
    queue.streams = None
    return queue


def test_worker_and_webhook_queue_claim_fallback_recovery_and_status():
    client = FakeRedis()
    worker = queue_instance(WorkerQueue, client)
    webhook = queue_instance(DurableWebhookQueue, client)
    worker._claim = lambda timeout=5: None
    assert worker.requeue_stale() == 0
    webhook._claim = lambda timeout=5: None
    assert webhook.consume_once(lambda _: None) is False
    task_id = webhook.submit("web", "account", {"secret": "token=hidden", "text": "hi"}, "trace", "tenant-a")
    assert webhook.status(task_id)["status"] == "accepted"
    webhook._claim = lambda timeout=5: client.lists[webhook.queue_key].pop()
    assert webhook.consume_once(lambda item: {"ok": True, "answer": "done", "task_id": item["task_id"]})
    assert webhook.status(task_id)["status"] == "completed"
    client.lists[webhook.queue_key].append("bad")
    assert webhook.consume_once(lambda _: None)
    assert client.lists[webhook.dead_letter_key]
    webhook.close()
    worker.close()


def test_queue_recovery_dead_letter_paths():
    client = FakeRedis()
    queue = queue_instance(DurableWebhookQueue, client)
    raw = json.dumps({"task_id": "stale", "attempt": 0, "tenant_id": "t"})
    client.lists[queue.processing_key] = [raw]
    client.hashes[queue.processing_meta_key] = {"stale": json.dumps({"raw": raw, "claimed_at": 0})}
    assert queue.requeue_stale() == 1
    assert json.loads(client.lists[queue.queue_key][0])["attempt"] == 1
    client.lists[queue.processing_key] = [json.dumps({"task_id": "dead", "attempt": 1})]
    client.hashes[queue.processing_meta_key] = {}
    assert queue.requeue_stale() == 1
    assert client.lists[queue.dead_letter_key]


def test_sqlite_tool_governance_and_object_store_protocols(tmp_path, monkeypatch):
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    governance = SQLiteToolGovernanceStore(connection, __import__("threading").RLock())
    approval = governance.create_or_get("t", "a", "s", "r", "send", "hash", expires_seconds=60)
    assert governance.approve("t", "a", "operator").approved_by == "operator"
    assert governance.consume("t", "a", "r", "hash").status == "consumed"
    execution = governance.begin_execution("t", "e", "r", "s", "send", "call", "hash", True, 3)
    assert governance.complete_execution("t", "call", {"ok": True}, 3).status == "succeeded"
    assert governance.list_executions_by_tenant("t")
    assert governance.restore_execution(execution)
    assert governance.reserve_call("t", "r", "call-2", False, 3, 1).total_calls == 1

    client = FakeRedis()
    obj = RedisObjectStore.__new__(RedisObjectStore)
    obj.client, obj.prefix = client, "test"
    stored = obj.put_with_id("t", "o", b"data", "text/plain")
    assert obj.get("t", "o") == b"data"
    assert obj.list_by_tenant("t")[0].content_type == "text/plain"
    with pytest.raises(FileNotFoundError):
        obj.get("t", "missing")

    class S3:
        def put_object(self, **kwargs):
            self.kwargs = kwargs

        def get_object(self, **kwargs):
            return {"Body": SimpleNamespace(read=lambda: b"s3-data")}

        def list_objects_v2(self, **kwargs):
            return {"Contents": [{"Key": "test/t/o", "Size": 8}]}

    s3 = S3()
    s3_store = S3ObjectStore.__new__(S3ObjectStore)
    s3_store.bucket, s3_store.prefix, s3_store.client = "bucket", "test", s3
    assert s3_store.put_with_id("t", "o", b"s3-data", "text/plain").size == 7
    assert s3_store.get("t", "o") == b"s3-data"
    assert s3_store.list_by_tenant("t")[0].object_id == "o"


def test_remote_vector_qdrant_and_generic_provider(monkeypatch):
    store = RemoteVectorStore("https://vector.example.com", dimension=4)
    assert len(store._embedding("hello world")) == 4
    chunk = KnowledgeChunk("t", "docs", "c1", "hello", {"source": "test"})
    calls = []

    def request(method, path, payload=None):
        calls.append((method, path, payload))
        if path == "/collections":
            return {"result": {"collections": [{"name": "trpc-agent_docs"}]}}
        if path.endswith("/search"):
            return {"result": [{"id": 1, "payload": {"tenant_id": "t", "collection": "docs", "chunk_id": "c1", "text": "hello", "metadata": {}}}]}
        if path.endswith("/scroll"):
            return {"result": {"points": [], "next_page_offset": None}}
        return {}

    monkeypatch.setattr(store, "_request", request)
    store.upsert(chunk)
    assert store.search("t", "docs", "hello")
    assert store.list_by_tenant("t") == []
    generic = RemoteVectorStore("https://vector.example.com", provider="custom")
    monkeypatch.setattr(generic, "_request", lambda method, path, payload=None: {"items": [{
        "tenant_id": "t", "collection": "docs", "chunk_id": "c1", "text": "hello", "metadata": {"source": "test"}
    }]})
    generic.upsert(chunk)
    assert generic.search("t", "docs", "hello")[0].chunk_id == "c1"
    assert generic.list_by_tenant("t")[0].tenant_id == "t"


class SpyProjection:
    def __init__(self, **returns):
        self.returns = returns
        self.calls = []
        self.errors = set()

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.calls.append((name, args, kwargs))
            if name in self.errors:
                raise RuntimeError(f"{name} failed")
            value = self.returns.get(name)
            return value() if callable(value) else value

        return call


def test_mirror_projection_reads_fallback_and_write_compensation():
    failures = []
    enqueue = lambda operation, payload: failures.append((operation, payload))
    item = MemoryItem("t", "m", "s", "text")
    summary = Summary("t", "s", "summary", 1)
    audit = AuditRecord("a", "t", "allow", "trace")
    chunk = KnowledgeChunk("t", "docs", "c", "text")
    stored = SimpleNamespace(object_id="o", tenant_id="t")
    primary = SpyProjection(search=[], latest=None, list_by_tenant=[], get=None)
    secondary = SpyProjection(search=[item], latest=summary, list_by_tenant=[audit], get=stored)
    memory = _MirrorMemory(primary, secondary, enqueue)
    assert memory.search("t", "text") == [item]
    memory.put(item)
    secondary.errors.add("put")
    with pytest.raises(RuntimeError):
        memory.put(item)
    assert failures[-1][0] == "mirror.memory.put"
    summary_mirror = _MirrorSummary(primary, secondary, enqueue)
    assert summary_mirror.latest("t", "s") == summary
    audit_mirror = _MirrorAudit(primary, secondary, enqueue)
    assert audit_mirror.list_by_tenant("t") == [audit]
    artifact = _MirrorArtifact(primary, secondary, enqueue)
    primary.errors.clear()
    primary.put = lambda *args: stored
    assert artifact.put("t", b"x").object_id == "o"
    primary.errors.add("get")
    assert artifact.get("t", "o") == stored
    knowledge = _MirrorKnowledge(primary, secondary, enqueue)
    primary.errors.clear()
    primary.upsert = lambda *_: None
    secondary.upsert = lambda *_: None
    knowledge.upsert(chunk)
    primary.search = lambda *_: []
    assert knowledge.search("t", "docs", "text") == [item]
    assert knowledge.list_by_tenant("t") == [audit]


def test_mirror_session_locks_idempotency_and_governance_error_paths():
    failures = []
    enqueue = lambda operation, payload: failures.append((operation, payload))
    lease = SimpleNamespace(owner="worker", fencing_token=1)
    primary = SpyProjection(
        acquire_session_lock="lock", acquire_session_lease=lease, load_events=[],
        load_state=SimpleNamespace(state={}, state_version=0, latest_event_seq=0),
        compare_and_set_state=True, get=None, claim_delivery=True,
    )
    secondary = SpyProjection(
        acquire_session_lock="lock", acquire_session_lease=lease, load_events=["event"],
        load_state=SimpleNamespace(state={"x": 1}, state_version=1, latest_event_seq=1),
        compare_and_set_state=True, get=SimpleNamespace(), claim_delivery=True,
    )
    session = _MirrorSession(primary, secondary, enqueue)
    assert session.acquire_session_lock("t", "s", 1) == "lock"
    session.release_session_lock("t", "s", "lock")
    assert session.acquire_session_lease("t", "s", 1) == lease
    session.release_session_lease("t", "s", lease)
    assert session.renew_session_lease(lease) is None
    session.validate_session_lease(lease)
    assert session.load_events("t", "s") == ["event"]
    assert session.load_state("t", "s").state == {"x": 1}
    session.compare_and_set_state("t", "s", 0, {"x": 1})
    missing = _MirrorSession(SimpleNamespace(), SimpleNamespace(), enqueue)
    with pytest.raises(AttributeError):
        missing.acquire_session_lock("t", "s", 1)
    idem = _MirrorIdempotency(primary, secondary, enqueue)
    idem.start("t", "k", "trace")
    idem.complete("t", "k", "r", {"ok": True})
    idem.fail("t", "k", "err")
    assert idem.get("t", "k") is not None
    assert idem.claim_delivery("t", "k")
    idem.release_delivery("t", "k")
    governance = _MirrorToolGovernance(primary, secondary, enqueue)
    governance.create_or_get("t", "a")
    governance.approve("t", "a")
    governance.consume("t", "a")
    governance.reserve_call("t", "r", "c", False, 2, 1)
    governance.begin_execution("t", "e")
    governance.complete_execution("t", "c", {})
    governance.fail_execution("t", "c", "err", "message")
    governance.restore_execution(SimpleNamespace())
    assert failures == []
    secondary.errors.add("approve")
    with pytest.raises(RuntimeError):
        governance.approve("t", "a")
    assert failures[-1][0] == "mirror.tool_governance.approve"


def test_mirror_mailbox_v2_and_inbox_outbox_lifecycle():
    failures = []
    enqueue = lambda operation, payload: failures.append((operation, payload))
    first = InMemoryStorage()
    second = InMemoryStorage()
    v2 = _MirrorSessionMailboxV2(first.session_mailbox_v2, second.session_mailbox_v2, enqueue)
    accepted = v2.accept("t", "s", "m1")
    assert accepted.accepted_sequence == 1
    assert v2.get("t", "s") is not None
    lease = v2.claim("t", "s", "worker", 30)
    assert lease is not None
    v2.renew(lease, 30)
    v2.commit(lease)
    assert v2.recover("t", "s") is None
    assert v2.reconcile("t", "s") is None
    assert v2.sweep_expired_leases() == 0
    assert v2.schedule_retries() == 0
    assert v2.reconcile_sessions() == 0
    exported = v2.export_by_tenant("t")
    v2.restore_export(exported)
    assert v2.has_unresolved_message("t", "s", "m1") is False
    inbox_first = first.inbox_outbox
    inbox_second = second.inbox_outbox
    mirror = _MirrorInboxOutbox(inbox_first, inbox_second, enqueue)
    accepted, created = mirror.accept_inbox("t", "inbox", "s", {"x": 1}, "worker")
    assert created and accepted.tenant_id == "t"
    mirror.complete_inbox("t", "inbox", "worker", {"ok": True})
    outbox = mirror.enqueue_outbox("t", "topic", "s", {"x": 1}, event_id="event-1")
    assert outbox.event_id
    claimed = mirror.claim_outbox("worker")
    if claimed:
        mirror.complete_outbox(claimed[0].event_id, "worker", tenant_id="t")
    assert inbox_first.list_inbox_by_tenant("t")[0].status == "completed"


def test_mirror_compensation_replay_dispatches_every_projection():
    primary = InMemoryStorage()
    secondary = InMemoryStorage()
    bundle = MirroredStorageBundle(StorageBundle(primary), StorageBundle(secondary))
    event = SessionEvent("t", "s", "e", "message", {"text": "x"}, "trace")
    memory = MemoryItem("t", "m-replay", "s", "memory")
    summary = Summary("t", "s", "summary", 1)
    audit = AuditRecord("a-replay", "t", "allow", "trace")
    chunk = KnowledgeChunk("t", "docs", "c-replay", "knowledge")
    cases = [
        ("mirror.session.append_event", {"event": asdict(event) | {"created_at": event.created_at.isoformat()}}),
        ("mirror.session.compare_and_set", {"tenant_id": "t", "session_id": "s", "expected_version": 0, "state": {"v": 1}}),
        ("mirror.memory.put", {"item": asdict(memory) | {"created_at": memory.created_at.isoformat()}}),
        ("mirror.summary.put", {"summary": asdict(summary) | {"created_at": summary.created_at.isoformat()}}),
        ("mirror.audit.append", {"record": asdict(audit) | {"created_at": audit.created_at.isoformat()}}),
        ("mirror.idempotency.start", {"tenant_id": "t", "key": "r", "trace_id": "trace"}),
        ("mirror.idempotency.complete", {"tenant_id": "t", "key": "r", "response_ref": "out", "result": {"ok": True}}),
        ("mirror.idempotency.fail", {"tenant_id": "t", "key": "missing", "error_type": "x"}),
        ("mirror.idempotency.claim_delivery", {"tenant_id": "t", "key": "r"}),
        ("mirror.idempotency.release_delivery", {"tenant_id": "t", "key": "r"}),
        ("mirror.artifact.put", {"tenant_id": "t", "object_id": "o-replay", "content_base64": "eA==", "content_type": "text/plain"}),
        ("mirror.knowledge.upsert", {"chunk": asdict(chunk)}),
    ]
    for operation, payload in cases:
        if operation == "mirror.idempotency.complete":
            primary.idempotency.start("t", "r", "trace")
        if operation == "mirror.idempotency.claim_delivery":
            secondary.idempotency.start("t", "r", "trace")
            secondary.idempotency.complete("t", "r", "out", {"ok": True})
        if operation == "mirror.idempotency.fail":
            primary.idempotency.start("t", "missing", "trace")
            secondary.idempotency.start("t", "missing", "trace")
        bundle.replay_mirror_task(SimpleNamespace(operation=operation, payload=payload))
    for operation in ("renew", "commit", "retry"):
        session_id = f"s-{operation}"
        secondary.session_mailbox_v2.accept("t", session_id, f"m-{operation}")
        lease = secondary.session_mailbox_v2.claim("t", session_id, "worker", 30)
        raw_lease = asdict(lease)
        raw_lease["expires_at"] = lease.expires_at.isoformat()
        v2_kwargs = {"lease": raw_lease}
        if operation == "renew":
            v2_kwargs["lease_seconds"] = 30
        elif operation == "retry":
            v2_kwargs["increment_retry"] = False
        v2_payload = {"kwargs": v2_kwargs}
        bundle.replay_mirror_task(SimpleNamespace(operation=f"mirror.session_mailbox_v2.{operation}", payload=v2_payload))
    bundle.replay_mirror_task(SimpleNamespace(
        operation="mirror.session_mailbox_v2.restore_export", payload={"kwargs": {"payload": {}}}
    ))
    bundle.replay_mirror_task(SimpleNamespace(
        operation="mirror.inbox_outbox.list_inbox_by_tenant", payload={"args": ["t"], "kwargs": {}}
    ))
    execution = primary.tool_governance.begin_execution("t", "e", "r", "s", "tool", "call", "hash", False)
    raw_execution = asdict(execution)
    for key in ("started_at", "completed_at", "created_at", "updated_at"):
        if raw_execution.get(key) is not None:
            raw_execution[key] = raw_execution[key].isoformat()
    bundle.replay_mirror_task(SimpleNamespace(
        operation="mirror.tool_governance.restore_execution", payload={"args": [raw_execution], "kwargs": {}}
    ))
    with pytest.raises(ValueError):
        bundle.replay_mirror_task(SimpleNamespace(operation="mirror.unknown", payload={}))
    bundle.close()


def test_mirror_failure_fallbacks_and_consistency_guards():
    failures = []
    enqueue = lambda operation, payload: failures.append((operation, payload))
    event = SessionEvent("t", "s", "e", "message", {"x": 1}, "trace")
    state_empty = SimpleNamespace(state={}, state_version=0, latest_event_seq=0)
    state_current = SimpleNamespace(state={"ok": True}, state_version=1, latest_event_seq=1)
    primary = SpyProjection(
        append_event=1,
        load_events=[],
        load_state=state_empty,
        compare_and_set_state=False,
        search=[], latest=None, list_by_tenant=[], get=None,
    )
    secondary = SpyProjection(
        append_event=1,
        load_events=[event],
        load_state=state_current,
        compare_and_set_state=False,
        search=[event], latest=state_current, list_by_tenant=[event], get=SimpleNamespace(),
    )
    session = _MirrorSession(primary, secondary, enqueue)
    assert session.append_event(event) == 1
    assert session.load_events("t", "s") == [event]
    assert session.load_state("t", "s") is state_current
    assert session.compare_and_set_state("t", "s", 0, {"ok": True}) is False

    primary.errors.add("load_events")
    assert session.load_events("t", "s") == [event]
    primary.errors.add("load_state")
    assert session.load_state("t", "s") is state_current
    primary.errors.clear()
    primary.append_event = lambda *_args, **_kwargs: 1
    secondary.append_event = lambda *_args, **_kwargs: 2
    with pytest.raises(RuntimeError, match="sequence mismatch"):
        session.append_event(event)
    secondary.append_event = lambda *_args, **_kwargs: 1
    primary.compare_and_set_state = lambda *_args, **_kwargs: True
    secondary.compare_and_set_state = lambda *_args, **_kwargs: False
    secondary.load_state = lambda *_args, **_kwargs: SimpleNamespace(
        state={"different": True}, state_version=1, latest_event_seq=1
    )
    with pytest.raises(RuntimeError, match="state mismatch"):
        session.compare_and_set_state("t", "s", 0, {"ok": True})

    memory = _MirrorMemory(primary, secondary, enqueue)
    primary.search = lambda *_: (_ for _ in ()).throw(RuntimeError("read failure"))
    assert memory.search("t", "q") == [event]
    summary = _MirrorSummary(primary, secondary, enqueue)
    secondary.errors.add("put")
    with pytest.raises(RuntimeError):
        summary.put(Summary("t", "s", "x", 1))
    primary.latest = lambda *_: (_ for _ in ()).throw(RuntimeError("read failure"))
    assert summary.latest("t", "s") is state_current
    audit = _MirrorAudit(primary, secondary, enqueue)
    secondary.errors.add("append")
    with pytest.raises(RuntimeError):
        audit.append(AuditRecord("a", "t", "allow", "trace"))
    primary.list_by_tenant = lambda *_: (_ for _ in ()).throw(RuntimeError("read failure"))
    assert audit.list_by_tenant("t") == [event]

    idem = _MirrorIdempotency(primary, secondary, enqueue)
    primary.get = lambda *_: (_ for _ in ()).throw(RuntimeError("read failure"))
    assert idem.get("t", "key") is not None
    primary.claim_delivery = lambda *_args, **_kwargs: False
    assert idem.claim_delivery("t", "key") is False
    secondary.errors.add("release_delivery")
    with pytest.raises(RuntimeError):
        idem.release_delivery("t", "key")

    v2 = _MirrorSessionMailboxV2(primary, secondary, enqueue)
    primary.get = lambda *_: None
    secondary.get = lambda *_: SimpleNamespace()
    assert v2.get("t", "s") is not None
    primary.export_by_tenant = lambda *_: {"items": []}
    assert v2.export_by_tenant("t") == {"items": []}
    primary.has_unresolved_message = lambda *_: True
    secondary.has_unresolved_message = lambda *_: False
    with pytest.raises(RuntimeError, match="message state mismatch"):
        v2.has_unresolved_message("t", "s", "m")
    primary.has_unresolved_message = secondary.has_unresolved_message = lambda *_: False
    assert v2.has_unresolved_message("t", "s", "m") is False
    primary.sweep_expired_leases = lambda **_: 1
    secondary.sweep_expired_leases = lambda **_: 2
    with pytest.raises(RuntimeError, match="sweep_expired_leases mismatch"):
        v2.sweep_expired_leases(limit=1)

    artifact = _MirrorArtifact(primary, secondary, enqueue)
    primary.put_with_id = lambda *_: SimpleNamespace(object_id="o")
    secondary.errors.add("put_with_id")
    with pytest.raises(RuntimeError):
        artifact.put_with_id("t", "o", b"x")
    primary.list_by_tenant = lambda *_args, **_kwargs: []
    secondary.list_by_tenant = lambda *_args, **_kwargs: [SimpleNamespace(object_id="o")]
    assert artifact.list_by_tenant("t")
    knowledge = _MirrorKnowledge(primary, secondary, enqueue)
    primary.search = lambda *_: (_ for _ in ()).throw(RuntimeError("read failure"))
    secondary.search = lambda *_args, **_kwargs: [SimpleNamespace(chunk_id="c")]
    assert knowledge.search("t", "docs", "q")


def test_sqlite_tool_governance_conflicts_expiry_and_budgets():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    store = SQLiteToolGovernanceStore(connection, __import__("threading").RLock())
    store.create_or_get("t", "approval", "s", "r", "send", "hash", expires_seconds=60)
    ambiguous = store.create_or_get("t", "approval", "s", "r", "send", "different", expires_seconds=60)
    assert ambiguous.status == "ambiguous"
    with pytest.raises(RuntimeError):
        store.approve("t", "approval")
    execution = store.begin_execution("t", "e", "r", "s", "tool", "call", "hash", True, fencing_token=2)
    with pytest.raises(RuntimeError, match="stale"):
        store.begin_execution("t", "e2", "r", "s", "tool", "call", "hash", True, fencing_token=3)
    store.fail_execution("t", "call", "ProviderError", "token=secret", fencing_token=2)
    assert store.begin_execution("t", "e2", "r", "s", "tool", "call", "hash", True, fencing_token=2).attempt == 2
    store.reserve_call("t", "budget", "one", True, 1, 1)
    with pytest.raises(RuntimeError, match="call budget"):
        store.reserve_call("t", "budget", "two", False, 1, 1)
    store.reserve_call("t", "side-budget", "one", False, 3, 0)
    with pytest.raises(RuntimeError, match="side-effect"):
        store.reserve_call("t", "side-budget", "two", True, 3, 0)
    expired = store.create_or_get("t", "expired", "s", "r", "tool", "hash", expires_seconds=-1)
    assert store.get("t", "expired").status == "expired"
    assert store.list_by_tenant("t")
    assert store.list_budgets_by_tenant("t")


def test_file_external_redis_knowledge_and_metrics_contracts(tmp_path, monkeypatch):
    files = FileObjectStore(tmp_path / "objects")
    saved = files.put_with_id("tenant-a", "object-a", b"payload", "text/plain")
    assert files.get("tenant-a", "object-a") == b"payload"
    assert files.list_by_tenant("tenant-a")[0].size == 7
    assert files.list_by_tenant("missing") == []
    with pytest.raises(FileNotFoundError):
        files.get("tenant-a", "missing")

    external = ExternalMemoryStore("https://memory.example.com", "secret")
    requests = []
    monkeypatch.setattr(external, "_request", lambda method, path, payload=None: requests.append((method, path, payload)) or {
        "items": [{"tenant_id": "t", "memory_id": "m", "content": "hello", "scope_key": "s"}]
    })
    external.put(MemoryItem("t", "m", "s", "hello"))
    assert external.search("t", "hello", scope_keys=("s",))[0].memory_id == "m"
    assert requests[0][0] == "PUT"

    client = FakeRedis()
    knowledge = RedisKnowledgeStore.__new__(RedisKnowledgeStore)
    knowledge.client, knowledge.prefix = client, "test"
    knowledge.upsert(KnowledgeChunk("t", "docs", "c1", "hello world"))
    knowledge.upsert(KnowledgeChunk("t", "other", "c2", "unrelated"))
    assert knowledge.search("t", "docs", "hello")
    assert knowledge.list_by_tenant("t")[0].chunk_id in {"c1", "c2"}
    knowledge.close()

    context = TenantContext("t", "app", 1, "trace", "session", "web", "user")
    assert with_traceparent(context, "00-trace").traceparent == "00-trace"
    metrics.observe_request("t", "web", "ok")
    metrics.observe_tokens("t", 2)
    metrics.observe_model_latency("t", 0.01)
    metrics.observe_tool("t", "search", "ok")
    metrics.observe_tool_latency("t", "search", 0.01)
    metrics.observe_delivery("web", "ok", "t")
    metrics.observe_cost("t", 0.1)
    metrics.observe_session_backend_latency("t", "get", 0.01)
    metrics.observe_error("t", "web", "error")
    assert isinstance(metrics.generate_latest(), bytes)


def test_secret_resolution_redaction_and_quota_boundaries(monkeypatch):
    monkeypatch.setenv("SECRET_API_KEY", "secret-value")
    assert SecretManager().resolve("secret://api-key") == "secret-value"
    assert "[secret-redacted]" in redact_secret_text("Bearer secret-value token=secret-value")
    assert redact_secret_data({"api_key": "secret-value", "safe": ["token=secret-value"]})["api_key"] == "[secret-redacted]"
    with pytest.raises(SecretResolutionError):
        SecretManager().resolve("not-a-secret")
    with pytest.raises(SecretResolutionError):
        SecretManager({}).resolve("secret://not-configured")

    policy = QuotaPolicy(qps_limit=1, daily_token_limit=10, daily_cost_limit=1.0)
    quota = QuotaEnforcer()
    quota.reserve("t", policy, requested_tokens=2, requested_cost=0.1)
    with pytest.raises(QuotaExceeded, match="QPS"):
        quota.check("t", policy)
    quota.record("t", 2, 0.1, reserved_tokens=2, reserved_cost=0.1)
    quota.release("t", 2, 0.1)


class FakeCursor:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, query, params=()):
        self.calls.append((query, params))

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class FakeConnection:
    closed = False

    def __init__(self, rows=()):
        self.cursor_obj = FakeCursor(rows)

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


def test_postgres_knowledge_adapter_uses_tenant_queries(monkeypatch):
    chunk = KnowledgeChunk("t", "docs", "c", "text", {})
    row = ("t", "docs", "c", "text", {})
    store = PostgresKnowledgeStore.__new__(PostgresKnowledgeStore)
    store._connection = FakeConnection([row])
    store._lock = __import__("threading").RLock()
    store.upsert(chunk)
    assert store.search("t", "docs", "text")[0].chunk_id == "c"
    assert store.list_by_tenant("t")[0].tenant_id == "t"
    store.close()
    assert store._connection.closed


class RedisLockOwner(RedisSessionLockMixin):
    def __init__(self, client):
        self.client = client
        self.prefix = "test"

    def _key(self, kind, tenant_id, session_id):
        return f"test:{kind}:{tenant_id}:{session_id}"


def test_lock_helpers_cover_backend_and_local_fallbacks(monkeypatch):
    client = FakeRedis()
    owner = RedisLockOwner(client)
    token = owner.acquire_session_lock("t", "s", 0)
    assert token
    owner.release_session_lock("t", "s", token)
    client.eval_results = [2]
    lease = owner.acquire_session_lease("t", "s", 0)
    client.values[owner._key("session-lease", "t", "s")] = json.dumps({"owner": lease.owner, "fencing_token": 2})
    client.eval_results = [1]
    renewed = owner.renew_session_lease(lease)
    assert renewed.fencing_token == 2
    owner.validate_session_lease(renewed)
    owner.release_session_lease("t", "s", renewed)
    client.eval_results = [0]
    with pytest.raises(SessionLeaseLost):
        owner.renew_session_lease(renewed)
    client.values.pop(owner._key("session-lease", "t", "s"), None)
    with pytest.raises(SessionLeaseLost):
        owner.validate_session_lease(renewed)

    lease_store = SimpleNamespace(
        acquire_session_lease=lambda tenant, session, timeout: lease,
        release_session_lease=lambda tenant, session, value: None,
        renew_session_lease=lambda value: value,
        validate_session_lease=lambda value: None,
    )
    with session_lease(lease_store, "t", "s") as active:
        assert active is lease
    assert renew_session_lease(lease_store, lease) is lease
    validate_session_lease(lease_store, lease)
    legacy = SimpleNamespace(
        acquire_session_lock=lambda tenant, session, timeout: "legacy",
        release_session_lock=lambda tenant, session, token: None,
    )
    with session_lease(legacy, "t", "s") as active:
        assert active == "legacy"
    with session_lock(legacy, "t", "s") as active:
        assert active == "legacy"
    with session_lease(object(), "t", "s") as active:
        assert isinstance(active, SessionLease)
    with session_lock(object(), "t", "s") as active:
        assert active is None
    assert renew_session_lease(object(), "not-a-lease") == "not-a-lease"
    validate_session_lease(object(), None)


def test_sqlite_session_mailbox_ordering_recovery_and_export():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    SQLiteInboxOutbox(connection, __import__("threading").RLock())
    store = SQLiteSessionMailboxStore(connection, __import__("threading").RLock())
    first = store.accept("t", "s", "m1", priority=2, trace_id="trace-1")
    assert first.accepted_sequence == 1
    assert store.accept("t", "s", "m1").accepted_sequence == 1
    waiting = store.accept("t", "s", "m2", retry_at=now_utc() + __import__("datetime").timedelta(seconds=60))
    assert waiting.accepted_sequence == 2
    assert store.claim_session("t", "s", "worker", 30, expected_generation=999).status == SessionMailboxClaimStatus.STALE
    lease = store.claim("t", "s", "worker", 30)
    assert lease is not None
    store.renew(lease, 30)
    store.commit(lease)
    assert store.claim("t", "s", "worker", 30) is None
    connection.execute(
        "UPDATE session_mailbox_item SET retry_at=? WHERE tenant_id='t' AND session_id='s' AND sequence=2",
        ((now_utc() - __import__("datetime").timedelta(seconds=1)).isoformat(),),
    )
    connection.commit()
    connection.execute(
        "UPDATE session_mailbox SET retry_at=?, status='retry_wait' WHERE tenant_id='t' AND session_id='s'",
        ((now_utc() - __import__("datetime").timedelta(seconds=1)).isoformat(),),
    )
    connection.commit()
    assert store.schedule_retries() == 1
    lease = store.claim("t", "s", "worker", 30)
    assert lease is not None
    store.retry(lease, increment_retry=True)
    lease = store.claim("t", "s", "worker", 30)
    assert lease is not None
    connection.execute(
        "UPDATE session_mailbox SET lease_until=? WHERE tenant_id='t' AND session_id='s'",
        ((now_utc() - __import__("datetime").timedelta(seconds=1)).isoformat(),),
    )
    connection.commit()
    assert store.recover("t", "s") is not None
    exported = store.export_by_tenant("t")
    restored_connection = sqlite3.connect(":memory:")
    restored_connection.row_factory = sqlite3.Row
    SQLiteInboxOutbox(restored_connection, __import__("threading").RLock())
    restored = SQLiteSessionMailboxStore(restored_connection, __import__("threading").RLock())
    restored.restore_export(exported)
    assert restored.get("t", "s").accepted_sequence == 2
    assert store.reconcile("missing", "session") is None
    with pytest.raises(ValueError):
        store.sweep_expired_leases(limit=0)


def test_http_management_health_and_error_contracts(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from trpc_service.web.app import create_app

    monkeypatch.setenv("TENANT_DB_PATH", str(tmp_path / "tenants.sqlite3"))
    monkeypatch.setenv("ADMIN_API_KEY", "test-admin")
    monkeypatch.setenv("REDIS_URL", "")
    monkeypatch.setenv("WORKER_QUEUE_URL", "")
    application = create_app()
    with TestClient(application) as client:
        assert client.get("/livez").json() == {"status": "ok"}
        assert client.get("/health").json()["status"] == "ok"
        assert client.get("/readyz").status_code == 200
        assert client.get("/metrics").status_code == 200
        assert "<!doctype html>" in client.get("/ui").text.lower()
        headers = {"X-Admin-API-Key": "test-admin"}
        tenant_response = client.get("/admin/v1/tenants/tenant_demo", headers=headers)
        assert tenant_response.status_code == 200
        assert tenant_response.headers["etag"] == '"1"'
        assert client.get("/admin/v1/tenants/tenant_demo/health", headers=headers).status_code == 200
        assert client.get("/admin/v1/tenants/tenant_demo/audit?limit=1", headers=headers).json()["count"] == 0
        updated = client.put(
            "/admin/v1/tenants/tenant_demo/config",
            json={},
            headers={**headers, "If-Match": 'W/"1"'},
        )
        assert updated.status_code == 200
        version = updated.json()["config_version"]
        assert client.post(
            "/admin/v1/tenants/tenant_demo/publish",
            json={"version": version},
            headers=headers,
        ).status_code == 200
        assert client.post(
            "/admin/v1/tenants/tenant_demo/rollback",
            json={"version": 1},
            headers=headers,
        ).status_code == 200
        gray = client.post(
            "/admin/v1/tenants/tenant_demo/gray-release",
            json={"enabled": False, "percent": 0},
            headers=headers,
        )
        assert gray.status_code == 200
        added = client.post(
            "/admin/v1/tenants/tenant_demo/channels",
            json={"channel": "web", "account_id": "web-extra", "agent_app_id": "app_support"},
            headers=headers,
        )
        assert added.status_code == 200
        assert client.get("/webhooks/web/web_demo").status_code == 200
        assert client.get("/admin/v1/tenants/tenant_demo/audit?limit=0", headers=headers).status_code == 400
        assert client.post("/admin/v1/tenants/tenant_demo/publish", json={}, headers=headers).status_code == 400
        assert client.post("/admin/v1/tenants/tenant_demo/rollback", json={}, headers=headers).status_code == 400
        assert client.post("/admin/v1/tenants/tenant_demo/gray-release", json={}, headers=headers).status_code == 400
        assert client.post("/admin/v1/tenants/tenant_demo/channels", json={}, headers=headers).status_code == 400
        assert client.post("/admin/v1/tenants/tenant_demo/compensations/replay", json={}, headers=headers).status_code == 200
        assert client.post("/webhooks/unknown/account", content=b"{}", headers={"content-type": "application/json"}).status_code in {400, 404}
        assert client.get("/admin/v1/webhook-tasks/missing", headers=headers).status_code == 503
        assert client.get("/admin/v1/tenants/missing", headers=headers).status_code == 404


class ScriptedPGCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, query, params=()):
        query = " ".join(str(query).split())
        self.connection.executed.append((query, params))
        if self.connection.rowcounts:
            self.rowcount = self.connection.rowcounts.pop(0)

    def fetchone(self):
        return self.connection.one.pop(0) if self.connection.one else None

    def fetchall(self):
        return self.connection.many.pop(0) if self.connection.many else []


class ScriptedPGConnection:
    def __init__(self):
        self.closed = False
        self.autocommit = False
        self.one = []
        self.many = []
        self.rowcounts = []
        self.executed = []

    def cursor(self):
        return ScriptedPGCursor(self)

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def close(self):
        self.closed = True


def pg_storage():
    storage = PostgresStorage.__new__(PostgresStorage)
    storage._connection = ScriptedPGConnection()
    storage._lock = RLock()
    storage._dsn = "postgresql://fake"
    storage._process_role = "runtime"
    return storage


def test_postgres_inbox_outbox_protocol_lifecycle(monkeypatch):
    now = datetime.now(UTC)
    inbox_row = (
        "message-1", "tenant-a", "dedupe-1", "session-a", {"text": "hello"},
        InboxStatus.PROCESSING, 1, "worker-a", now + timedelta(minutes=2), None, now, now,
    )
    completed_inbox = (
        "message-1", "tenant-a", "dedupe-1", "session-a", {"text": "hello"},
        InboxStatus.COMPLETED, 1, None, None, {"ok": True}, now, now,
    )
    outbox_row = (
        "event-1", "tenant-a", "topic", "session-a", {"ok": True},
        OutboxStatus.PENDING, 0, now, None, None, None, now, now,
    )
    processing_outbox = (
        "event-1", "tenant-a", "topic", "session-a", {"ok": True},
        OutboxStatus.PROCESSING, 1, now, "worker-a", now + timedelta(minutes=2), None, now, now,
    )

    connection = ScriptedPGConnection()
    store = PostgresInboxOutbox.__new__(PostgresInboxOutbox)
    store._conn = connection
    store._lock = RLock()

    connection.one = [("message-1",)]
    record, created = store.accept_inbox(
        "tenant-a", "dedupe-1", "session-a", {"text": "hello"}, "worker-a", message_id="message-1"
    )
    assert created and record.message_id == "message-1"

    connection.one = [None, inbox_row]
    existing, created = store.accept_inbox(
        "tenant-a", "dedupe-1", "session-a", {"text": "hello"}, "worker-b"
    )
    assert not created and existing.message_id == "message-1"

    expired = inbox_row[:8] + (now - timedelta(minutes=2),) + inbox_row[9:]
    connection.one = [None, expired, completed_inbox]
    resumed, created = store.accept_inbox(
        "tenant-a", "dedupe-1", "session-a", {"text": "retry"}, "worker-b"
    )
    assert created and resumed.status == InboxStatus.COMPLETED

    connection.rowcounts = [1]
    store.complete_inbox("tenant-a", "dedupe-1", "worker-a", {"ok": True})
    connection.rowcounts = [1]
    connection.one = [outbox_row]
    atomically_completed = store.complete_inbox_and_enqueue_outbox(
        "tenant-a", "dedupe-1", "worker-a", {"ok": True}, "topic", "session-a", "event-1"
    )
    assert atomically_completed.event_id == "event-1"
    connection.rowcounts = [1, 1]
    store.fail_inbox("tenant-a", "dedupe-1", "worker-a", "api_key=secret")
    store.dead_inbox("tenant-a", "dedupe-1", "worker-a", "permanent")

    connection.one = [("event-2",)]
    queued = store.enqueue_outbox("tenant-a", "topic", "session-a", {"n": 2}, event_id="event-2")
    assert queued.event_id == "event-2"
    connection.one = [None, outbox_row]
    assert store.enqueue_outbox("tenant-a", "topic", "session-a", {"n": 1}, event_id="event-1").event_id == "event-1"

    connection.many = [[outbox_row]]
    connection.one = [processing_outbox]
    claimed = store.claim_outbox("worker-a", tenant_id="tenant-a")
    assert len(claimed) == 1 and claimed[0].status == OutboxStatus.PROCESSING
    connection.rowcounts = [1, 1]
    store.complete_outbox("event-1", "worker-a", tenant_id="tenant-a")
    store.fail_outbox("event-1", "worker-a", "api_key=secret", retry_after_seconds=0, tenant_id="tenant-a")

    connection.many = [[inbox_row]]
    assert store.list_inbox_by_tenant("tenant-a")[0].dedupe_key == "dedupe-1"
    connection.many = [[outbox_row]]
    assert store.list_outbox_by_tenant("tenant-a")[0].event_id == "event-1"

    connection.one = [inbox_row]
    restored_inbox = store.restore_inbox(InboxRecord(
        "message-2", "tenant-a", "dedupe-2", "session-a", {"x": 1}, created_at=now, updated_at=now
    ))
    assert restored_inbox.message_id == "message-1"
    connection.one = [outbox_row]
    restored_outbox = store.restore_outbox(OutboxRecord("event-1", "tenant-a", "topic", "session-a", {"ok": True}))
    assert restored_outbox.event_id == "event-1"

    dead_row = outbox_row[:5] + (OutboxStatus.DEAD,) + outbox_row[6:]
    connection.one = [dead_row]
    assert store.replay_outbox("event-1", "tenant-a").status == OutboxStatus.DEAD

    with pytest.raises(RuntimeError, match="ownership lost"):
        connection.rowcounts = [0]
        store.complete_outbox("event-1", "other", tenant_id="tenant-a")
    monkeypatch.setenv("MAX_OUTBOX_ATTEMPTS", "2")


def test_postgres_storage_structured_state_and_projection_protocols():
    storage = pg_storage()
    connection = storage._connection
    event = SessionEvent("tenant-a", "session-a", "event-1", "message", {"text": "hello"}, "trace", "key")
    connection.one = [None, (0,)]
    assert storage.append_event(event) == 1
    assert any("INSERT INTO message_event" in query for query, _ in connection.executed)

    connection.many = [[("tenant-a", "event-1", "session-a", 1, "key", "message", {"text": "hello"}, "trace", datetime.now(UTC))]]
    assert storage.load_events("tenant-a", "session-a")[0].payload == {"text": "hello"}
    connection.one = [None]
    assert storage.load_state("tenant-a", "session-a").latest_event_seq == 0
    connection.one = [(2, 7, '{"answer": "ok"}')]
    assert storage.load_state("tenant-a", "session-a").state == {"answer": "ok"}

    connection.rowcounts = [1]
    assert storage.compare_and_set_state("tenant-a", "session-a", 2, {"answer": "new"})
    connection.rowcounts = [0]
    assert not storage.compare_and_set_state("tenant-a", "session-a", 2, {"answer": "stale"})
    storage.restore_state("tenant-a", "session-a", {"restored": True}, 4)

    memory = MemoryItem("tenant-a", "memory-1", "scope-a", "hello world", {"kind": "note"}, 2)
    storage.put(memory)
    summary = Summary("tenant-a", "session-a", "summary", 7)
    connection.one = [(1, 4)]
    storage.put(summary)
    connection.one = [(9, 8)]
    storage.put(summary)
    memory_row = ("tenant-a", "memory-1", "scope-a", "hello world", 2, {"kind": "note"}, datetime.now(UTC))
    connection.many = [[memory_row]]
    assert storage.search("tenant-a", "hello")[0].memory_id == "memory-1"
    connection.many = [[memory_row]]
    assert storage.search("tenant-a", "hello", scope_keys=("scope-a",))[0].scope_key == "scope-a"
    connection.one = [None]
    assert storage.latest("tenant-a", "session-a") is None
    connection.one = [("tenant-a", "session-a", "summary", 7, 1, datetime.now(UTC))]
    assert storage.latest("tenant-a", "session-a").source_event_seq == 7

    record = AuditRecord("audit-1", "tenant-a", "allow", "trace")
    storage.append(record)
    connection.many = [[(
        "audit-1", "tenant-a", "web", "user", "session-a", "agent", "tool", "allow",
        4, None, 2, 0.1, "trace", {"ok": True}, datetime.now(UTC),
    )]]
    assert storage.list_by_tenant("tenant-a")[0].audit_id == "audit-1"


def test_postgres_storage_idempotency_and_compensation_protocols(monkeypatch):
    storage = pg_storage()
    connection = storage._connection
    now = datetime.now(UTC)
    full_row = ("tenant-a", "request-1", "processing", None, None, "trace", 1, now, now)
    connection.one = [("failed", now - timedelta(seconds=100), 2), full_row]
    started = storage.start("tenant-a", "request-1", "trace-new", lease_seconds=10)
    assert started.status.value == "processing" and started.attempt == 1
    connection.rowcounts = [1]
    connection.one = [full_row]
    assert storage.complete("tenant-a", "request-1", "response", {"ok": True}).response_ref is None
    connection.rowcounts = [1]
    connection.one = [("tenant-a", "request-1", "failed", None, '{"error_type":"bad"}', "trace", 2, now, now)]
    assert storage.fail("tenant-a", "request-1", "bad").status.value == "failed"
    connection.rowcounts = [1]
    assert storage.claim_delivery("tenant-a", "request-1")
    storage.release_delivery("tenant-a", "request-1")
    connection.one = [None]
    with pytest.raises(KeyError):
        storage._update_idempotency("tenant-a", "missing", __import__("trpc_service.storage.base", fromlist=["IdempotencyStatus"]).IdempotencyStatus.COMPLETED, "r", {})

    task_row = ("task-1", "tenant-a", "memory.put", {"content": "x"}, "pending", 0, now, None, now, now)
    connection.one = [task_row]
    assert storage._compensation_enqueue("tenant-a", "memory.put", {"content": "x"}, "task-1").task_id == "task-1"
    connection.many = [[task_row]]
    connection.one = [task_row]
    assert storage._compensation_claim(2, tenant_id="tenant-a")[0].task_id == "task-1"
    storage._compensation_complete("task-1", tenant_id="tenant-a")
    monkeypatch.setenv("COMPENSATION_MAX_ATTEMPTS", "2")
    storage._compensation_fail("task-1", "api_key=secret", retry_after_seconds=0)
    connection.one = [task_row]
    assert storage._compensation_replay("task-1").task_id == "task-1"
    storage.close()


def test_postgres_mailbox_lifecycle_and_export_protocol(monkeypatch):
    store = PostgresSessionMailboxStore.__new__(PostgresSessionMailboxStore)
    store._conn = ScriptedPGConnection()
    store._lock = RLock()
    now = datetime.now(UTC)
    idle = ("tenant-a", "session-a", "idle", 0, 0, None, None, 0, None, 0, None, 0, 0, 0, None, now)
    item = ("tenant-a", "session-a", 1, "message-1", "trace-1", 3, 0, 0, None, now, None)
    assert _mailbox_from_pg(idle).status == "idle"
    assert _mailbox_export_row(idle)["tenant_id"] == "tenant-a"
    assert _item_export_row(item)["message_id"] == "message-1"

    def server_now():
        return now

    def initial_row(*args, **kwargs):
        return idle

    def accept_fetchone(query, params=()):
        if "SELECT 1 FROM session_mailbox_item" in query:
            return None
        if "SELECT tenant_id,session_id,sequence" in query:
            return item
        if "UPDATE session_mailbox" in query:
            return ("tenant-a", "session-a", "queued", 1, 0, None, None, 1, None, 0, None, 0, 0, 3, None, now)
        return None

    store._server_now = server_now
    store._row = initial_row
    store._fetchone = accept_fetchone
    assert store.get("tenant-a", "session-a") is not None
    assert not store.has_unresolved_message("tenant-a", "session-a", "message-1")
    assert store.accept("tenant-a", "session-a", "message-1").status == "queued"

    running = ("tenant-a", "session-a", "running", 1, 0, 1, "message-1", 1, "worker", 1, now + timedelta(seconds=20), 0, 1, 3, None, now)
    lease = SessionMailboxLease("tenant-a", "session-a", "message-1", 1, "worker", 1, now + timedelta(seconds=20), 1, 0, 3)
    store._row = lambda *args, **kwargs: running
    store._fetchone = lambda query, params=(): (now + timedelta(seconds=40),) if "RETURNING lease_until" in query else None
    renewed = store.renew(lease, 30)
    assert renewed.expires_at > now
    store._fetchone = lambda query, params=(): None
    with pytest.raises(SessionLeaseLost):
        store.renew(lease, 30)

    payload = {"mailboxes": [{"tenant_id": "tenant-a", "session_id": "session-a", "status": "idle", "accepted_sequence": 0, "resolved_sequence": 0, "processing_sequence": None, "processing_message_id": None, "queue_generation": 0, "lease_owner": None, "lease_epoch": 0, "lease_until": None, "retry_count": 0, "attempt": 0, "priority": 0, "retry_at": None, "updated_at": now.isoformat()}], "items": []}
    store.restore_export(payload)
    assert store._conn.executed
