from __future__ import annotations

import json
import os
import tempfile
import types
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import RLock
from unittest.mock import patch

os.environ.setdefault("TRPC_AGENT_NO_AUTO_APP", "1")

from trpc_service.channels.base import InboundMessage
from trpc_service.gateway.redis_streams import RedisStreamsTransport
from trpc_service.gateway.router import AgentGateway
from trpc_service.storage.base import IdempotencyRecord, IdempotencyStatus, SessionEvent
from trpc_service.storage.durable import (
    DurableOutboxDispatcher,
    InboxStatus,
    InMemoryInboxOutbox,
    OutboxStatus,
)
from trpc_service.storage.in_memory import InMemoryStorage
from trpc_service.storage.locking import SessionLeaseLost
from trpc_service.storage.sql_store import SQLiteStorage
from trpc_service.tenant.models import default_demo_config
from trpc_service.tenant.repository import (
    InMemoryTenantRepository,
    SQLiteTenantRepository,
    TenantRepositoryConflict,
)
from trpc_service.tenant.service import TenantConfigConflict, TenantService


class ReliabilityTests(unittest.TestCase):
    def test_redis_idempotency_updates_keep_a_ttl(self):
        from trpc_service.storage.redis_store import RedisStorage

        class FakeRedis:
            def __init__(self):
                self.calls = []

            def set(self, *args, **kwargs):
                self.calls.append((args, kwargs))

        storage = RedisStorage.__new__(RedisStorage)
        storage.client = FakeRedis()
        storage.prefix = "test"
        storage.idempotency_ttl = 123
        record = IdempotencyRecord(
            "tenant",
            "request",
            IdempotencyStatus.COMPLETED,
            response_ref="response",
            result={"ok": True},
        )

        storage._save_idempotency(record)

        self.assertEqual(storage.client.calls[0][1]["ex"], 123)

    def test_postgres_json_readers_accept_driver_return_variants(self):
        from trpc_service.storage.durable import _json_object

        expected = {"text": "中文", "count": 2}
        self.assertEqual(_json_object(expected), expected)
        self.assertEqual(_json_object(json.dumps(expected)), expected)
        self.assertEqual(_json_object(json.dumps(expected).encode("utf-8")), expected)

    def test_postgres_reconnect_updates_inbox_outbox_connection(self):
        from trpc_service.storage.postgres_store import PostgresStorage

        class FakeConnection:
            def __init__(self):
                self.autocommit = False
                self.closed = False

        replacement = FakeConnection()

        class FakePsycopg:
            def __init__(self):
                self.dsn = None

            def connect(self, dsn):
                self.dsn = dsn
                return replacement

        class FakeInboxOutbox:
            def __init__(self):
                self.connection = None

            def set_connection(self, connection):
                self.connection = connection

        psycopg = FakePsycopg()
        inbox_outbox = FakeInboxOutbox()
        storage = PostgresStorage.__new__(PostgresStorage)
        storage._psycopg = psycopg
        storage._dsn = "postgresql://example"
        storage._lock = RLock()
        storage._connection = None
        storage.inbox_outbox = inbox_outbox

        storage._connect()

        self.assertIs(storage._connection, replacement)
        self.assertIs(inbox_outbox.connection, replacement)
        self.assertTrue(replacement.autocommit)
        self.assertEqual(psycopg.dsn, "postgresql://example")

    def test_session_fencing_rejects_superseded_worker(self):
        storage = InMemoryStorage()
        first = storage.session.acquire_session_lease("tenant", "session", timeout=0)
        storage.session._leases[("tenant", "session")] = first.__class__(
            first.tenant_id,
            first.session_id,
            first.owner,
            first.fencing_token,
            datetime.now(UTC) - timedelta(seconds=1),
        )
        second = storage.session.acquire_session_lease("tenant", "session", timeout=0)
        self.assertGreater(second.fencing_token, first.fencing_token)
        with self.assertRaises(SessionLeaseLost):
            storage.session.validate_session_lease(first)
        storage.session.validate_session_lease(second)
        storage.session.release_session_lease("tenant", "session", second)

    def test_superseded_fencing_token_cannot_append_or_update_state(self):
        storage = InMemoryStorage()
        first = storage.session.acquire_session_lease("tenant", "session", timeout=0)
        storage.session._leases[("tenant", "session")] = first.__class__(
            first.tenant_id,
            first.session_id,
            first.owner,
            first.fencing_token,
            datetime.now(UTC) - timedelta(seconds=1),
        )
        second = storage.session.acquire_session_lease("tenant", "session", timeout=0)
        event = SessionEvent(
            tenant_id="tenant",
            session_id="session",
            event_id="event-1",
            event_type="user_message",
            payload={"text": "blocked"},
            trace_id="trace",
        )
        with self.assertRaises(SessionLeaseLost):
            storage.session.append_event(event, fencing_token=first.fencing_token)
        with self.assertRaises(SessionLeaseLost):
            storage.session.compare_and_set_state(
                "tenant",
                "session",
                0,
                {"blocked": True},
                fencing_token=first.fencing_token,
            )
        self.assertEqual(storage.session.append_event(event, fencing_token=second.fencing_token), 1)
        self.assertTrue(
            storage.session.compare_and_set_state(
                "tenant",
                "session",
                0,
                {"accepted": True},
                fencing_token=second.fencing_token,
            )
        )
        storage.session.release_session_lease("tenant", "session", second)

    def test_sqlite_inbox_outbox_deduplicates_and_recovers_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = SQLiteStorage(Path(directory) / "state.sqlite3")
            first, created = storage.inbox_outbox.accept_inbox(
                "tenant", "message-1", "session-1", {"text": "hello"}, "owner-a", lease_seconds=10
            )
            self.assertTrue(created)
            duplicate, created = storage.inbox_outbox.accept_inbox(
                "tenant", "message-1", "session-1", {"text": "hello"}, "owner-b", lease_seconds=10
            )
            self.assertFalse(created)
            self.assertEqual(duplicate.message_id, first.message_id)
            storage.inbox_outbox.complete_inbox("tenant", "message-1", "owner-a", {"text": "ok"})
            completed, created = storage.inbox_outbox.accept_inbox(
                "tenant", "message-1", "session-1", {"text": "hello"}, "owner-c", lease_seconds=10
            )
            self.assertFalse(created)
            self.assertEqual(completed.status, InboxStatus.COMPLETED)

            storage.inbox_outbox.enqueue_outbox(
                "tenant", "agent.response", "session-1", {"text": "ok"}, event_id="event-1"
            )
            claimed = storage.inbox_outbox.claim_outbox("owner-a", limit=1, lease_seconds=10)
            self.assertEqual([item.event_id for item in claimed], ["event-1"])
            storage.inbox_outbox.fail_outbox("event-1", "owner-a", "temporary", retry_after_seconds=0)
            reclaimed = storage.inbox_outbox.claim_outbox("owner-b", limit=1, lease_seconds=10)
            self.assertEqual([item.event_id for item in reclaimed], ["event-1"])
            storage.inbox_outbox.complete_outbox("event-1", "owner-b")
            storage.close()

    def test_inbox_failure_redacts_secret_details(self):
        store = InMemoryInboxOutbox()
        store.accept_inbox("tenant", "message-1", "session-1", {"text": "hello"}, "owner-a")
        store.fail_inbox("tenant", "message-1", "owner-a", "Authorization: Bearer top-secret")
        record = store._inbox[("tenant", "message-1")]
        self.assertNotIn("top-secret", record.result["error"])
        self.assertIn("[secret-redacted]", record.result["error"])

    def test_outbox_dispatcher_completes_successful_record(self):
        store = InMemoryInboxOutbox()
        store.enqueue_outbox("tenant", "test", "aggregate", {"value": 1}, event_id="event-1")
        received = []
        dispatcher = DurableOutboxDispatcher(store, received.append, owner="dispatcher-a")
        self.assertEqual(dispatcher.run_once(), 1)
        self.assertEqual(received[0].payload, {"value": 1})
        self.assertEqual(store._outbox["event-1"].status, "completed")

    def test_durable_outbox_moves_poison_event_to_dead(self):
        store = InMemoryInboxOutbox()
        store.enqueue_outbox("tenant", "test", "aggregate", {"value": 1}, event_id="event-1")
        dispatcher = DurableOutboxDispatcher(
            store,
            lambda _record: (_ for _ in ()).throw(RuntimeError("token=secret")),
            owner="dispatcher-a",
        )
        with patch.dict(os.environ, {"DURABLE_OUTBOX_MAX_ATTEMPTS": "1"}, clear=False):
            self.assertEqual(dispatcher.run_once(), 0)
        self.assertEqual(store._outbox["event-1"].status, OutboxStatus.DEAD)
        self.assertNotIn("token=secret", store._outbox["event-1"].last_error)
        self.assertEqual(store.claim_outbox("dispatcher-b"), [])

    def test_gateway_durable_inbox_returns_idempotent_result(self):
        repository = InMemoryTenantRepository()
        repository.create(default_demo_config())
        tenants = TenantService(repository)
        storage = InMemoryStorage()
        gateway = AgentGateway(tenants, storage)
        message = InboundMessage(
            channel="telegram",
            account_id="corp_account_1",
            external_message_id="durable-message",
            external_user_id="durable-user",
            text="hello",
        )
        with patch.dict(os.environ, {"DURABLE_INBOX_OUTBOX": "1", "TRPC_AGENT_RUNTIME_MODE": "local"}, clear=False):
            first = gateway.dispatch(message)
            second = gateway.dispatch(message)
        self.assertEqual(first[2], second[2])
        self.assertEqual(len(storage.inbox_outbox._outbox), 1)
        record = next(iter(storage.inbox_outbox._inbox.values()))
        self.assertEqual(record.status, InboxStatus.COMPLETED)
        storage.close()

    def test_admin_etag_rejects_stale_update(self):
        try:
            from fastapi.testclient import TestClient
        except (ImportError, RuntimeError):
            self.skipTest("FastAPI TestClient dependency is unavailable")
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "TENANT_DB_PATH": str(Path(directory) / "tenant.sqlite3"),
                "ADMIN_API_KEY": "etag-key",
                "REQUIRE_ETAG": "1",
            },
            clear=False,
        ):
            from trpc_service.web.app import create_app

            with TestClient(create_app()) as client:
                headers = {"X-Admin-API-Key": "etag-key"}
                current = client.get("/admin/v1/tenants/tenant_demo", headers=headers)
                self.assertEqual(current.status_code, 200)
                self.assertEqual(current.headers.get("etag"), '"1"')
                payload = current.json()
                stale = client.put(
                    "/admin/v1/tenants/tenant_demo/config",
                    headers={**headers, "If-Match": '"999"'},
                    json=payload,
                )
                self.assertEqual(stale.status_code, 412)
                missing = client.put(
                    "/admin/v1/tenants/tenant_demo/config",
                    headers=headers,
                    json=payload,
                )
                self.assertEqual(missing.status_code, 428)

    def test_sqlite_tenant_repository_checks_expected_version_inside_write(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteTenantRepository(Path(directory) / "tenant.sqlite3")
            original = repository.create(default_demo_config())
            changed = repository.get("tenant_demo")
            changed.apps[0].prompt = "version two"
            saved = repository.save_version(changed, expected_version=original.config_version)
            repository.publish("tenant_demo", saved.config_version, expected_version=original.config_version)

            stale = repository.get("tenant_demo", version=original.config_version)
            stale.apps[0].prompt = "stale version"
            with self.assertRaises(TenantRepositoryConflict):
                repository.save_and_publish(stale, expected_version=original.config_version)
            self.assertEqual(repository.get("tenant_demo").config_version, saved.config_version)
            repository.close()

    def test_tenant_service_maps_repository_conflict_to_config_conflict(self):
        repository = InMemoryTenantRepository()
        current = repository.create(default_demo_config())
        service = TenantService(repository)
        changed = repository.get(current.tenant_id)
        changed.apps[0].prompt = "new"
        repository.save_and_publish(changed, expected_version=current.config_version)
        stale = repository.get(current.tenant_id, version=current.config_version)
        stale.apps[0].prompt = "stale"
        with self.assertRaises(TenantConfigConflict):
            service.create_config_version(
                current.tenant_id,
                stale,
                expected_version=current.config_version,
            )

    def test_streams_malformed_payload_is_dead_lettered_and_acked(self):
        class FakeRedis:
            def __init__(self):
                self.acks = []
                self.deleted = []
                self.dead_letters = []

            def xack(self, stream, group, message_id):
                self.acks.append((stream, group, message_id))

            def xdel(self, stream, message_id):
                self.deleted.append((stream, message_id))

            def xadd(self, stream, fields):
                self.dead_letters.append((stream, fields))
                return "dlq-1"

        transport = RedisStreamsTransport.__new__(RedisStreamsTransport)
        transport.client = FakeRedis()
        transport.stream_key = "worker:stream"
        transport.group = "workers"
        transport.dead_letter_key = "worker:dead-letter"
        transport.max_attempts = 3
        transport._process("1-0", {"payload": "{broken"}, lambda payload: None)
        self.assertEqual(transport.client.acks, [("worker:stream", "workers", "1-0")])
        self.assertEqual(transport.client.deleted, [("worker:stream", "1-0")])
        self.assertEqual(transport.client.dead_letters[0][0], "worker:dead-letter")
        self.assertIn("malformed stream payload", transport.client.dead_letters[0][1]["error"])

        transport.max_attempts = 1
        transport._process(
            "2-0",
            {"payload": json.dumps({"request_id": "request-1"})},
            lambda _payload: (_ for _ in ()).throw(RuntimeError("token=top-secret")),
        )
        self.assertNotIn("top-secret", transport.client.dead_letters[1][1]["error"])

    def test_redis_rate_limit_uses_shared_wall_clock_window(self):
        from trpc_service.channels import reliable

        class FakePipeline:
            def __init__(self, client):
                self.client = client
                self.key = None

            def incr(self, key):
                self.key = key
                self.client.counts[key] = self.client.counts.get(key, 0) + 1

            def expire(self, _key, _seconds):
                return None

            def execute(self):
                return [self.client.counts[self.key], True]

        class FakeRedis:
            def __init__(self):
                self.counts = {}
                self.keys = []

            def pipeline(self, transaction=True):
                self.assert_transaction = transaction
                pipeline = FakePipeline(self)
                original_incr = pipeline.incr

                def incr(key):
                    self.keys.append(key)
                    original_incr(key)

                pipeline.incr = incr
                return pipeline

        limiter = reliable.ChannelRateLimiter.__new__(reliable.ChannelRateLimiter)
        limiter._redis = FakeRedis()
        limiter._prefix = "test"
        with patch.object(reliable.time, "time", return_value=1234.9):
            limiter.acquire("wecom:corp", 2)
            limiter.acquire("wecom:corp", 2)
        self.assertEqual(
            limiter._redis.keys,
            ["test:channel-rate:wecom:corp:1234", "test:channel-rate:wecom:corp:1234"],
        )

    def test_stream_worker_does_not_publish_transient_error_result(self):
        from trpc_service.gateway.worker_queue import WorkerQueue

        class FakeRedis:
            def __init__(self):
                self.results = []
                self.retries = []

            def setex(self, *args):
                self.results.append(args)

        queue = WorkerQueue.__new__(WorkerQueue)
        queue.client = FakeRedis()
        queue.prefix = "test"
        queue.max_attempts = 3
        handler = queue._stream_handler(
            lambda _request, _config: (_ for _ in ()).throw(RuntimeError("temporary"))
        )
        payload = {
            "request_id": "request-1",
            "attempt": 0,
            "request": {
                "tenant_context": {
                    "tenant_id": "tenant",
                    "agent_app_id": "app",
                    "config_version": 1,
                    "trace_id": "trace",
                    "session_id": "session",
                    "channel": "web",
                    "user_id": "user",
                },
                "user_input": {"text": "hello", "metadata": {}},
                "idempotency_key": "message-1",
            },
            "config": default_demo_config().to_dict(),
        }
        with self.assertRaises(RuntimeError):
            handler(payload)
        self.assertEqual(queue.client.results, [])

        payload["attempt"] = 2
        with self.assertRaises(RuntimeError):
            handler(payload)
        self.assertEqual(len(queue.client.results), 1)
        self.assertIn("RuntimeError", queue.client.results[0][2])

    def test_list_worker_malformed_payload_is_dead_lettered(self):
        class FakeRedis:
            def __init__(self):
                self.dead_letters = []
                self.removed = []

            def rpush(self, key, value):
                self.dead_letters.append((key, value))

            def lrem(self, key, count, value):
                self.removed.append((key, count, value))
                return 1

        from trpc_service.gateway.worker_queue import WorkerQueue

        queue = WorkerQueue.__new__(WorkerQueue)
        queue.client = FakeRedis()
        queue.streams = None
        queue.processing_key = "worker:processing"
        queue.processing_meta_key = "worker:processing-meta"
        queue.dead_letter_key = "worker:dead-letter"
        queue.requeue_stale = lambda: 0
        claims = iter(["{broken"])
        queue._claim = lambda _timeout: next(claims)
        with self.assertRaises(StopIteration):
            queue.consume(lambda _request, _config: None, timeout=0)
        self.assertEqual(queue.client.removed, [("worker:processing", 1, "{broken")])
        self.assertIn("malformed worker payload", queue.client.dead_letters[0][1])

    def test_webhook_and_outbound_malformed_payloads_are_dead_lettered(self):
        class FakeRedis:
            def __init__(self):
                self.dead_letters = []
                self.removed = []

            def rpush(self, key, value):
                self.dead_letters.append((key, value))

            def lrem(self, key, count, value):
                self.removed.append((key, count, value))
                return 1

            def hdel(self, *_args):
                return 1

            def hget(self, _key, _field):
                return "{broken"

        from trpc_service.channels.outbound_queue import OutboundDeliveryQueue
        from trpc_service.gateway.worker_queue import DurableWebhookQueue

        webhook = DurableWebhookQueue.__new__(DurableWebhookQueue)
        webhook.client = FakeRedis()
        webhook.dead_letter_key = "webhook:dead-letter"
        webhook.processing_key = "webhook:processing"
        webhook.processing_meta_key = "webhook:processing-meta"
        webhook.requeue_stale = lambda: 0
        webhook._claim = lambda _timeout: "{broken"
        self.assertTrue(webhook.consume_once(lambda _item: None, timeout=0))
        self.assertIn("malformed webhook payload", webhook.client.dead_letters[0][1])

        outbound = OutboundDeliveryQueue.__new__(OutboundDeliveryQueue)
        outbound.client = FakeRedis()
        outbound.data_key = "outbound:data"
        outbound.processing_key = "outbound:processing"
        outbound.processing_meta_key = "outbound:processing-meta"
        outbound.dead_letter_key = "outbound:dead-letter"
        outbound.requeue_stale = lambda: 0
        outbound._move_to_processing = lambda _timeout: "task-1"
        self.assertTrue(outbound.consume_once(lambda _item: None, timeout=0))
        self.assertIn("malformed outbound payload", outbound.client.dead_letters[0][1])

    def test_durable_webhook_status_records_completion_without_answer_content(self):
        class FakeRedis:
            def __init__(self):
                self.values = {}
                self.items = []

            def rpush(self, _key, value):
                self.items.append(value)

            def setex(self, key, _ttl, value):
                self.values[key] = value

            def get(self, key):
                return self.values.get(key)

            def lrem(self, *_args):
                return 1

            def hdel(self, *_args):
                return 1

            def hget(self, *_args):
                return None

        from trpc_service.gateway.worker_queue import DurableWebhookQueue

        queue = DurableWebhookQueue.__new__(DurableWebhookQueue)
        queue.client = FakeRedis()
        queue.queue_key = "webhook:queue"
        queue.processing_key = "webhook:processing"
        queue.processing_meta_key = "webhook:processing-meta"
        queue.dead_letter_key = "webhook:dead-letter"
        queue.status_prefix = "webhook:status:"
        queue.requeue_stale = lambda: 0
        queue._claim = lambda _timeout: queue.client.items.pop(0)
        queue._start_heartbeat = lambda _task_id: types.SimpleNamespace(set=lambda: None)
        task_id = queue.submit("telegram", "account", {"text": "hello"}, None, tenant_id="tenant")
        self.assertTrue(queue.consume_once(lambda _item: {"ok": True, "answer": "private answer"}, timeout=0))
        status = queue.status(task_id)
        self.assertEqual(status["status"], "completed")
        self.assertEqual(status["tenant_id"], "tenant")
        self.assertEqual(status["result"]["answer_length"], len("private answer"))
        self.assertNotIn("private answer", str(status))


if __name__ == "__main__":
    unittest.main()
