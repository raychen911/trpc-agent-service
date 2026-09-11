from __future__ import annotations

import unittest
from datetime import datetime, timezone

from trpc_service.queue import OutboxDispatcher, OutboxRecord


class Repository:
    def __init__(self, records):  # type: ignore[no-untyped-def]
        self.records = list(records)
        self.published = []
        self.released = []
        self.reconciled = 0

    async def reconcile_inbound(self, limit):  # type: ignore[no-untyped-def]
        self.reconciled += limit
        return 0

    async def claim_outbox(self, owner_id, limit, lease_seconds):  # type: ignore[no-untyped-def]
        _ = owner_id, limit, lease_seconds
        values, self.records = self.records, []
        return values

    async def mark_outbox_published(self, record, owner_id):  # type: ignore[no-untyped-def]
        self.published.append((record.outbox_id, owner_id))

    async def release_outbox(self, record, owner_id, error_type):  # type: ignore[no-untyped-def]
        self.released.append((record.outbox_id, owner_id, error_type))


class Queue:
    async def put(self, message):  # type: ignore[no-untyped-def]
        raise AssertionError(f"unexpected inbound publish: {message}")


def record(event_type: str, attempts: int = 1) -> OutboxRecord:
    return OutboxRecord("acme", f"event-{event_type}", event_type, {}, attempts, datetime.now(timezone.utc))


class OutboxDispatcherTest(unittest.IsolatedAsyncioTestCase):
    async def test_custom_handler_marks_event_published(self) -> None:
        repository = Repository([record("session.project")])
        handled = []

        async def project(value):  # type: ignore[no-untyped-def]
            handled.append(value.outbox_id)

        dispatcher = OutboxDispatcher(repository, Queue(), "relay-1", handlers={"session.project": project})

        self.assertEqual(1, await dispatcher.dispatch_once())
        self.assertEqual(["event-session.project"], handled)
        self.assertEqual(100, repository.reconciled)
        self.assertEqual([("event-session.project", "relay-1")], repository.published)
        self.assertEqual([], repository.released)

    async def test_handler_failure_releases_event_for_retry(self) -> None:
        repository = Repository([record("session.project")])

        async def project(value):  # type: ignore[no-untyped-def]
            _ = value
            raise ConnectionError("projection unavailable")

        dispatcher = OutboxDispatcher(repository, Queue(), "relay-1", handlers={"session.project": project})

        self.assertEqual(0, await dispatcher.dispatch_once())
        self.assertEqual([], repository.published)
        self.assertEqual([("event-session.project", "relay-1", "ConnectionError")], repository.released)

    async def test_unknown_event_is_released_for_repository_dlq_policy(self) -> None:
        repository = Repository([record("unknown.event", attempts=8)])
        dispatcher = OutboxDispatcher(repository, Queue(), "relay-1")

        self.assertEqual(0, await dispatcher.dispatch_once())
        self.assertEqual([("event-unknown.event", "relay-1", "ValueError")], repository.released)


if __name__ == "__main__":
    unittest.main()
