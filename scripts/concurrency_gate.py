"""Disposable PostgreSQL/Redis concurrency and fencing acceptance."""

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import sys
import time
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _mailbox_writer(dsn: str, tenant_id: str, index: int, queue) -> None:
    try:
        os.environ["POSTGRES_AUTO_CREATE_SCHEMA"] = "1"
        from trpc_service.storage.postgres_store import PostgresStorage

        storage = PostgresStorage(dsn)
        try:
            record = storage.mailbox.enqueue(
                tenant_id,
                "concurrent-session",
                f"message-{index}",
                f"dedupe-{index}",
                {"index": index},
            )
            queue.put(record.sequence)
        finally:
            storage.close()
    except Exception as exc:
        queue.put({"error": f"{type(exc).__name__}: {exc}"})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--postgres-dsn", required=True)
    parser.add_argument("--redis-url", required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    import redis

    redis_client = redis.Redis.from_url(args.redis_url)
    redis_client.ping()
    tenant_id = f"acceptance-{uuid4().hex}"
    queue = multiprocessing.Queue()
    processes = [
        multiprocessing.Process(target=_mailbox_writer, args=(args.postgres_dsn, tenant_id, index, queue))
        for index in range(max(2, args.workers))
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(60)
    values = [queue.get(timeout=5) for _ in processes]
    errors = [value for value in values if isinstance(value, dict)]
    sequences = sorted(value for value in values if isinstance(value, int))
    mailbox_ok = not errors and sequences == list(range(1, len(processes) + 1))
    from trpc_service.storage.locking import SessionLeaseLost
    from trpc_service.storage.postgres_store import PostgresStorage

    storage = PostgresStorage(args.postgres_dsn)
    try:
        inbox_tenant = f"{tenant_id}-inbox"
        first, accepted_first = storage.inbox_outbox.accept_inbox(
            inbox_tenant, "dedupe-1", "session", {"text": "one"}, "gate", lease_seconds=30
        )
        second, accepted_second = storage.inbox_outbox.accept_inbox(
            inbox_tenant, "dedupe-1", "session", {"text": "one"}, "gate-2", lease_seconds=30
        )
        outbox = storage.inbox_outbox.complete_inbox_and_enqueue_outbox(
            inbox_tenant, "dedupe-1", "gate", {"ok": True},
            "message.sent", "session", f"{tenant_id}-event",
        )
        claimed = storage.mailbox.claim_next(tenant_id, "concurrent-session", "gate-a", 1)
        time.sleep(1.1)
        storage.mailbox.recover_expired(tenant_id, "concurrent-session")
        reclaimed = storage.mailbox.claim_next(tenant_id, "concurrent-session", "gate-b", 30)
        try:
            storage.mailbox.complete(claimed)
        except SessionLeaseLost:
            fencing_ok = True
        else:
            fencing_ok = False
        storage.mailbox.complete(reclaimed)
        inbox_outbox_ok = (
            accepted_first
            and not accepted_second
            and first.message_id == second.message_id
            and outbox.event_id == f"{tenant_id}-event"
        )
    finally:
        storage.close()
    report = {
        "redis": "PONG",
        "sequences": sequences,
        "expected_sequences": list(range(1, len(processes) + 1)),
        "mailbox_concurrency": mailbox_ok,
        "lease_fencing": fencing_ok,
        "inbox_outbox_source_of_truth": inbox_outbox_ok,
        "ok": mailbox_ok and fencing_ok and inbox_outbox_ok,
        "errors": errors,
        "generated_at": time.time(),
    }
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
