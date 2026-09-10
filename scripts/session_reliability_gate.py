"""Real PostgreSQL/Redis gate for session ordering and durable recovery."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _postgres_checks(dsn: str, run_id: str) -> list[dict[str, object]]:
    from trpc_service.database import upgrade_database
    from trpc_service.storage.locking import SessionLeaseLost
    from trpc_service.storage.postgres_store import PostgresStorage

    upgrade_database(dsn)
    tenant_id = f"phase3-{run_id}"
    session_id = "ordered-session"
    setup = PostgresStorage(dsn)
    try:
        setup.session_mailbox_v2.accept(tenant_id, session_id, "message-1")
        setup.session_mailbox_v2.accept(tenant_id, session_id, "message-2")
    finally:
        setup.close()

    def claim(owner: str):
        storage = PostgresStorage(dsn)
        try:
            return storage.session_mailbox_v2.claim(tenant_id, session_id, owner, 1)
        finally:
            storage.close()

    with ThreadPoolExecutor(max_workers=4) as executor:
        claims = list(executor.map(claim, [f"worker-{index}" for index in range(4)]))
    live = [lease for lease in claims if lease is not None]
    concurrency_ok = len(live) == 1 and live[0].sequence == 1
    stale = live[0]

    time.sleep(1.1)
    storage = PostgresStorage(dsn)
    try:
        replacement = storage.session_mailbox_v2.claim(
            tenant_id, session_id, "replacement", 30
        )
        takeover_ok = replacement is not None and replacement.epoch > stale.epoch
        try:
            storage.session_mailbox_v2.commit(stale)
        except SessionLeaseLost:
            stale_fenced = True
        else:
            stale_fenced = False
        state = storage.session_mailbox_v2.dead_letter(
            replacement, "api_key=phase3-secret"
        )
        next_lease = storage.session_mailbox_v2.claim(
            tenant_id, session_id, "next-worker", 30
        )
        poison_ok = (
            state.resolved_sequence == 1
            and next_lease is not None
            and next_lease.sequence == 2
            and not storage.session_mailbox_v2.has_unresolved_message(
                tenant_id, session_id, "message-1"
            )
            and storage.session_mailbox_v2.has_unresolved_message(
                tenant_id, session_id, "message-2"
            )
        )
        outbox_rows = storage.inbox_outbox.list_outbox_by_tenant(tenant_id)
        dead_rows = [row for row in outbox_rows if row.topic == "session.dead_letter.v2"]
        dead_letter_ok = (
            len(dead_rows) == 1
            and "phase3-secret" not in str(dead_rows[0].payload.get("error", ""))
        )
        storage.session_mailbox_v2.commit(next_lease)

        outbox_tenant = f"{tenant_id}-outbox"
        event_id = f"outbox-{run_id}"
        storage.inbox_outbox.enqueue_outbox(
            outbox_tenant, "phase3.test", session_id, {"ok": True}, event_id=event_id
        )
        first = storage.inbox_outbox.claim_outbox(
            "publisher-a", limit=1, lease_seconds=1, tenant_id=outbox_tenant
        )[0]
        time.sleep(1.1)
        second = storage.inbox_outbox.claim_outbox(
            "publisher-b", limit=1, lease_seconds=30, tenant_id=outbox_tenant
        )[0]
        try:
            storage.inbox_outbox.complete_outbox(
                first.event_id, "publisher-a", tenant_id=outbox_tenant
            )
        except RuntimeError:
            outbox_stale_fenced = True
        else:
            outbox_stale_fenced = False
        previous_max = os.environ.get("DURABLE_OUTBOX_MAX_ATTEMPTS")
        os.environ["DURABLE_OUTBOX_MAX_ATTEMPTS"] = "1"
        try:
            storage.inbox_outbox.fail_outbox(
                second.event_id,
                "publisher-b",
                "permanent",
                retry_after_seconds=0,
                tenant_id=outbox_tenant,
            )
            replayed = storage.inbox_outbox.replay_outbox(event_id, outbox_tenant)
        finally:
            if previous_max is None:
                os.environ.pop("DURABLE_OUTBOX_MAX_ATTEMPTS", None)
            else:
                os.environ["DURABLE_OUTBOX_MAX_ATTEMPTS"] = previous_max
        replay_ok = replayed.status == "pending" and replayed.attempts == 0

        compensation = storage.compensation.enqueue(
            outbox_tenant, "audit.append", {}, task_id=f"compensation-{run_id}"
        )
        storage.compensation.claim(limit=1, tenant_id=outbox_tenant)
        previous_compensation_max = os.environ.get("COMPENSATION_MAX_ATTEMPTS")
        os.environ["COMPENSATION_MAX_ATTEMPTS"] = "1"
        try:
            storage.compensation.fail(
                compensation.task_id,
                "permanent",
                retry_after_seconds=0,
                tenant_id=outbox_tenant,
            )
            replayed_compensation = storage.compensation.replay(
                compensation.task_id,
                tenant_id=outbox_tenant,
            )
        finally:
            if previous_compensation_max is None:
                os.environ.pop("COMPENSATION_MAX_ATTEMPTS", None)
            else:
                os.environ["COMPENSATION_MAX_ATTEMPTS"] = previous_compensation_max
        compensation_replay_ok = (
            replayed_compensation.status == "pending"
            and replayed_compensation.attempt == 0
        )

        inbox_tenant = f"{tenant_id}-dead-inbox"
        inbox, claimed = storage.inbox_outbox.accept_inbox(
            inbox_tenant,
            "dedupe-1",
            session_id,
            {"text": "poison"},
            "inbox-worker-a",
        )
        storage.inbox_outbox.dead_inbox(
            inbox_tenant,
            "dedupe-1",
            "inbox-worker-a",
            "token=phase3-secret",
        )
        duplicate, duplicate_claimed = storage.inbox_outbox.accept_inbox(
            inbox_tenant,
            "dedupe-1",
            session_id,
            {"text": "poison"},
            "inbox-worker-b",
        )
        dead_inbox_ok = (
            claimed
            and not duplicate_claimed
            and duplicate.status == "dead"
            and duplicate.message_id == inbox.message_id
            and "phase3-secret" not in str(duplicate.result)
        )

        retry_tenant = f"{tenant_id}-retry"
        storage.session_mailbox_v2.accept(
            retry_tenant, "retry-session", "retry-message"
        )
        retry_lease = storage.session_mailbox_v2.claim(
            retry_tenant, "retry-session", "retry-worker", 30
        )
        from datetime import timedelta

        from trpc_service.storage.base import now_utc

        storage.session_mailbox_v2.retry(
            retry_lease, retry_at=now_utc() + timedelta(milliseconds=100)
        )
        time.sleep(0.2)
        scheduled = storage.session_mailbox_v2.schedule_retries(
            limit=10, tenant_id=retry_tenant
        )
        scheduled_lease = storage.session_mailbox_v2.claim(
            retry_tenant, "retry-session", "scheduled-worker", 30
        )
        tenant_scheduler_ok = scheduled == 1 and scheduled_lease is not None
        storage.session_mailbox_v2.commit(scheduled_lease)
    finally:
        storage.close()

    return [
        {"name": "postgres concurrent single claim", "ok": concurrency_ok},
        {"name": "postgres expired lease takeover", "ok": takeover_ok},
        {"name": "postgres stale epoch fenced", "ok": stale_fenced},
        {"name": "postgres poison message unblocks session", "ok": poison_ok},
        {"name": "postgres atomic redacted dead letter", "ok": dead_letter_ok},
        {"name": "postgres outbox stale owner fenced", "ok": outbox_stale_fenced},
        {"name": "postgres dead outbox manual replay", "ok": replay_ok},
        {"name": "postgres dead compensation manual replay", "ok": compensation_replay_ok},
        {"name": "postgres dead inbox rejects duplicate", "ok": dead_inbox_ok},
        {"name": "postgres tenant-scoped retry scheduler", "ok": tenant_scheduler_ok},
    ]


def _redis_checks(url: str, run_id: str) -> list[dict[str, object]]:
    import redis

    from trpc_service.storage.compensation import RedisCompensationStore

    client = redis.Redis.from_url(url, decode_responses=True)
    client.ping()
    prefix = f"trpc-agent:phase3:{run_id}"
    store = RedisCompensationStore(
        client,
        prefix=prefix,
        visibility_timeout=1,
        max_attempts=3,
    )
    store.orphan_grace_seconds = 0
    try:
        store.enqueue("tenant", "audit.append", {}, task_id="task-1")
        first = store.claim(limit=1, tenant_id="tenant")[0]
        client.hset(
            store.processing_meta_key,
            first.task_id,
            json.dumps({"claimed_at": time.time() - 10}),
        )
        recovered = store.requeue_stale()
        replacement = store.claim(limit=1, tenant_id="tenant")[0]
        crash_recovery_ok = recovered == 1 and replacement.attempt > first.attempt
        store.max_attempts = replacement.attempt
        store.fail(replacement.task_id, "token=phase3-secret", tenant_id="tenant")
        dead_payload = client.lindex(store.dead_letter_key, -1) or ""
        dead_ok = "phase3-secret" not in dead_payload
        replayed = store.replay(replacement.task_id, tenant_id="tenant")
        replay_ok = replayed.status == "pending" and replayed.attempt == 0
    finally:
        keys = list(client.scan_iter(f"{prefix}:*"))
        if keys:
            client.delete(*keys)
        client.close()
    return [
        {"name": "redis crashed compensation reclaim", "ok": crash_recovery_ok},
        {"name": "redis compensation dead letter redacted", "ok": dead_ok},
        {"name": "redis dead compensation manual replay", "ok": replay_ok},
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--postgres-dsn", default=os.getenv("PHASE3_POSTGRES_DSN", ""))
    parser.add_argument("--redis-url", default=os.getenv("PHASE3_REDIS_URL", ""))
    parser.add_argument("--output", default="")
    args = parser.parse_args()
    if not args.postgres_dsn or not args.redis_url:
        report = {
            "status": "not_run",
            "reason": "PHASE3_POSTGRES_DSN and PHASE3_REDIS_URL are required",
        }
    else:
        run_id = uuid4().hex[:12]
        try:
            checks = _postgres_checks(args.postgres_dsn, run_id)
            checks.extend(_redis_checks(args.redis_url, run_id))
            report = {
                "status": "pass" if all(check["ok"] for check in checks) else "fail",
                "checks": checks,
            }
        except Exception as exc:
            report = {"status": "fail", "error": f"{type(exc).__name__}: {exc}"}
    encoded = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded, encoding="utf-8")
    print(encoded)
    return 0 if report["status"] in {"pass", "not_run"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
