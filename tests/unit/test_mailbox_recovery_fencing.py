from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from tests.unit.test_mailbox_store_branch_coverage import (
    Connection,
    Pool,
    item_row,
    mailbox_row,
)
from trpc_service.storage.mailbox import PostgresSessionMailboxStore
from trpc_service.storage.models import MailboxStatus

BASE = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
TENANT = "tenant-a"
SESSION = "session-a"
INBOUND = UUID("11111111-1111-1111-1111-111111111111")


def _expired_mailbox() -> dict[str, object]:
    return mailbox_row(
        status=MailboxStatus.RUNNING.value,
        processing=1,
        processing_inbound=INBOUND,
        owner="old-worker",
        epoch=4,
        expires=BASE - timedelta(seconds=1),
    )


def _session_lease(*, owner: str, epoch: int, expires: datetime) -> dict[str, object]:
    return {
        "lease_owner": owner,
        "lease_epoch": epoch,
        "lease_expires_at": expires,
    }


@pytest.mark.asyncio
async def test_recovery_refuses_mailbox_when_new_authoritative_lease_is_active() -> None:
    """A replacement session lease wins over an expired derived mailbox lease."""

    connection = Connection(
        fetchrows=[
            _expired_mailbox(),
            _session_lease(owner="new-worker", epoch=5, expires=BASE + timedelta(minutes=1)),
        ],
        fetchvals=[BASE],
    )

    recovered = await PostgresSessionMailboxStore(Pool(connection)).recover(TENANT, SESSION)

    assert recovered is None
    assert not any(
        kind == "fetchrow" and "UPDATE session_mailboxes" in str(args[0])
        for kind, args in connection.calls
    )
    locks = [
        str(args[0])
        for kind, args in connection.calls
        if kind == "fetchrow" and "FOR UPDATE" in str(args[0])
    ]
    assert locks[0].find("FROM session_mailboxes") < locks[1].find("FROM sessions")


@pytest.mark.asyncio
async def test_recovery_allows_expired_authoritative_lease_and_emits_ready_atomically() -> None:
    """An expired old session lease can be recovered and wakes the mailbox."""

    updated = mailbox_row(status=MailboxStatus.QUEUED.value, generation=5)
    connection = Connection(
        fetchrows=[
            _expired_mailbox(),
            _session_lease(owner="old-worker", epoch=4, expires=BASE - timedelta(seconds=1)),
            item_row(),
            updated,
        ],
        fetchvals=[BASE],
    )

    recovered = await PostgresSessionMailboxStore(Pool(connection)).recover(TENANT, SESSION)

    assert recovered is not None and recovered.status is MailboxStatus.QUEUED
    updates = [
        index
        for index, (kind, args) in enumerate(connection.calls)
        if kind == "fetchrow" and "UPDATE session_mailboxes" in str(args[0])
    ]
    outbox = [
        index
        for index, (kind, args) in enumerate(connection.calls)
        if kind == "execute" and "session.ready.v2" in str(args[0])
    ]
    assert updates and outbox and updates[0] < outbox[0]


@pytest.mark.asyncio
async def test_reconcile_recovers_active_derived_lease_when_authoritative_lease_is_missing() -> (
    None
):
    """A mailbox lease alone is not proof of a live worker."""

    active_mailbox = mailbox_row(
        status=MailboxStatus.RUNNING.value,
        processing=1,
        processing_inbound=INBOUND,
        owner="stale-mailbox-owner",
        epoch=7,
        expires=BASE + timedelta(minutes=1),
    )
    updated = mailbox_row(status=MailboxStatus.QUEUED.value, generation=8)
    connection = Connection(
        fetchrows=[active_mailbox, None, item_row(), updated],
        fetchvals=[BASE],
    )

    recovered = await PostgresSessionMailboxStore(Pool(connection)).reconcile(TENANT, SESSION)

    assert recovered is not None and recovered.status is MailboxStatus.QUEUED


def test_recovery_migration_is_bounded_and_locks_authority_after_mailbox() -> None:
    """The post-0010 definitions are safe for already-upgraded databases."""

    migration = Path("migrations/versions/0011_mailbox_recovery_fencing.py").read_text(
        encoding="utf-8"
    )
    normalized = " ".join(migration.lower().split())
    assert 'revision = "0011_mailbox_recovery_fencing"' in migration
    assert 'down_revision = "0010_consistency_guards"' in migration
    assert normalized.count("limit p_limit") >= 2
    assert normalized.count("for update of m skip locked") >= 2
    assert normalized.count("from public.sessions as s") >= 2
    assert normalized.count("for update;") >= 2
    assert normalized.count("session.ready.v2") >= 2
    assert normalized.count("on conflict do nothing") >= 2
    assert normalized.count("and not exists (") >= 2
    assert "active.lease_expires_at > clock_timestamp()" in normalized
    # Every recovery function documents and implements mailbox -> sessions
    # locking, rather than joining and mutating both relations in one update.
    for function_name in (
        "sweep_expired_session_leases",
        "reconcile_session_mailboxes_v2",
    ):
        function = normalized.split(f"create or replace function {function_name}", 1)[1]
        assert function.find("from public.session_mailboxes as m") < function.find(
            "from public.sessions as s"
        )
        assert function.find("not exists (") < function.find("limit p_limit")


def test_recovery_migration_downgrade_restores_0010_definitions() -> None:
    migration = Path("migrations/versions/0011_mailbox_recovery_fencing.py").read_text(
        encoding="utf-8"
    )
    downgrade = migration.split("def downgrade()", 1)[1].lower()
    assert "op.execute" in downgrade
    assert "create or replace function sweep_expired_session_leases" in downgrade
    assert "create or replace function reconcile_session_mailboxes_v2" in downgrade
    assert "update public.session_mailboxes as m" in downgrade
    assert "return reconcile_session_mailboxes_v2_legacy" in downgrade
    assert "create or replace function reconcile_session_mailboxes(p_limit integer)" in downgrade
    assert "grant execute on function sweep_expired_session_leases(integer)" in downgrade
    assert "grant execute on function reconcile_session_mailboxes_v2(integer,integer)" in downgrade
    assert "pass" not in downgrade
