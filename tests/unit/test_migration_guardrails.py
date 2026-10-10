from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from trpc_service.storage.migration import (
    MigrationGuardError,
    MigrationLeaseLost,
    MigrationManifestConflict,
    MigrationScopeManifest,
    MigrationSourceKind,
    MigrationTargetNotEmpty,
    PostgresMigrationGuard,
)


class FakeConnection:
    def __init__(self) -> None:
        self.manifests: dict[tuple[str, str], dict[str, object]] = {}
        self.leases: dict[tuple[str, str], dict[str, object]] = {}
        self.barriers: dict[str, dict[str, object]] = {}
        self.target_counts = {
            "inbound_messages": 0,
            "outbound_messages": 0,
            "delivery_attempts": 0,
            "sessions": 0,
            "session_turns": 0,
            "turn_intents": 0,
            "session_events": 0,
            "memories": 0,
            "session_summaries": 0,
            "artifacts": 0,
            "knowledge_items": 0,
            "knowledge_embeddings": 0,
            "outbox_events": 0,
            "dead_letters": 0,
            "tool_executions": 0,
            "confirmation_challenges": 0,
            "audit_logs": 0,
            "session_mailboxes": 0,
            "session_mailbox_items": 0,
            "wecom_connection_state": 0,
            "im_acceptance_evidence_events": 0,
            "im_acceptance_runs": 0,
            "migration_checkpoints": 0,
            "migration_scope_manifests": 0,
            "migration_leases": 0,
        }
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.fetchvals: list[tuple[str, tuple[object, ...]]] = []
        self.history: list[tuple[str, tuple[object, ...]]] = []

    def acquire(self):
        return self

    def transaction(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def execute(self, query: str, *args: object):
        self.executed.append((query, args))
        self.history.append((query, args))
        if "INSERT INTO migration_scope_manifests" in query:
            key = (str(args[0]), str(args[1]))
            self.manifests.setdefault(
                key,
                {
                    "tenant_id": args[0],
                    "migration_id": args[1],
                    "source_kind": args[2],
                    "kinds": list(args[3]),
                    "source_snapshot_id": args[4],
                    "source_count": args[5],
                    "source_checksum": args[6],
                    "app_id": args[7],
                    "app_revision": args[8],
                    "config_version": args[9],
                    "binding_id": args[10],
                    "binding_revision": args[11],
                },
            )

    async def fetchval(self, query: str, *args: object):
        self.fetchvals.append((query, args))
        self.history.append((query, args))
        return 0

    async def fetchrow(self, query: str, *args: object):
        self.executed.append((query, args))
        self.history.append((query, args))
        if "INSERT INTO migration_scope_manifests" in query:
            key = (str(args[0]), str(args[1]))
            self.manifests.setdefault(
                key,
                {
                    "tenant_id": args[0],
                    "migration_id": args[1],
                    "source_kind": args[2],
                    "kinds": list(args[3]),
                    "source_snapshot_id": args[4],
                    "source_count": args[5],
                    "source_checksum": args[6],
                    "app_id": args[7],
                    "app_revision": args[8],
                    "config_version": args[9],
                    "binding_id": args[10],
                    "binding_revision": args[11],
                },
            )
            return None
        if "INSERT INTO migration_write_barriers" in query:
            tenant_id = str(args[0])
            self.barriers[tenant_id] = {
                "tenant_id": args[0],
                "migration_id": args[1],
                "owner_instance": args[2],
                "lease_epoch": args[3],
                "mode": "active",
            }
            return {"tenant_id": args[0]}
        if "UPDATE migration_write_barriers" in query:
            tenant_id = str(args[0])
            barrier = self.barriers.get(tenant_id)
            if "SET mode='released'" in query:
                if (
                    barrier is None
                    or barrier["migration_id"] != args[1]
                    or barrier["owner_instance"] != args[2]
                    or barrier["lease_epoch"] != args[3]
                    or barrier["mode"] != "active"
                ):
                    return None
                barrier["mode"] = "released"
                return {"tenant_id": args[0]}
            if barrier is None:
                return None
            barrier.update(
                {
                    "migration_id": args[1],
                    "owner_instance": args[2],
                    "lease_epoch": args[3],
                    "mode": "active",
                }
            )
            return {"tenant_id": args[0]}
        if "UPDATE migration_leases AS l" in query:
            key = (str(args[0]), str(args[1]))
            row = self.leases.get(key)
            barrier = self.barriers.get(str(args[0]))
            now = datetime.now(UTC)
            if (
                row is None
                or barrier is None
                or barrier["migration_id"] != row["migration_id"]
                or barrier["owner_instance"] != args[3]
                or barrier["lease_epoch"] != args[4]
                or barrier["mode"] != "active"
                or row["owner_id"] != args[2]
                or row.get("owner_instance", "legacy") != args[3]
                or row["lease_epoch"] != args[4]
                or row["expires_at"] <= now
            ):
                return None
            row["expires_at"] = max(
                row["expires_at"] + timedelta(microseconds=1),
                now + timedelta(seconds=float(args[5])),
            )
            return row
        if "FROM migration_scope_manifests" in query:
            return self.manifests.get((str(args[0]), str(args[1])))
        if "SELECT tenant_id,migration_id,owner_id,owner_instance,lease_epoch,expires_at" in query:
            now = datetime.now(UTC)
            active = [
                row
                for row in self.leases.values()
                if row["tenant_id"] == args[0] and row["expires_at"] > now
            ]
            return max(active, key=lambda row: row["expires_at"]) if active else None
        if "SET expires_at=now()" in query:
            key = (str(args[0]), str(args[1]))
            row = self.leases.get(key)
            if (
                row is None
                or row["owner_id"] != args[2]
                or row.get("owner_instance", "legacy") != args[3]
                or row["lease_epoch"] != args[4]
            ):
                return None
            if row["expires_at"] <= datetime.now(UTC):
                return None
            row["expires_at"] = datetime.now(UTC)
            return {"tenant_id": args[0]}
        if "UPDATE migration_leases" in query:
            key = (str(args[0]), str(args[1]))
            row = self.leases.get(key)
            now = datetime.now(UTC)
            if (
                row is None
                or row["owner_id"] != args[2]
                or row.get("owner_instance", "legacy") != args[3]
                or row["lease_epoch"] != args[4]
                or row["expires_at"] <= now
            ):
                return None
            row["expires_at"] = max(
                row["expires_at"] + timedelta(microseconds=1),
                now + timedelta(seconds=float(args[5])),
            )
            return row
        if "INSERT INTO migration_leases" in query:
            key = (str(args[0]), str(args[1]))
            existing = self.leases.get(key)
            now = datetime.now(UTC)
            if existing is not None and existing["expires_at"] > now:
                return None
            epoch = int(existing["lease_epoch"]) + 1 if existing is not None else 1
            row = {
                "tenant_id": args[0],
                "migration_id": args[1],
                "owner_id": args[2],
                "owner_instance": args[3],
                "lease_epoch": epoch,
                "expires_at": now + timedelta(seconds=float(args[4])),
            }
            self.leases[key] = row
            return row
        if "FROM migration_leases" in query and "owner_instance=$4" in query:
            key = (str(args[0]), str(args[1]))
            row = self.leases.get(key)
            barrier = self.barriers.get(str(args[0]))
            if (
                row is None
                or barrier is None
                or barrier["migration_id"] != row["migration_id"]
                or barrier["owner_instance"] != args[3]
                or barrier["lease_epoch"] != args[4]
                or barrier["mode"] != "active"
                or row.get("owner_instance", "legacy") != args[3]
                or row["owner_id"] != args[2]
                or row["lease_epoch"] != args[4]
                or row["expires_at"] <= datetime.now(UTC)
            ):
                return None
            return {"tenant_id": args[0]}
        raise AssertionError(f"unhandled fetchrow query: {query}")

    async def fetch(self, query: str, *args: object):
        self.history.append((query, args))
        assert "sessions" in query and "memories" in query
        assert "session_summaries" in query and "artifacts" in query
        assert "knowledge_items" in query
        assert "session_mailboxes" in query and "migration_leases" in query
        return [
            {"table_name": name, "row_count": count}
            for name, count in sorted(self.target_counts.items())
        ]


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def acquire(self):
        return self.connection


def make_manifest(**updates: object) -> MigrationScopeManifest:
    value: dict[str, object] = {
        "tenant_id": "tenant-1",
        "migration_id": "redis-to-pg-1",
        "source_kind": MigrationSourceKind.REDIS,
        "kinds": ("memory", "session"),
        "source_snapshot_id": "redis-snapshot-1",
        "source_count": 2,
        "source_checksum": "a" * 64,
        "app_id": "assistant",
        "app_revision": 1,
        "config_version": 3,
        "binding_id": "feishu-binding",
        "binding_revision": 2,
    }
    value.update(updates)
    return MigrationScopeManifest(**value)


@pytest.mark.asyncio
async def test_acquire_sets_rls_and_advisory_lock_and_manifest_cannot_drift() -> None:
    connection = FakeConnection()
    guard = PostgresMigrationGuard(FakePool(connection))
    lease, preflight = await guard.acquire_with_target_preflight(make_manifest(), "worker-a")

    assert lease.lease_epoch == 1
    assert len(lease.owner_instance) == 32
    assert preflight.empty
    assert any("set_config('app.tenant_id'" in query for query, _ in connection.executed)
    assert any("pg_advisory_xact_lock" in query for query, _ in connection.fetchvals)
    assert any("FOR UPDATE" in query for query, _ in connection.fetchvals + connection.executed)

    with pytest.raises(MigrationManifestConflict):
        await guard.acquire(make_manifest(source_snapshot_id="different"), "worker-a")


@pytest.mark.asyncio
async def test_expired_lease_takeover_increments_epoch_and_stale_mutations_fail() -> None:
    connection = FakeConnection()
    guard = PostgresMigrationGuard(FakePool(connection))
    old, preflight = await guard.acquire_with_target_preflight(
        make_manifest(), "worker-a", lease_for=timedelta(seconds=2)
    )
    assert preflight.empty
    assert await guard.release(old) is True
    assert (old.tenant_id, old.migration_id) in connection.leases
    assert connection.leases[(old.tenant_id, old.migration_id)]["lease_epoch"] == 1

    replacement = await guard.acquire(make_manifest(), "worker-b")
    assert replacement.lease_epoch == old.lease_epoch + 1
    assert replacement.owner_instance != old.owner_instance
    with pytest.raises(MigrationLeaseLost):
        await guard.renew(old)
    with pytest.raises(MigrationLeaseLost):
        await guard.release(old)
    assert await guard.release(replacement) is True


@pytest.mark.asyncio
async def test_single_active_tenant_and_target_empty_preflight() -> None:
    connection = FakeConnection()
    guard = PostgresMigrationGuard(FakePool(connection))
    first, preflight = await guard.acquire_with_target_preflight(make_manifest(), "worker-a")
    assert preflight.empty
    # A second acquire by the same logical worker is still a duplicate
    # process attempt; only renew may extend the active lease.
    with pytest.raises(Exception, match="active migration lease"):
        await guard.acquire(make_manifest(), "worker-a")
    with pytest.raises(Exception, match="active migration lease"):
        await guard.acquire(
            make_manifest(migration_id="another-migration"),
            "worker-b",
        )
    await guard.release(first)
    other = await guard.acquire(
        make_manifest(migration_id="another-migration"),
        "worker-b",
    )
    assert other.migration_id == "another-migration"

    result = await guard.preflight_target_empty("tenant-1")
    assert result.empty and result.checked_tables == (
        "inbound_messages",
        "outbound_messages",
        "delivery_attempts",
        "sessions",
        "session_turns",
        "turn_intents",
        "session_events",
        "memories",
        "session_summaries",
        "artifacts",
        "knowledge_items",
        "knowledge_embeddings",
        "outbox_events",
        "dead_letters",
        "tool_executions",
        "confirmation_challenges",
        "audit_logs",
        "session_mailboxes",
        "session_mailbox_items",
        "wecom_connection_state",
        "im_acceptance_evidence_events",
        "im_acceptance_runs",
        "migration_checkpoints",
        "migration_scope_manifests",
        "migration_leases",
    )
    target_query = next(
        query for query, _args in connection.history if "migration_protected_target_counts" in query
    )
    assert "public.migration_protected_target_counts($1)" in target_query
    assert "FROM public.wecom_connection_state" not in target_query
    assert "FROM public.im_acceptance_evidence_events" not in target_query
    assert "FROM public.im_acceptance_runs" not in target_query
    connection.target_counts["memories"] = 1
    with pytest.raises(MigrationTargetNotEmpty, match="memories"):
        await guard.target_empty_preflight("tenant-1")
    connection.target_counts["memories"] = 0
    connection.target_counts["im_acceptance_evidence_events"] = 1
    with pytest.raises(MigrationTargetNotEmpty, match="im_acceptance_evidence_events"):
        await guard.target_empty_preflight("tenant-1")
    connection.target_counts["im_acceptance_evidence_events"] = 0
    connection.target_counts["im_acceptance_runs"] = 1
    with pytest.raises(MigrationTargetNotEmpty, match="im_acceptance_runs"):
        await guard.target_empty_preflight("tenant-1")


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_row", [("sessions", 0), ("unexpected_table", 0)])
async def test_target_empty_preflight_rejects_non_exact_table_sets(
    invalid_row: tuple[str, int],
) -> None:
    class InvalidTargetCountConnection(FakeConnection):
        async def fetch(self, query: str, *args: object):
            rows = await super().fetch(query, *args)
            rows.append({"table_name": invalid_row[0], "row_count": invalid_row[1]})
            return rows

    guard = PostgresMigrationGuard(FakePool(InvalidTargetCountConnection()))

    with pytest.raises(MigrationGuardError, match="exact guarded table set"):
        await guard.preflight_target_empty("tenant-1")


@pytest.mark.asyncio
async def test_atomic_target_empty_preflight_and_lease_acquisition_share_lock() -> None:
    connection = FakeConnection()
    guard = PostgresMigrationGuard(FakePool(connection))

    lease, preflight = await guard.acquire_with_target_preflight(make_manifest(), "worker-a")

    assert lease.owner_id == "worker-a"
    assert preflight.empty
    advisory_index = next(
        index
        for index, (query, _args) in enumerate(connection.history)
        if "pg_advisory_xact_lock" in query
    )
    # The target count query must happen after the tenant lock and before the
    # manifest/lease writes.  This is the ordering that closes the
    # preflight-to-write race for production callers.
    queries = [query for query, _args in connection.history]
    target_index = next(index for index, query in enumerate(queries) if "inbound_messages" in query)
    manifest_index = next(
        index
        for index, query in enumerate(queries)
        if "INSERT INTO migration_scope_manifests" in query
    )
    lease_index = next(
        index for index, query in enumerate(queries) if "INSERT INTO migration_leases" in query
    )
    assert advisory_index < target_index < manifest_index < lease_index


@pytest.mark.asyncio
async def test_atomic_target_preflight_rejects_occupied_target_before_lease() -> None:
    connection = FakeConnection()
    connection.target_counts["sessions"] = 1
    guard = PostgresMigrationGuard(FakePool(connection))

    with pytest.raises(MigrationTargetNotEmpty, match="sessions"):
        await guard.acquire_with_target_preflight(make_manifest(), "worker-a")

    assert connection.leases == {}
    assert connection.manifests == {}


@pytest.mark.asyncio
async def test_assert_active_requires_owner_instance_and_fencing_epoch() -> None:
    connection = FakeConnection()
    guard = PostgresMigrationGuard(FakePool(connection))
    lease, preflight = await guard.acquire_with_target_preflight(make_manifest(), "worker-a")
    assert preflight.empty
    await guard.assert_active(lease)
    with pytest.raises(MigrationLeaseLost):
        await guard.assert_active(lease.model_copy(update={"owner_instance": "different-instance"}))
    with pytest.raises(MigrationLeaseLost):
        await guard.assert_active(lease.model_copy(update={"lease_epoch": 2}))


def test_manifest_is_content_free_immutable_and_strict() -> None:
    manifest = make_manifest(record_kinds=("memory", "session"), config_revision=3)
    assert manifest.config_revision == 3
    with pytest.raises((TypeError, ValidationError)):
        manifest.tenant_id = "other"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        MigrationScopeManifest.model_validate({**manifest.model_dump(), "payload": "secret"})
    with pytest.raises(ValidationError):
        make_manifest(kinds=("session", "session"))


def test_alembic_timeout_settings_are_inside_managed_transaction() -> None:
    source = Path("migrations/env.py").read_text(encoding="utf-8")
    online = source.split("def run_migrations_online()", 1)[1]
    configured = online.index("context.configure(")
    transaction = online.index("with context.begin_transaction():")
    lock_timeout = online.index('connection.exec_driver_sql(f"SET lock_timeout', transaction)
    statement_timeout = online.index(
        'connection.exec_driver_sql(f"SET statement_timeout', transaction
    )
    run = online.index("context.run_migrations()", transaction)

    assert configured < transaction < lock_timeout < statement_timeout < run


def test_first_long_revision_widens_alembic_version_column_before_recording_head() -> None:
    source = Path("migrations/versions/0012_migration_lease_owner_instance.py").read_text(
        encoding="utf-8"
    )

    assert len("0012_migration_lease_owner_instance") > 32
    assert source.index(
        "ALTER TABLE alembic_version ALTER COLUMN version_num TYPE varchar(64)"
    ) < source.index("ALTER TABLE migration_leases")
