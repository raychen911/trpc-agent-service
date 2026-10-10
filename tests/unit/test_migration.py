from __future__ import annotations

import asyncio
from argparse import Namespace
from datetime import UTC, datetime, timedelta

import pytest

import trpc_service.storage.migration as migration_module
from trpc_service.storage.migration import (
    InMemoryMigrationCheckpointStore,
    MigrationCoordinator,
    MigrationLease,
    MigrationLeaseLost,
    MigrationManifestConflict,
    MigrationPhase,
    MigrationRecord,
    MigrationScopeManifest,
    MigrationSourceKind,
    MigrationSourceSnapshot,
    PostgresMigrationCheckpointStore,
    PostgresMigrationTarget,
    RedisMigrationSource,
)


class Source:
    def __init__(self) -> None:
        self.records = tuple(
            MigrationRecord(kind="session", resource_id=str(index), payload={"value": index})
            for index in range(5)
        )

    async def fetch(self, tenant_id, *, cursor, limit):
        start = int(cursor or 0)
        values = self.records[start : start + limit]
        next_cursor = str(start + len(values)) if start + len(values) < len(self.records) else None
        return values, next_cursor


class Target:
    def __init__(self) -> None:
        self.records = {}
        self.actions = []

    async def prepare(self, tenant_id):
        self.actions.append("prepare")

    async def upsert(self, tenant_id, record):
        self.records[(tenant_id, record.kind, record.resource_id)] = record

    async def read(self, tenant_id, kind, resource_id):
        return self.records.get((tenant_id, kind, resource_id))

    async def set_dual_write(self, tenant_id, enabled):
        self.actions.append(f"dual:{enabled}")

    async def cutover(self, tenant_id):
        self.actions.append("cutover")

    async def cleanup(self, tenant_id):
        self.actions.append("cleanup")

    async def rollback(self, tenant_id):
        self.actions.append("rollback")


def _source_checksum(records: tuple[MigrationRecord, ...]) -> str:
    checksum = "0" * 64
    for record in records:
        checksum = migration_module._rolling_checksum(checksum, record.checksum)
    return checksum


@pytest.mark.asyncio
async def test_full_migration_state_machine_and_checksum() -> None:
    source = Source()
    target = Target()
    checkpoints = InMemoryMigrationCheckpointStore()
    coordinator = MigrationCoordinator(source, target, checkpoints, batch_size=2)

    for phase in (
        MigrationPhase.PREPARE,
        MigrationPhase.BACKFILL,
        MigrationPhase.SHADOW_READ,
        MigrationPhase.DUAL_WRITE,
        MigrationPhase.CUTOVER,
        MigrationPhase.VERIFY,
        MigrationPhase.CLEANUP,
    ):
        result = await coordinator.run("tenant", "redis-to-pg", phase)
        assert result.gate == "pass"

    checkpoint = await checkpoints.load("tenant", "redis-to-pg")
    assert checkpoint is not None and checkpoint.source_count == 5
    assert checkpoint.checksum != "0" * 64
    assert target.actions == ["prepare", "dual:True", "cutover", "cleanup", "dual:False"]


@pytest.mark.asyncio
async def test_enumerable_target_publishes_observed_target_checksum() -> None:
    source = Source()

    class EnumerableTarget(Target):
        async def list_records(self, tenant_id: str, kind: str, *, limit: int = 10000):
            values = [
                record
                for (row_tenant, row_kind, _), record in self.records.items()
                if row_tenant == tenant_id and row_kind == kind
            ]
            return tuple(sorted(values, key=lambda record: record.resource_id)[:limit])

    target = EnumerableTarget()
    coordinator = MigrationCoordinator(
        source, target, InMemoryMigrationCheckpointStore(), batch_size=2
    )
    prepared = await coordinator.run("tenant", "migration", MigrationPhase.PREPARE)
    assert prepared.case_deltas["target_count"] == 0
    assert prepared.case_deltas["target_checksum"] == "0" * 64

    backfilled = await coordinator.run("tenant", "migration", MigrationPhase.BACKFILL)
    assert backfilled.case_deltas["target_count"] == len(source.records)
    assert backfilled.case_deltas["target_checksum"] == _source_checksum(source.records)


@pytest.mark.asyncio
async def test_mixed_kind_target_checksum_uses_canonical_source_order() -> None:
    class MixedSource:
        records = (
            MigrationRecord(kind="session", resource_id="session-0", payload={"value": 0}),
            MigrationRecord(kind="session", resource_id="session-1", payload={"value": 1}),
            MigrationRecord(kind="memory", resource_id="memory-0", payload={"value": 0}),
            MigrationRecord(kind="memory", resource_id="memory-1", payload={"value": 1}),
        )

        async def fetch(self, tenant_id: str, *, cursor: str | None, limit: int):
            del tenant_id
            start = int(cursor or 0)
            values = self.records[start : start + limit]
            end = start + len(values)
            return values, str(end) if end < len(self.records) else None

    class MixedTarget(Target):
        async def list_records_page(
            self,
            tenant_id: str,
            kind: str,
            *,
            cursor: str | None,
            limit: int,
        ):
            del cursor, limit
            values = [
                record
                for (record_tenant, record_kind, _), record in self.records.items()
                if record_tenant == tenant_id and record_kind == kind
            ]
            # Simulate a backend-specific page order.  The coordinator must
            # normalize it without changing the manifest's kind order.
            return tuple(reversed(values)), None

    source = MixedSource()
    target = MixedTarget()
    coordinator = MigrationCoordinator(
        source,
        target,
        InMemoryMigrationCheckpointStore(),
        batch_size=10,
    )
    await coordinator.run("tenant", "mixed-kinds", MigrationPhase.PREPARE)
    backfilled = await coordinator.run("tenant", "mixed-kinds", MigrationPhase.BACKFILL)

    assert backfilled.case_deltas["target_count"] == len(source.records)
    assert backfilled.case_deltas["target_checksum"] == _source_checksum(source.records)


@pytest.mark.asyncio
async def test_enumerated_target_comparison_reuses_checksums_and_reports_extras() -> None:
    class ShortSource:
        records = (
            MigrationRecord(kind="session", resource_id="session-0", payload={"value": 0}),
            MigrationRecord(kind="session", resource_id="session-1", payload={"value": 1}),
        )

        async def fetch(self, tenant_id: str, *, cursor: str | None, limit: int):
            del tenant_id
            start = int(cursor or 0)
            values = self.records[start : start + limit]
            end = start + len(values)
            return values, str(end) if end < len(self.records) else None

    class ChecksumTarget(Target):
        def __init__(self) -> None:
            super().__init__()
            self.read_calls = 0

        async def read(self, tenant_id, kind, resource_id):
            self.read_calls += 1
            return await super().read(tenant_id, kind, resource_id)

        async def list_records_page(
            self,
            tenant_id: str,
            kind: str,
            *,
            cursor: str | None,
            limit: int,
        ):
            del cursor, limit
            values = [
                record
                for (record_tenant, record_kind, _), record in self.records.items()
                if record_tenant == tenant_id and record_kind == kind
            ]
            return tuple(sorted(values, key=lambda record: record.resource_id)), None

    source = ShortSource()
    target = ChecksumTarget()
    coordinator = MigrationCoordinator(
        source,
        target,
        InMemoryMigrationCheckpointStore(),
        batch_size=10,
    )
    await coordinator.run("tenant", "checksum-reuse", MigrationPhase.PREPARE)
    await coordinator.run("tenant", "checksum-reuse", MigrationPhase.BACKFILL)
    target.records["tenant", "session", "session-1"] = MigrationRecord(
        kind="session", resource_id="session-1", payload={"changed": True}
    )
    target.records["tenant", "session", "session-extra"] = MigrationRecord(
        kind="session", resource_id="session-extra", payload={"value": 3}
    )

    result = await coordinator.run("tenant", "checksum-reuse", MigrationPhase.SHADOW_READ)

    assert result.gate == "fail"
    assert result.case_deltas["target_count"] == 3
    assert result.case_deltas["differences"] == [
        "session/session-1",
        "target-only:session/session-extra",
    ]
    assert target.read_calls == 0


@pytest.mark.asyncio
async def test_manifest_rejects_cursor_complete_checksum_drift() -> None:
    expected = tuple(
        MigrationRecord(kind="session", resource_id=str(index), payload={"value": index})
        for index in range(5)
    )

    class SnapshotSource(Source):
        async def snapshot(self, _tenant_id: str) -> MigrationSourceSnapshot:
            return MigrationSourceSnapshot(
                source_snapshot_id="source-snapshot",
                source_count=len(expected),
                source_checksum=_source_checksum(expected),
            )

        async def fetch(self, tenant_id, *, cursor, limit):
            # Keep the count/cursor valid but alter one record.  A cursor-only
            # implementation would accept this; the manifest checksum must not.
            records = list(expected)
            records[2] = MigrationRecord(
                kind="session", resource_id="2", payload={"value": "changed"}
            )
            start = int(cursor or 0)
            values = tuple(records[start : start + limit])
            end = start + len(values)
            return values, str(end) if end < len(records) else None

    source = SnapshotSource()
    snapshot = await source.snapshot("tenant")
    manifest = MigrationScopeManifest(
        tenant_id="tenant",
        migration_id="manifested",
        source_kind=MigrationSourceKind.REDIS,
        kinds=("session",),
        source_snapshot_id=snapshot.source_snapshot_id,
        source_count=snapshot.source_count,
        source_checksum=snapshot.source_checksum,
        app_id="app",
        config_version=1,
        binding_id="binding",
        binding_revision=1,
    )
    coordinator = MigrationCoordinator(
        source,
        Target(),
        InMemoryMigrationCheckpointStore(),
        batch_size=2,
        manifest=manifest,
    )
    await coordinator.run("tenant", "manifested", MigrationPhase.PREPARE)
    with pytest.raises(MigrationManifestConflict, match="checksum"):
        await coordinator.run("tenant", "manifested", MigrationPhase.BACKFILL)


@pytest.mark.asyncio
async def test_invalid_transition_difference_and_rollback() -> None:
    source = Source()
    target = Target()
    checkpoints = InMemoryMigrationCheckpointStore()
    coordinator = MigrationCoordinator(source, target, checkpoints, batch_size=3)

    with pytest.raises(ValueError, match="start"):
        await coordinator.run("tenant", "migration", MigrationPhase.BACKFILL)
    await coordinator.run("tenant", "migration", MigrationPhase.PREPARE)
    with pytest.raises(ValueError, match="order"):
        await coordinator.run("tenant", "migration", MigrationPhase.CUTOVER)
    await coordinator.run("tenant", "migration", MigrationPhase.BACKFILL)
    target.records[("tenant", "session", "2")] = MigrationRecord(
        kind="session", resource_id="2", payload={"wrong": True}
    )
    shadow = await coordinator.run("tenant", "migration", MigrationPhase.SHADOW_READ)
    assert shadow.gate == "fail"
    await coordinator.run("tenant", "migration", MigrationPhase.ROLLBACK)
    assert target.actions[-2:] == ["rollback", "dual:False"]


@pytest.mark.asyncio
async def test_coordinator_resumes_incomplete_phase_and_rejects_verify_differences() -> None:
    source = Source()
    target = Target()
    checkpoints = InMemoryMigrationCheckpointStore()
    coordinator = MigrationCoordinator(source, target, checkpoints, batch_size=2)
    await checkpoints.save(
        migration_module.MigrationCheckpoint(
            tenant_id="tenant",
            migration_id="resume",
            phase=MigrationPhase.BACKFILL,
            cursor="2",
            source_count=2,
            target_count=2,
            completed=False,
        )
    )
    resumed = await coordinator.run("tenant", "resume", MigrationPhase.BACKFILL)
    assert resumed.gate == "pass" and resumed.case_deltas["source_count"] == 5

    await checkpoints.save(
        migration_module.MigrationCheckpoint(
            tenant_id="tenant",
            migration_id="verify",
            phase=MigrationPhase.CUTOVER,
            completed=True,
        )
    )
    target.records["tenant", "session", "0"] = MigrationRecord(
        kind="session", resource_id="0", payload={"wrong": True}
    )
    with pytest.raises(ValueError, match="verification"):
        await coordinator.run("tenant", "verify", MigrationPhase.VERIFY)

    await checkpoints.save(
        migration_module.MigrationCheckpoint(
            tenant_id="tenant",
            migration_id="blocked",
            phase=MigrationPhase.PREPARE,
            completed=False,
        )
    )
    with pytest.raises(ValueError, match="not completed"):
        await coordinator.run("tenant", "blocked", MigrationPhase.BACKFILL)

    await checkpoints.save(
        migration_module.MigrationCheckpoint(
            tenant_id="tenant",
            migration_id="finished",
            phase=MigrationPhase.CLEANUP,
            completed=True,
        )
    )
    with pytest.raises(ValueError, match="order"):
        await coordinator.run("tenant", "finished", MigrationPhase.PREPARE)


def test_batch_size_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        MigrationCoordinator(Source(), Target(), InMemoryMigrationCheckpointStore(), batch_size=0)

    with pytest.raises(ValueError, match="no greater than"):
        MigrationCoordinator(
            Source(),
            Target(),
            InMemoryMigrationCheckpointStore(),
            batch_size=migration_module.MAX_MIGRATION_BATCH_SIZE + 1,
        )


@pytest.mark.asyncio
async def test_guarded_batch_heartbeats_and_checks_fence_before_each_write() -> None:
    class HeartbeatGuard:
        def __init__(self, *, lose_on_renew: bool = False) -> None:
            self.renew_calls = 0
            self.assert_calls = 0
            self.lose_on_renew = lose_on_renew

        async def renew(self, lease: MigrationLease, *, lease_for: timedelta) -> MigrationLease:
            self.renew_calls += 1
            if self.lose_on_renew:
                raise MigrationLeaseLost("heartbeat fence lost")
            return lease.model_copy(update={"expires_at": datetime.now(UTC) + lease_for})

        async def assert_active(self, lease: MigrationLease) -> None:
            del lease
            self.assert_calls += 1

    class SlowTarget(Target):
        async def upsert(self, tenant_id, record):
            await asyncio.sleep(0.02)
            await super().upsert(tenant_id, record)

    guard = HeartbeatGuard()
    coordinator = MigrationCoordinator(
        Source(),
        SlowTarget(),
        InMemoryMigrationCheckpointStore(),
        batch_size=2,
        guard=guard,  # type: ignore[arg-type]
        lease=MigrationLease(
            tenant_id="tenant",
            migration_id="migration",
            owner_id="worker",
            owner_instance="run-1",
            lease_epoch=1,
            expires_at=datetime.now(UTC) + timedelta(seconds=1),
        ),
        lease_for=timedelta(seconds=0.06),
        heartbeat_interval=timedelta(seconds=0.01),
    )
    await coordinator.run("tenant", "migration", MigrationPhase.PREPARE)
    result = await coordinator.run("tenant", "migration", MigrationPhase.BACKFILL)
    assert result.gate == "pass"
    assert guard.renew_calls >= 2
    assert guard.assert_calls >= len(Source().records)


@pytest.mark.asyncio
async def test_guarded_comparison_checks_fence_once_per_source_page() -> None:
    class Guard:
        def __init__(self) -> None:
            self.assert_calls = 0

        async def renew(self, lease: MigrationLease, *, lease_for: timedelta) -> MigrationLease:
            del lease_for
            return lease

        async def assert_active(self, lease: MigrationLease) -> None:
            del lease
            self.assert_calls += 1

    source = Source()
    target = Target()
    for record in source.records:
        target.records[("tenant", record.kind, record.resource_id)] = record
    guard = Guard()
    coordinator = MigrationCoordinator(
        source,
        target,
        InMemoryMigrationCheckpointStore(),
        batch_size=2,
        guard=guard,  # type: ignore[arg-type]
        lease=MigrationLease(
            tenant_id="tenant",
            migration_id="migration",
            owner_id="worker",
            owner_instance="run-1",
            lease_epoch=1,
            expires_at=datetime.now(UTC) + timedelta(minutes=1),
        ),
    )

    compared = await coordinator._compare_all(
        migration_module.MigrationCheckpoint(
            tenant_id="tenant",
            migration_id="migration",
            phase=MigrationPhase.SHADOW_READ,
        ),
        reject_differences=False,
    )

    assert compared.completed
    assert guard.assert_calls == 3


@pytest.mark.asyncio
async def test_guarded_batch_stops_after_heartbeat_lease_loss() -> None:
    class LostGuard:
        def __init__(self) -> None:
            self.calls = 0

        async def renew(self, lease: MigrationLease, *, lease_for: timedelta) -> MigrationLease:
            self.calls += 1
            if self.calls == 1:
                return lease
            del lease, lease_for
            raise MigrationLeaseLost("lease expired")

        async def assert_active(self, lease: MigrationLease) -> None:
            del lease

    class SlowTarget(Target):
        async def upsert(self, tenant_id, record):
            await asyncio.sleep(0.02)
            await super().upsert(tenant_id, record)

    guard = LostGuard()
    coordinator = MigrationCoordinator(
        Source(),
        SlowTarget(),
        InMemoryMigrationCheckpointStore(),
        batch_size=2,
        guard=guard,  # type: ignore[arg-type]
        lease=MigrationLease(
            tenant_id="tenant",
            migration_id="migration",
            owner_id="worker",
            owner_instance="run-1",
            lease_epoch=1,
            expires_at=datetime.now(UTC) + timedelta(seconds=1),
        ),
        lease_for=timedelta(seconds=0.06),
        heartbeat_interval=timedelta(seconds=0.01),
    )
    await coordinator.run("tenant", "migration", MigrationPhase.PREPARE)
    with pytest.raises(MigrationLeaseLost):
        await coordinator.run("tenant", "migration", MigrationPhase.BACKFILL)

    with pytest.raises(ValueError, match="positive integer"):
        MigrationCoordinator(
            Source(),
            Target(),
            InMemoryMigrationCheckpointStore(),
            batch_size=True,  # type: ignore[arg-type]
        )


class Redis:
    def __init__(self) -> None:
        self.values = {
            "trpc:projection:session:tenant:session-1": (
                "hash",
                {
                    "payload": '{"app_id":"support","principal_id":"user-1",'
                    '"state":{"seen":true},"version":2,"next_sequence":2,'
                    '"events":[{"sequence":1,"id":"event-1","author":"user",'
                    '"timestamp":1.5,"event":{"text":"hello"},"state_delta":{}}]}'
                },
            ),
            "trpc:memory:tenant:memory-1": (
                "string",
                '{"principal_id":"user-1","memory":{"fact":"blue"},"source_sequence":1}',
            ),
        }

    async def scan_iter(self, *, match, count=1000):
        del count
        prefix = match.removesuffix("*")
        for key in sorted(self.values):
            if key.startswith(prefix):
                yield key

    async def type(self, key):
        return self.values[key][0]

    async def get(self, key):
        return self.values[key][1]

    async def hget(self, key, field):
        return self.values[key][1].get(field)

    async def hgetall(self, key):
        return self.values[key][1]


class RecordingRedisPipeline:
    def __init__(self, redis) -> None:
        self.redis = redis
        self.commands = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def type(self, key):
        self.commands.append(("type", key))
        return self

    def get(self, key):
        self.commands.append(("get", key))
        return self

    def hget(self, key, field):
        self.commands.append(("hget", key, field))
        return self

    def hgetall(self, key):
        self.commands.append(("hgetall", key))
        return self

    async def execute(self):
        commands = self.commands
        self.commands = []
        self.redis.execute_batches.append(commands)
        results = []
        for command in commands:
            name, *args = command
            results.append(await getattr(self.redis, name)(*args))
        return results


class PipelinedRedis(Redis):
    def __init__(self) -> None:
        super().__init__()
        self.values["trpc:memory:tenant:memory-hash"] = (
            "hash",
            {"principal_id": "user-1", "memory": '{"fact":"green"}'},
        )
        self.execute_batches = []

    def pipeline(self, *, transaction=True):
        del transaction
        return RecordingRedisPipeline(self)


@pytest.mark.asyncio
async def test_redis_migration_source_is_stable_and_canonical() -> None:
    source = RedisMigrationSource(Redis())
    first, cursor = await source.fetch("tenant", cursor=None, limit=1)
    second, end = await source.fetch("tenant", cursor=cursor, limit=1)

    assert cursor and end is None
    assert first[0].kind == "session"
    assert first[0].resource_id == "session-1"
    assert second[0].kind == "memory"
    assert second[0].resource_id == "memory-1"
    assert second[0].payload["memory"] == {"fact": "blue"}
    assert first[0].payload["events"][0]["event_id"] == "event-1"

    snapshot = await source.snapshot("tenant")
    assert snapshot.source_checksum == migration_module._rolling_checksum(
        migration_module._rolling_checksum("0" * 64, first[0].checksum),
        second[0].checksum,
    )

    resumed, resumed_end = await RedisMigrationSource(Redis()).fetch(
        "tenant", cursor=cursor, limit=1
    )
    assert resumed_end is None and resumed[0].resource_id == "memory-1"


@pytest.mark.asyncio
async def test_redis_pipeline_reads_match_serial_and_bound_round_trips() -> None:
    serial_redis = PipelinedRedis()
    serial_redis.pipeline = None
    serial_source = RedisMigrationSource(serial_redis)
    pipelined_redis = PipelinedRedis()
    pipelined_source = RedisMigrationSource(pipelined_redis)

    serial_records, serial_cursor = await serial_source.fetch("tenant", cursor=None, limit=10)
    pipelined_records, pipelined_cursor = await pipelined_source.fetch(
        "tenant", cursor=None, limit=10
    )
    assert pipelined_records == serial_records
    assert pipelined_cursor == serial_cursor
    assert [batch[0][0] for batch in pipelined_redis.execute_batches] == [
        "type",
        "hget",
        "hgetall",
    ]
    assert all(len(batch) <= 3 for batch in pipelined_redis.execute_batches)

    serial_snapshot = await serial_source.snapshot("tenant")
    pipelined_snapshot = await pipelined_source.snapshot("tenant")
    assert pipelined_snapshot == serial_snapshot
    assert len(pipelined_redis.execute_batches) == 6
    assert all(len(batch) <= 3 for batch in pipelined_redis.execute_batches)


@pytest.mark.asyncio
async def test_redis_source_deduplicates_legacy_and_v2_session_keys() -> None:
    payload = '{"app_id":"support","principal_id":"user-1","state":{},"events":[]}'
    values = {
        "trpc:projection:session:tenant:session-1": ("hash", {"payload": payload}),
        "trpc:projection:session:v2:dGVuYW50.c2Vzc2lvbi0x": ("hash", {"payload": payload}),
    }

    class DualProjectionRedis:
        async def scan_iter(self, *, match, count=1000):
            del count
            prefix = match.removesuffix("*")
            for key in sorted(values):
                if key.startswith(prefix):
                    yield key

        async def type(self, key):
            return values[key][0]

        async def get(self, key):
            return None

        async def hget(self, key, field):
            return values[key][1].get(field)

        async def hgetall(self, key):
            return values[key][1]

    source = RedisMigrationSource(DualProjectionRedis(), kinds=("session",))
    snapshot = await source.snapshot("tenant")
    records, cursor = await source.fetch("tenant", cursor=None, limit=10)
    assert snapshot.source_count == 1
    assert cursor is None
    assert [record.resource_id for record in records] == ["session-1"]


@pytest.mark.asyncio
async def test_redis_source_handles_hash_fallback_none_keys_and_invalid_values() -> None:
    class Variants:
        def __init__(self) -> None:
            self.values = {
                "trpc:memory:tenant:hash": (
                    "hash",
                    {"principal_id": "user", "field": b'"value"'},
                ),
                "trpc:memory:tenant:none": ("none", None),
                "trpc:memory:tenant:bad": ("list", None),
                "trpc:memory:tenant:": ("string", "{}"),
                "trpc:memory:other:ignored": ("string", "{}"),
            }

        async def scan_iter(self, *, match, count=1000):
            del count, match
            for key in sorted(self.values):
                yield key.encode()

        async def type(self, key):
            return self.values[key][0]

        async def get(self, key):
            return self.values[key][1]

        async def hget(self, key, field):
            return self.values[key][1].get(field)

        async def hgetall(self, key):
            return self.values[key][1]

    with pytest.raises(ValueError, match="kinds"):
        RedisMigrationSource(Variants(), kinds=())
    with pytest.raises(ValueError, match="kinds"):
        RedisMigrationSource(Variants(), kinds=("unknown",))
    source = RedisMigrationSource(Variants(), kinds=("memory",))
    with pytest.raises(ValueError, match="positive"):
        await source.fetch("tenant", cursor=None, limit=0)
    with pytest.raises(ValueError, match="unsupported"):
        await source.fetch("tenant", cursor=None, limit=10)

    class NoneOnly(Variants):
        def __init__(self) -> None:
            super().__init__()
            self.values = {"trpc:memory:tenant:none": ("none", None)}

    with pytest.raises(ValueError, match="JSON object"):
        await RedisMigrationSource(NoneOnly(), kinds=("memory",)).fetch(
            "tenant", cursor=None, limit=10
        )

    class HashOnly(Variants):
        def __init__(self) -> None:
            super().__init__()
            self.values.pop("trpc:memory:tenant:bad")
            self.values.pop("trpc:memory:tenant:none")

    records, end = await RedisMigrationSource(HashOnly(), kinds=("memory",)).fetch(
        "tenant", cursor=None, limit=10
    )
    assert end is None and records[0].payload["memory"]["field"] == "value"

    class EmptyHash(HashOnly):
        def __init__(self) -> None:
            super().__init__()
            self.values["trpc:memory:tenant:empty"] = ("hash", {"principal_id": "user"})

        async def hget(self, key, field):
            return None

    empty_records, _ = await RedisMigrationSource(EmptyHash(), kinds=("memory",)).fetch(
        "tenant", cursor=None, limit=10
    )
    assert any(item.resource_id == "empty" for item in empty_records)


def test_migration_cursor_and_payload_normalisation_error_paths() -> None:
    keys = (("memory", "a", "k1"), ("session", "b", "k2"))
    assert migration_module._cursor_position(None, keys) == 0
    assert migration_module._cursor_position("1", keys) == 1
    assert migration_module._parse_cursor(None, upper_bound=len(keys)) == 0
    with pytest.raises(ValueError, match="outside"):
        migration_module._parse_cursor("-1", upper_bound=len(keys))
    with pytest.raises(ValueError, match="non-negative"):
        migration_module._parse_cursor("not-a-number", upper_bound=len(keys))
    with pytest.raises(ValueError, match="invalid"):
        migration_module._cursor_position("-1", keys)
    with pytest.raises(ValueError, match="outside"):
        migration_module._cursor_position("3", keys)
    with pytest.raises(ValueError, match="invalid"):
        migration_module._cursor_position("not-base64", keys)
    malformed = migration_module._encode_source_cursor(("memory", "a"))
    with pytest.raises(ValueError, match="invalid"):
        migration_module._cursor_position(malformed, keys)
    assert (
        migration_module._cursor_position(
            migration_module._encode_source_cursor(("memory", "a", "k1")), keys
        )
        == 1
    )

    assert migration_module._text(b"bytes") == "bytes"
    assert migration_module._text(3) == "3"
    assert migration_module._json_value(b'{"x":1}') == {"x": 1}
    assert migration_module._json_value("not-json") == "not-json"
    with pytest.raises(ValueError, match="JSON column"):
        migration_module._json_object("[]")
    assert migration_module._redis_pattern_literal("a*b?[") == r"a\*b\?\["
    assert migration_module._redis_pattern_literal("a\\b") == "a\\\\b"

    with pytest.raises(ValueError, match="app_id"):
        migration_module._canonical_session_payload("tenant", "session", {})
    with pytest.raises(ValueError, match="principal_id"):
        migration_module._canonical_session_payload("tenant", "session", {"app_id": "app"})
    with pytest.raises(ValueError, match="state"):
        migration_module._canonical_session_payload(
            "tenant", "session", {"app_id": "app", "principal_id": "user", "state": []}
        )
    with pytest.raises(ValueError, match="events"):
        migration_module._canonical_session_payload(
            "tenant", "session", {"app_id": "app", "principal_id": "user", "events": {}}
        )
    with pytest.raises(ValueError, match="non-object"):
        migration_module._canonical_session_payload(
            "tenant",
            "session",
            {"app_id": "app", "principal_id": "user", "events": ["bad"]},
        )
    with pytest.raises(ValueError, match="invalid event"):
        migration_module._canonical_session_payload(
            "tenant",
            "session",
            {"app_id": "app", "principal_id": "user", "events": [{"sequence": 0}]},
        )
    with pytest.raises(ValueError, match="must be an object"):
        migration_module._canonical_session_payload(
            "tenant",
            "session",
            {
                "app_id": "app",
                "principal_id": "user",
                "events": [{"event": [], "state_delta": {}}],
            },
        )
    with pytest.raises(ValueError, match="state_delta"):
        migration_module._canonical_session_payload(
            "tenant",
            "session",
            {
                "app_id": "app",
                "principal_id": "user",
                "events": [{"event": {}, "state_delta": []}],
            },
        )
    with pytest.raises(ValueError, match="precedes"):
        migration_module._canonical_session_payload(
            "tenant",
            "session",
            {
                "app_id": "app",
                "principal_id": "user",
                "next_sequence": 1,
                "events": [{"event": {}}],
            },
        )
    normalized = migration_module._canonical_session_payload(
        "tenant",
        "session",
        {"app_name": "app", "user_id": "user", "events": None},
    )
    assert normalized["events"] == []
    normalized = migration_module._canonical_session_payload(
        "tenant",
        "session",
        {
            "app_id": "app",
            "principal_id": "user",
            "events": [{"timestamp": None, "event": {}, "state_delta": {}}],
        },
    )
    assert normalized["events"][0]["timestamp"] == 0.0
    with pytest.raises(ValueError, match="principal_id"):
        migration_module._canonical_memory_payload({})


@pytest.mark.asyncio
async def test_postgres_target_requires_real_control_hooks_for_cutover() -> None:
    target = PostgresMigrationTarget(Pool(Connection()))
    with pytest.raises(RuntimeError, match="control hook"):
        await target.set_dual_write("tenant", True)
    with pytest.raises(RuntimeError, match="control hook"):
        await target.cutover("tenant")


def test_postgres_target_accepts_lease_renewal_but_rejects_fence_changes() -> None:
    target = PostgresMigrationTarget(Pool(Connection()))
    lease = MigrationLease(
        tenant_id="tenant",
        migration_id="migration",
        owner_id="worker",
        owner_instance="instance",
        lease_epoch=1,
        expires_at=datetime.now(UTC) + timedelta(seconds=30),
    )
    renewed = lease.model_copy(update={"expires_at": datetime.now(UTC) + timedelta(minutes=1)})
    target.bind_migration_lease(lease)
    target.bind_migration_lease(renewed)
    assert target._lease == renewed
    with pytest.raises(MigrationManifestConflict, match="owner or epoch"):
        target.bind_migration_lease(renewed.model_copy(update={"lease_epoch": 2}))


class Connection:
    def __init__(self, row=None, *, fetchval_value=None, fetch_rows=None):
        self.row = row
        self.fetchval_value = fetchval_value
        self.fetch_rows = list(fetch_rows or [])
        self.executed = []

    def transaction(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def execute(self, query, *args):
        self.executed.append((query, args))

    async def fetchrow(self, query, *args):
        return self.row

    async def fetchval(self, query, *args):
        return self.fetchval_value

    async def fetch(self, query, *args):
        return self.fetch_rows


class Pool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return self.connection


class ScriptedConnection(Connection):
    def __init__(self, *, rows=None, values=None, fetch_rows=None):
        super().__init__(fetch_rows=fetch_rows)
        self.rows = list(rows or [])
        self.values = list(values or [])

    async def fetchrow(self, query, *args):
        self.executed.append((query, args))
        return self.rows.pop(0) if self.rows else None

    async def fetchval(self, query, *args):
        self.executed.append((query, args))
        return self.values.pop(0) if self.values else None


class PageConnection(Connection):
    def __init__(self, fetches):
        super().__init__()
        self.fetches = list(fetches)
        self.transaction_enters = 0
        self.transaction_calls = 0

    def transaction(self):
        self.transaction_calls += 1
        return self

    async def __aenter__(self):
        self.transaction_enters += 1
        return self

    async def fetch(self, query, *args):
        self.executed.append((query, args))
        return self.fetches.pop(0)


@pytest.mark.asyncio
async def test_postgres_target_page_batches_session_events_and_memory_rows() -> None:
    session_rows = [
        {
            "session_id": "session-1",
            "app_id": "app",
            "principal_id": "user-1",
            "state_json": '{"seen":true}',
            "version": 2,
            "next_sequence": 2,
        },
        {
            "session_id": "session-2",
            "app_id": "app",
            "principal_id": "user-2",
            "state_json": {},
            "version": 0,
            "next_sequence": 1,
        },
        {
            "session_id": "session-3",
            "app_id": "app",
            "principal_id": "user-3",
            "state_json": {},
            "version": 0,
            "next_sequence": 1,
        },
    ]
    session_events = [
        {
            "session_id": "session-1",
            "sequence": 1,
            "event_id": "event-1",
            "author": "user",
            "event_timestamp": 1.5,
            "event_json": '{"text":"hello"}',
            "state_delta": {},
        }
    ]
    memory_id = "11111111-1111-4111-8111-111111111111"
    memory_rows = [
        {
            "memory_id": memory_id,
            "source_record_id": "memory-1",
            "principal_id": "user-1",
            "session_id": "session-1",
            "source_sequence": 3,
            "memory_json": '{"fact":"blue"}',
            "projection_status": "projected",
        },
        {
            "memory_id": "22222222-2222-4222-8222-222222222222",
            "source_record_id": None,
            "principal_id": "user-2",
            "session_id": None,
            "source_sequence": None,
            "memory_json": {"fact": "green"},
            "projection_status": "pending",
        },
    ]
    connection = PageConnection([session_rows, session_events, memory_rows])
    target = PostgresMigrationTarget(Pool(connection))

    sessions, session_cursor = await target.list_records_page(
        "tenant", "session", cursor=None, limit=2
    )
    memories, memory_cursor = await target.list_records_page(
        "tenant", "memory", cursor=None, limit=1
    )

    assert [record.resource_id for record in sessions] == ["session-1", "session-2"]
    assert sessions[0].payload["events"][0]["event_id"] == "event-1"
    assert session_cursor == "session-2"
    assert memories[0].resource_id == "memory-1"
    assert memories[0].payload["memory"] == {"fact": "blue"}
    assert memory_cursor == memory_id
    assert connection.transaction_calls == 2
    fetch_calls = [(query, args) for query, args in connection.executed if "FROM " in query]
    assert len(fetch_calls) == 3
    assert fetch_calls[0][1] == ("tenant", None, 3)
    assert "FROM session_events" in fetch_calls[1][0]
    assert fetch_calls[1][1] == ("tenant", ["session-1", "session-2"])
    assert "source_record_id" in fetch_calls[2][0]
    assert fetch_calls[2][1] == ("tenant", None, 2)
    assert not any("SELECT session_id FROM" in query for query, _ in fetch_calls)


@pytest.mark.asyncio
async def test_postgres_target_prepare_upsert_read_and_control_hooks_offline() -> None:
    binding = {"binding_id": "binding", "channel": "feishu", "account_id": "account"}
    session = MigrationRecord(
        kind="session",
        resource_id="session-1",
        payload={
            "app_id": "app",
            "principal_id": "user",
            "state": {"seen": True},
            "version": 2,
            "next_sequence": 2,
            "events": [
                {
                    "sequence": 1,
                    "event_id": "event-1",
                    "author": "user",
                    "timestamp": 1.5,
                    "event": {"text": "hello"},
                    "state_delta": {},
                }
            ],
        },
    )
    memory = MigrationRecord(
        kind="memory",
        resource_id="memory-1",
        payload={"principal_id": "user", "memory": {"fact": "blue"}},
    )
    connection = ScriptedConnection(values=[None, 3], rows=[binding])
    target = PostgresMigrationTarget(Pool(connection))
    await target.prepare("tenant")
    missing_target = PostgresMigrationTarget(
        Pool(ScriptedConnection(values=["sessions, memories"]))
    )
    with pytest.raises(RuntimeError, match="missing tables"):
        await missing_target.prepare("tenant")
    await target.upsert("tenant", session)
    await target.upsert(
        "tenant",
        MigrationRecord(
            kind="session",
            resource_id="empty-session",
            payload={"app_id": "app", "principal_id": "user", "events": []},
        ),
    )
    await target.upsert("tenant", memory)
    with pytest.raises(ValueError, match="unsupported"):
        await target.upsert("tenant", MigrationRecord(kind="artifact", resource_id="x", payload={}))

    session_row = {
        "app_id": "app",
        "principal_id": "user",
        "version": 2,
        "next_sequence": 2,
        "state_json": '{"seen":true}',
    }
    event_rows = [
        {
            "sequence": 1,
            "event_id": "event-1",
            "author": "user",
            "event_timestamp": 1.5,
            "event_json": '{"text":"hello"}',
            "state_delta": "{}",
        }
    ]
    read_session = PostgresMigrationTarget(
        Pool(ScriptedConnection(rows=[session_row], fetch_rows=event_rows))
    )
    restored = await read_session.read("tenant", "session", "session-1")
    assert restored is not None and restored.payload["events"][0]["event_id"] == "event-1"
    read_memory = PostgresMigrationTarget(
        Pool(
            ScriptedConnection(
                rows=[
                    {
                        "principal_id": "user",
                        "session_id": None,
                        "source_sequence": None,
                        "memory_json": '{"fact":"blue"}',
                        "projection_status": "pending",
                    }
                ]
            )
        )
    )
    restored_memory = await read_memory.read("tenant", "memory", "memory-1")
    assert restored_memory is not None and restored_memory.payload["memory"] == {"fact": "blue"}
    missing = PostgresMigrationTarget(Pool(ScriptedConnection(rows=[None, None])))
    assert await missing.read("tenant", "session", "missing") is None
    assert await missing.read("tenant", "memory", "missing") is None
    with pytest.raises(ValueError, match="unsupported"):
        await missing.read("tenant", "artifact", "x")

    class Control:
        def __init__(self) -> None:
            self.actions = []

        async def set_dual_write(self, tenant_id, enabled):
            self.actions.append(("dual", tenant_id, enabled))

        async def cutover(self, tenant_id):
            self.actions.append(("cutover", tenant_id))

        async def cleanup(self, tenant_id):
            self.actions.append(("cleanup", tenant_id))

        async def rollback(self, tenant_id):
            self.actions.append(("rollback", tenant_id))

    control = Control()
    controlled = PostgresMigrationTarget(Pool(ScriptedConnection()), control=control)
    await controlled.set_dual_write("tenant", True)
    await controlled.cutover("tenant")
    await controlled.cleanup("tenant")
    await controlled.rollback("tenant")
    assert [item[0] for item in control.actions] == [
        "dual",
        "cutover",
        "cleanup",
        "rollback",
    ]

    with pytest.raises(RuntimeError, match="config revision"):
        await target._migration_context(ScriptedConnection(values=[None]), "tenant", "app")
    with pytest.raises(RuntimeError, match="config revision"):
        await target._migration_context(
            ScriptedConnection(values=[3], rows=[None]), "tenant", "app"
        )


@pytest.mark.asyncio
async def test_postgres_checkpoint_store_round_trips_status_and_checksum() -> None:
    connection = Connection(
        {
            "phase": "backfill",
            "cursor": "4",
            "source_count": 4,
            "target_count": 4,
            "checksum": "a" * 64,
            "differences": ["session/x"],
            "status": "running",
        }
    )
    store = PostgresMigrationCheckpointStore(Pool(connection))
    checkpoint = await store.load("tenant", "migration")
    assert checkpoint is not None
    assert checkpoint.phase == MigrationPhase.BACKFILL
    assert checkpoint.source_count == 4 and not checkpoint.completed
    assert checkpoint.cursor == "4"

    await store.save(checkpoint.model_copy(update={"completed": True}))
    assert any("INSERT INTO migration_checkpoints" in query for query, _ in connection.executed)

    completed_connection = Connection(
        {
            "phase": "verify",
            "cursor": None,
            "source_count": 2,
            "target_count": 2,
            "checksum": None,
            "differences": '["memory/x"]',
            "status": "completed",
        }
    )
    completed = await PostgresMigrationCheckpointStore(Pool(completed_connection)).load(
        "tenant", "migration"
    )
    assert completed is not None and completed.completed
    assert completed.differences == ("memory/x",)
    assert (
        await PostgresMigrationCheckpointStore(Pool(Connection())).load("tenant", "missing") is None
    )


@pytest.mark.asyncio
async def test_live_migration_report_is_not_run_without_explicit_opt_in(
    monkeypatch, tmp_path
) -> None:
    from scripts.migrate_data import _run

    monkeypatch.delenv("TRPC_RUN_REAL_MIGRATION", raising=False)
    output = tmp_path / "migration.json"
    result = await _run(Namespace(output=output, phase="verify", batch_size=1))
    assert result["gate"] == "not_run"
    assert result["production_gate"] == "not_run"
    assert result["rejection_reasons"]
