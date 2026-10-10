from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from tests.conftest import envelope
from trpc_service.agent.fake import DeterministicAgent
from trpc_service.agent.worker import AgentWorker, ProcessStatus
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.memory import InMemoryRuntimeRepository
from trpc_service.storage.migration import (
    InMemoryMigrationCheckpointStore,
    MigrationCoordinator,
    MigrationPhase,
    MigrationRecord,
)
from trpc_service.storage.models import BindingRoute, StoredEvent, TurnCommit
from trpc_service.storage.protocols import FencingConflict
from trpc_service.tenant.models import (
    Channel,
    ChannelBinding,
    ModelPolicy,
    StorageSelection,
    TenantConfig,
)

TENANTS = 4
SESSIONS_PER_TENANT = 5
TURNS_PER_SESSION = 5
NODES = 8
ROUTING_KEY = b"v" * 32


async def _agent_loader(_config: TenantConfig) -> DeterministicAgent:
    return DeterministicAgent(name="virtual-node-agent", response="virtual-response")


def _runtime() -> tuple[InMemoryRuntimeRepository, TenantRuntime, tuple[str, ...]]:
    repository = InMemoryRuntimeRepository()
    binding_ids: list[str] = []
    for index in range(TENANTS):
        tenant_id = f"virtual-tenant-{index}"
        binding_id = f"virtual-binding-{index}"
        config = TenantConfig(
            tenant_id=tenant_id,
            app_id="shared-app",
            version=1,
            model=ModelPolicy(provider="offline", model="deterministic"),
            storage=StorageSelection(profile_id="virtual"),
        )
        binding = ChannelBinding(
            binding_id=binding_id,
            tenant_id=tenant_id,
            app_id=config.app_id,
            channel=Channel.FEISHU,
            account_id="shared-im-account",
        )
        repository.add_config(config)
        repository.add_route(BindingRoute(binding=binding, active_config_version=1))
        binding_ids.append(binding_id)
    return repository, TenantRuntime(repository, routing_key=ROUTING_KEY), tuple(binding_ids)


@pytest.mark.asyncio
async def test_eight_nodes_isolate_four_tenants_under_duplicate_and_out_of_order_load() -> None:
    repository, runtime, binding_ids = _runtime()
    accepted = []
    session_ids: dict[tuple[str, str], str] = {}
    duplicates = []

    # Every tenant intentionally uses identical app/account/user/message identifiers.
    for turn in range(TURNS_PER_SESSION):
        for binding_id in binding_ids:
            for session_index in range(SESSIONS_PER_TENANT):
                inbound = envelope(
                    f"shared-message-{session_index}-{turn}",
                    user_id=f"shared-user-{session_index}",
                    account_id="shared-im-account",
                )
                acceptance = await runtime.accept(binding_id, inbound)
                accepted.append(acceptance)
                session_ids[(acceptance.context.tenant_id, inbound.external_user_id)] = (
                    acceptance.context.session_id
                )
                if turn == 0 and session_index == 0:
                    duplicate = await runtime.accept(binding_id, inbound)
                    assert duplicate.duplicate and duplicate.inbound_id == acceptance.inbound_id
                    duplicates.append(duplicate)

    expected_turns = TENANTS * SESSIONS_PER_TENANT * TURNS_PER_SESSION
    assert len(accepted) == expected_turns
    assert len(set(session_ids.values())) == TENANTS * SESSIONS_PER_TENANT

    # Node 0 dies after acquiring the first turn. Its expired epoch must be fenced
    # while another virtual node takes over the same durable acceptance.
    stale = await repository.acquire(
        acceptance=accepted[0],
        worker_id="virtual-node-killed",
        lease_for=timedelta(milliseconds=1),
    )
    assert stale is not None
    await asyncio.sleep(0.01)

    workers = [
        AgentWorker(
            repository,
            worker_id=f"virtual-node-{index}",
            agent_loader=_agent_loader,
            lease_for=timedelta(seconds=2),
        )
        for index in range(NODES)
    ]
    pending = list(reversed(accepted))
    committed = 0
    busy = 0
    for _ in range(TURNS_PER_SESSION + 3):
        if not pending:
            break
        results = await asyncio.gather(
            *(workers[index % NODES].process(item) for index, item in enumerate(pending))
        )
        next_pending = []
        for acceptance, result in zip(pending, results, strict=True):
            if result.status == ProcessStatus.COMMITTED:
                committed += 1
            else:
                assert result.status == ProcessStatus.BUSY
                busy += 1
                next_pending.append(acceptance)
        pending = next_pending
        await asyncio.sleep(0)

    assert pending == []
    assert committed == expected_turns
    assert busy > 0  # reverse delivery really exercised per-session ordering

    for tenant_index in range(TENANTS):
        tenant_id = f"virtual-tenant-{tenant_index}"
        for session_index in range(SESSIONS_PER_TENANT):
            session_id = session_ids[(tenant_id, f"shared-user-{session_index}")]
            snapshot = await repository.get_session_snapshot(tenant_id, session_id)
            assert snapshot is not None
            assert [event.sequence for event in snapshot.events] == list(
                range(1, TURNS_PER_SESSION * 2 + 1)
            )
            other_tenant = f"virtual-tenant-{(tenant_index + 1) % TENANTS}"
            assert await repository.get_session_snapshot(other_tenant, session_id) is None

    duplicate_results = await asyncio.gather(
        *(workers[index].process(item) for index, item in enumerate(duplicates))
    )
    assert all(result.status == ProcessStatus.DUPLICATE for result in duplicate_results)

    with pytest.raises(FencingConflict):
        await repository.commit(
            TurnCommit(
                context=accepted[0].context,
                lease=stale,
                state={"stale": True},
                events=(
                    StoredEvent(
                        event_id="stale-event",
                        author="stale-node",
                        timestamp=1,
                        event={},
                    ),
                ),
            )
        )


class _PartitionedSource:
    def __init__(self) -> None:
        self.records = {
            tenant_id: tuple(
                MigrationRecord(
                    kind="session",
                    resource_id=str(index),
                    payload={"tenant": tenant_id, "value": index},
                )
                for index in range(7)
            )
            for tenant_id in ("migration-tenant-a", "migration-tenant-b")
        }

    async def fetch(self, tenant_id, *, cursor, limit):
        start = int(cursor or 0)
        records = self.records[tenant_id]
        values = records[start : start + limit]
        end = start + len(values)
        return values, str(end) if end < len(records) else None


class _InterruptibleTarget:
    def __init__(self) -> None:
        self.records = {}
        self.failed_once = False
        self.actions: dict[str, list[str]] = {}

    def _record(self, tenant_id: str, action: str) -> None:
        self.actions.setdefault(tenant_id, []).append(action)

    async def prepare(self, tenant_id):
        self._record(tenant_id, "prepare")

    async def upsert(self, tenant_id, record):
        if tenant_id == "migration-tenant-a" and record.resource_id == "3" and not self.failed_once:
            self.failed_once = True
            raise ConnectionError("virtual target interruption")
        self.records[(tenant_id, record.kind, record.resource_id)] = record

    async def read(self, tenant_id, kind, resource_id):
        return self.records.get((tenant_id, kind, resource_id))

    async def set_dual_write(self, tenant_id, enabled):
        self._record(tenant_id, f"dual:{enabled}")

    async def cutover(self, tenant_id):
        self._record(tenant_id, "cutover")

    async def cleanup(self, tenant_id):
        self._record(tenant_id, "cleanup")

    async def rollback(self, tenant_id):
        self._record(tenant_id, "rollback")


@pytest.mark.asyncio
async def test_two_tenant_migration_resumes_independently_after_target_interruption() -> None:
    source = _PartitionedSource()
    target = _InterruptibleTarget()
    checkpoints = InMemoryMigrationCheckpointStore()
    coordinator = MigrationCoordinator(source, target, checkpoints, batch_size=2)
    tenants = tuple(source.records)

    for tenant_id in tenants:
        await coordinator.run(tenant_id, "virtual-migration", MigrationPhase.PREPARE)

    with pytest.raises(ConnectionError, match="interruption"):
        await coordinator.run("migration-tenant-a", "virtual-migration", MigrationPhase.BACKFILL)
    interrupted = await checkpoints.load("migration-tenant-a", "virtual-migration")
    untouched = await checkpoints.load("migration-tenant-b", "virtual-migration")
    assert interrupted is not None and interrupted.cursor == "2" and not interrupted.completed
    assert untouched is not None and untouched.phase == MigrationPhase.PREPARE

    for tenant_id in tenants:
        for phase in (
            MigrationPhase.BACKFILL,
            MigrationPhase.SHADOW_READ,
            MigrationPhase.DUAL_WRITE,
            MigrationPhase.CUTOVER,
            MigrationPhase.VERIFY,
        ):
            result = await coordinator.run(tenant_id, "virtual-migration", phase)
            assert result.gate == "pass"
            assert result.case_deltas["differences"] == []

    for tenant_id in tenants:
        tenant_records = [key for key in target.records if key[0] == tenant_id]
        assert len(tenant_records) == len(source.records[tenant_id])
    await coordinator.run("migration-tenant-b", "virtual-migration", MigrationPhase.ROLLBACK)
    assert target.actions["migration-tenant-b"][-2:] == ["rollback", "dual:False"]
    assert "rollback" not in target.actions["migration-tenant-a"]
