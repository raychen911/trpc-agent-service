from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

import trpc_service.storage.migration as migration_module
from trpc_service.channels.envelopes import DeliveryReceipt, DeliveryStatus
from trpc_service.storage.migration import (
    InMemoryMigrationCheckpointStore,
    MigrationCheckpoint,
    MigrationCoordinator,
    MigrationGuardError,
    MigrationLease,
    MigrationLeaseConflict,
    MigrationLeaseLost,
    MigrationManifestConflict,
    MigrationPhase,
    MigrationRecord,
    MigrationScopeManifest,
    MigrationSourceKind,
    MigrationSourceSnapshot,
    PostgresMigrationCheckpointStore,
    PostgresMigrationGuard,
    PostgresMigrationTarget,
    RedisMigrationSource,
    TargetEmptyPreflight,
)
from trpc_service.storage.models import OutboxRecord
from trpc_service.storage.postgres import PostgresRuntimeRepository
from trpc_service.storage.services import (
    PostgresArtifactStore,
    PostgresAuditStore,
    PostgresKnowledgeStore,
    PostgresMemoryStore,
    PostgresSummaryStore,
    PostgresTenantServiceFactory,
    ProfileServiceFactory,
    RegisteredTenantServiceBundle,
    TenantDataServices,
    _decode,
)
from trpc_service.tenant.models import (
    AuditPolicy,
    Channel,
    ChannelBinding,
    ModelPolicy,
    StorageSelection,
    TenantConfig,
    TenantContext,
    ToolPolicy,
    ToolRisk,
    validate_model_base_url,
    validate_storage_backend,
)
from trpc_service.tool.confirmation import ConfirmationScope
from trpc_service.tool.execution import ExecutionStatus
from trpc_service.tool.governance import Decision
from trpc_service.tool.postgres import (
    PostgresBudgetLedger,
    PostgresConfirmationLedger,
    PostgresExecutionLedger,
    PostgresGovernanceAuditSink,
    ToolExecutionConflict,
)


class AsyncConnection:
    """Small deterministic asyncpg double used by all tests in this module."""

    def __init__(
        self,
        *,
        rows: list[object] | None = None,
        fetches: list[list[object]] | None = None,
        values: list[object] | None = None,
        executes: list[str] | None = None,
    ) -> None:
        self.rows = list(rows or [])
        self.fetches = list(fetches or [])
        self.values = list(values or [])
        self.executes = list(executes or [])
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self) -> AsyncConnection:
        return self

    async def __aenter__(self) -> AsyncConnection:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, query: str, *args: object) -> str:
        self.calls.append(("execute", (query, *args)))
        return self.executes.pop(0) if self.executes else "UPDATE 1"

    async def fetchrow(self, query: str, *args: object) -> object:
        self.calls.append(("fetchrow", (query, *args)))
        return self.rows.pop(0) if self.rows else None

    async def fetch(self, query: str, *args: object) -> list[object]:
        self.calls.append(("fetch", (query, *args)))
        return self.fetches.pop(0) if self.fetches else []

    async def fetchval(self, query: str, *args: object) -> object:
        self.calls.append(("fetchval", (query, *args)))
        value = self.values.pop(0) if self.values else None
        if isinstance(value, BaseException):
            raise value
        return value


class AsyncAcquire:
    def __init__(self, connection: AsyncConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> AsyncConnection:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class AsyncPool:
    def __init__(self, connection: AsyncConnection) -> None:
        self.connection = connection

    def acquire(self) -> AsyncAcquire:
        return AsyncAcquire(self.connection)


def make_config(
    tenant_id: str = "tenant-a", *, storage: StorageSelection | None = None
) -> TenantConfig:
    return TenantConfig(
        tenant_id=tenant_id,
        app_id="app",
        version=1,
        model=ModelPolicy(provider="fake", model="fake"),
        tools=ToolPolicy(),
        audit=AuditPolicy(),
        storage=storage or StorageSelection(profile_id="profile"),
    )


def make_context(tenant_id: str = "tenant-a") -> TenantContext:
    return TenantContext(
        tenant_id=tenant_id,
        app_id="app",
        config_version=1,
        channel_binding_id="binding",
        principal_id="user",
        session_id="session",
        request_id="request",
        trace_id="trace",
    )


def make_scope() -> ConfirmationScope:
    return ConfirmationScope(
        tenant_id="tenant",
        principal_id="principal",
        session_id="session",
        tool_name="write",
        arguments_hash="a" * 64,
    )


def make_outbox(*, status: str = "pending") -> OutboxRecord:
    return OutboxRecord(
        outbox_id=str(uuid4()),
        tenant_id="tenant",
        event_type="outbound.message",
        aggregate_id=str(uuid4()),
        payload={"status": status},
    )


def active_turn_rows(
    turn_id: str, *, owner: str = "worker", epoch: int = 1
) -> list[dict[str, object]]:
    return [
        {"session_id": "session"},
        {"session_id": "session", "lease_owner": owner, "lease_epoch": epoch},
        {"session_id": "session", "status": "processing", "fencing_token": epoch},
        {"lease_owner": owner, "lease_epoch": epoch, "lease_valid": True},
    ]


def test_model_endpoint_and_backend_validation_boundaries() -> None:
    assert validate_model_base_url("https://api.example.com/v1") == "https://api.example.com/v1"
    assert (
        validate_model_base_url(
            "https://api.example.com",
            allowed_hosts={"API.EXAMPLE.COM."},
            resolved_addresses={"8.8.8.8"},
        )
        == "https://api.example.com"
    )
    invalid = [
        (123, "invalid"),
        ("https://[::1", "invalid"),
        ("http://api.example.com", "HTTPS"),
        ("https:///path", "HTTPS"),
        ("https://user:pass@api.example.com", "userinfo"),
        ("https://api.example.com:99999", "port"),
        ("https://api.example.com?q=1", "query"),
        ("https://localhost", "not allowed"),
        ("https://127.0.0.1", "address"),
        ("https://api.example.com", "registered"),
    ]
    for value, message in invalid:
        kwargs = {"allowed_hosts": {"other.example.com"}} if message == "registered" else {}
        with pytest.raises(ValueError, match=message):
            validate_model_base_url(value, **kwargs)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="DNS result is invalid"):
        validate_model_base_url("https://api.example.com", resolved_addresses={"bad-ip"})
    with pytest.raises(ValueError, match="DNS result is not allowed"):
        validate_model_base_url("https://api.example.com", resolved_addresses={"10.0.0.1"})
    with pytest.raises(ValueError, match="not registered"):
        validate_storage_backend("missing")
    assert validate_storage_backend("custom", registered={"custom"}) == "custom"
    assert (
        StorageSelection(profile_id="p", session_backend="inmemory").session_backend == "inmemory"
    )


def test_model_aliases_and_secret_audit_validation() -> None:
    manifest = MigrationScopeManifest(
        tenant_id="tenant",
        migration_id="migration",
        source_kind=MigrationSourceKind.REDIS,
        record_kinds=["memory", "session"],
        source_snapshot_id="snapshot",
        source_count=0,
        source_checksum="0" * 64,
        app_id="app",
        config_revision=2,
        binding_id="binding",
        binding_revision=1,
    )
    assert manifest.kinds == ("session", "memory") and manifest.config_revision == 2
    with pytest.raises(ValueError, match="unique"):
        MigrationScopeManifest(
            tenant_id="t",
            migration_id="m",
            source_kind="redis",
            kinds=["session", "session"],
            source_snapshot_id="s",
            source_count=0,
            source_checksum="0" * 64,
            app_id="a",
            config_version=1,
            binding_id="b",
            binding_revision=1,
        )
    with pytest.raises(ValueError, match="unsupported"):
        MigrationScopeManifest(
            tenant_id="t",
            migration_id="m",
            source_kind="redis",
            kinds=["bad"],
            source_snapshot_id="s",
            source_count=0,
            source_checksum="0" * 64,
            app_id="a",
            config_version=1,
            binding_id="b",
            binding_revision=1,
        )
    lease = MigrationLease(
        tenant_id="t",
        migration_id="m",
        owner_id="o",
        fencing_token=3,
        expires_at=datetime.now(UTC),
    )
    assert lease.lease_epoch == lease.fencing_token == 3
    with pytest.raises(ValueError, match="content"):
        AuditPolicy(record_content=True)
    with pytest.raises(ValueError, match="secret reference"):
        ChannelBinding(
            binding_id="b",
            tenant_id="t",
            app_id="a",
            channel=Channel.FEISHU,
            account_id="acct",
            secret_refs={"unknown": "secret"},  # type: ignore[dict-item]
        )


@pytest.mark.asyncio
async def test_profile_factory_and_json_decode_edges() -> None:
    bundle = TenantDataServices(*([object()] * 6))  # type: ignore[arg-type]
    selection = make_config().storage
    registration = RegisteredTenantServiceBundle(selection=selection, services=bundle)
    factory = ProfileServiceFactory({("tenant-a", "profile"): registration})
    assert await factory.for_context(make_context(), make_config()) is bundle
    with pytest.raises(ValueError, match="another tenant"):
        await factory.for_context(make_context("other"), make_config())
    with pytest.raises(ValueError, match="registry key"):
        ProfileServiceFactory({"profile": registration})  # type: ignore[dict-item]
    with pytest.raises(LookupError, match="unavailable"):
        await ProfileServiceFactory({}).for_context(make_context(), make_config())
    assert _decode('{"ok":true}') == {"ok": True}
    assert _decode({"ok": True}) == {"ok": True}
    with pytest.raises(ValueError, match="not an object"):
        _decode("[]")


@pytest.mark.asyncio
async def test_memory_summary_and_audit_service_edges() -> None:
    memory_connection = AsyncConnection()
    memory = PostgresMemoryStore(AsyncPool(memory_connection))
    fixed = str(uuid4())
    assert await memory.put("tenant", "principal", {"fact": "x"}, memory_id=fixed)
    with pytest.raises(ValueError, match="between"):
        await memory.list_recent("tenant", "principal", limit=0)
    with pytest.raises(ValueError, match="between"):
        await memory.list_recent("tenant", "principal", limit=1001)
    memory_connection.fetches = [
        [
            {
                "memory_id": fixed,
                "session_id": None,
                "source_sequence": None,
                "memory_json": '{"fact":true}',
                "created_at": datetime.now(UTC),
            },
        ]
    ]
    values = await memory.list_recent("tenant", "principal")
    assert values[0]["memory"] == {"fact": True}
    memory_connection.fetches = [
        [
            {
                "memory_id": fixed,
                "session_id": "s",
                "source_sequence": 2,
                "memory_json": {"fact": "dict"},
                "created_at": "now",
            },
        ]
    ]
    assert (await memory.list_recent("tenant", "principal"))[0]["created_at"] == "now"
    summary_connection = AsyncConnection(
        rows=[None, {"up_to_sequence": 2, "summary_json": "{}", "version": 1}, {"version": 2}, None]
    )
    summary = PostgresSummaryStore(AsyncPool(summary_connection))
    assert await summary.get("tenant", "missing") is None
    assert (await summary.get("tenant", "s")) is not None
    with pytest.raises(ValueError, match="negative"):
        await summary.put("tenant", "s", up_to_sequence=-1, summary={})
    assert await summary.put("tenant", "s", up_to_sequence=2, summary={}, expected_version=1)
    assert not await summary.put("tenant", "s", up_to_sequence=3, summary={})
    audit_connection = AsyncConnection(values=[None, "audit-id"])
    audit = PostgresAuditStore(AsyncPool(audit_connection))
    with pytest.raises(ValueError, match="negative"):
        await audit.append("tenant", decision="x", trace_id="t", cost_units=-1)
    with pytest.raises(RuntimeError, match="id"):
        await audit.append("tenant", decision="x", trace_id="t")
    assert await audit.append("tenant", decision="x", trace_id="t") == "audit-id"


class ObjectStore:
    def __init__(self, *, stage_key: str = "new-key") -> None:
        self.stage_key = stage_key
        self.discarded: list[str] = []
        self.commit_calls = 0
        self.fail_stage = False
        self.fail_discard = False

    async def stage(self, _tenant: str, _artifact: str, _content: bytes, *, checksum: str) -> str:
        if self.fail_stage:
            raise OSError("provider down")
        return self.stage_key

    async def commit(self, _tenant: str, _artifact: str, _staged: str) -> str:
        self.commit_calls += 1
        return "committed-key"

    async def discard(self, key: str) -> None:
        if self.fail_discard:
            raise OSError("cleanup down")
        self.discarded.append(key)


@pytest.mark.asyncio
async def test_artifact_stage_commit_and_cleanup_edges() -> None:
    objects = ObjectStore()
    connection = AsyncConnection(
        rows=[None],
        executes=["INSERT 0 1"],
    )
    store = PostgresArtifactStore(AsyncPool(connection), objects)
    assert await store.stage("tenant", "artifact", b"data", checksum="sum") == "new-key"
    assert objects.discarded == []
    connection.rows = [
        {"checksum": "sum", "object_key": "old-key", "status": "staged", "size_bytes": 1}
    ]
    connection.executes = ["SET", "UPDATE 1"]
    assert await store.stage("tenant", "artifact", b"data", checksum="sum") == "new-key"
    assert objects.discarded == ["old-key"]
    objects.fail_discard = True
    connection.rows = [None]
    connection.executes = ["SET", "UPDATE 0"]
    with pytest.raises(RuntimeError, match="CAS"):
        await store.stage("tenant", "artifact", b"data", checksum="sum")
    objects.fail_discard = False
    objects.fail_stage = True
    connection.rows = [None]
    with pytest.raises(OSError, match="provider"):
        await store.stage("tenant", "artifact", b"data", checksum="sum")

    committed = AsyncConnection(
        rows=[
            {
                "checksum": "sum",
                "object_key": "authoritative",
                "status": "committed",
                "size_bytes": 1,
            }
        ]
    )
    assert (
        await PostgresArtifactStore(AsyncPool(committed), objects).stage(
            "tenant", "artifact", b"data", checksum="sum"
        )
        == "authoritative"
    )
    conflict = AsyncConnection(rows=[{"checksum": "other", "object_key": "k", "status": "staged"}])
    with pytest.raises(ValueError, match="checksum"):
        await PostgresArtifactStore(AsyncPool(conflict), objects).stage(
            "tenant", "artifact", b"data", checksum="sum"
        )
    unavailable = AsyncConnection(
        rows=[{"checksum": "sum", "object_key": "k", "status": "deleted"}]
    )
    with pytest.raises(ValueError, match="available"):
        await PostgresArtifactStore(AsyncPool(unavailable), objects).stage(
            "tenant", "artifact", b"data", checksum="sum"
        )

    missing = AsyncConnection(rows=[None])
    with pytest.raises(LookupError, match="metadata"):
        await PostgresArtifactStore(AsyncPool(missing), objects).commit("tenant", "a", "k")
    mismatch = AsyncConnection(
        rows=[{"checksum": "sum", "object_key": "other", "status": "staged"}]
    )
    with pytest.raises(LookupError, match="current"):
        await PostgresArtifactStore(AsyncPool(mismatch), objects).commit("tenant", "a", "k")
    committed_retry = AsyncConnection(
        rows=[{"checksum": "sum", "object_key": None, "status": "committed"}]
    )
    assert (
        await PostgresArtifactStore(AsyncPool(committed_retry), objects).commit("tenant", "a", "k")
        == "k"
    )
    cas_lost = AsyncConnection(
        rows=[
            {"checksum": "sum", "object_key": "k", "status": "staged"},
            {"checksum": "other", "object_key": "k2", "status": "staged"},
        ],
        executes=["SET", "UPDATE 0"],
    )
    with pytest.raises(RuntimeError, match="CAS"):
        await PostgresArtifactStore(AsyncPool(cas_lost), objects).commit("tenant", "a", "k")
    cas_recovered = AsyncConnection(
        rows=[
            {"checksum": "sum", "object_key": "k", "status": "staged"},
            {"checksum": "sum", "object_key": "authoritative", "status": "committed"},
        ],
        executes=["SET", "UPDATE 0"],
    )
    assert (
        await PostgresArtifactStore(AsyncPool(cas_recovered), objects).commit("tenant", "a", "k")
        == "authoritative"
    )
    await PostgresArtifactStore(AsyncPool(AsyncConnection()), objects).discard_for_tenant(
        "tenant", "a", "k"
    )


@pytest.mark.asyncio
async def test_knowledge_and_factory_selection_edges() -> None:
    with pytest.raises(ValueError, match="exactly 1536"):
        PostgresKnowledgeStore(AsyncPool(AsyncConnection()), profile_id="p", dimension=3)
    knowledge_connection = AsyncConnection()
    knowledge = PostgresKnowledgeStore(AsyncPool(knowledge_connection), profile_id="p")
    with pytest.raises(ValueError, match="dimension"):
        await knowledge.upsert("tenant", "item", [0.0], {})
    await knowledge.upsert("tenant", "item", [0.0] * 1536, {"source_uri": "file"})
    assert len(knowledge_connection.calls) >= 3

    pool = AsyncPool(AsyncConnection())
    artifact_objects = ObjectStore()
    factory = PostgresTenantServiceFactory(pool, artifact_objects=artifact_objects)
    assert (await factory.for_context(make_context(), make_config())).artifact is not None
    with pytest.raises(ValueError, match="another tenant"):
        await factory.for_context(make_context("other"), make_config())
    with pytest.raises(ValueError, match="dimensions"):
        await PostgresTenantServiceFactory(
            pool, artifact_objects=artifact_objects, profile_dimensions={"profile": 2}
        ).for_context(make_context(), make_config())
    with pytest.raises(LookupError, match="object store"):
        await PostgresTenantServiceFactory(pool).for_context(make_context(), make_config())
    with pytest.raises(ValueError, match="cannot satisfy"):
        await PostgresTenantServiceFactory(pool, artifact_objects=artifact_objects).for_context(
            make_context(),
            make_config(storage=StorageSelection(profile_id="profile", memory_backend="redis")),
        )


@pytest.mark.asyncio
async def test_tool_budget_confirmation_and_audit_edges() -> None:
    budget_connection = AsyncConnection(values=[True, None])
    budget = PostgresBudgetLedger(AsyncPool(budget_connection))
    assert await budget.reserve("tenant", token_units=1, cost_units=2, monthly_limit=10)
    assert not await budget.reserve("tenant", token_units=1, cost_units=2, monthly_limit=10)
    with pytest.raises(ValueError, match="negative"):
        await budget.reserve("tenant", token_units=-1, cost_units=0, monthly_limit=10)

    confirmation_connection = AsyncConnection(values=["challenge", None])
    confirmation = PostgresConfirmationLedger(AsyncPool(confirmation_connection))
    scope = make_scope()
    await confirmation.issue("private-token", int(datetime.now(UTC).timestamp()) + 60, scope)
    assert await confirmation.consume("private-token", scope)
    assert not await confirmation.consume("private-token", scope)
    assert "private-token" not in repr(confirmation_connection.calls)

    config = make_config()
    sink_connection = AsyncConnection()
    sink = PostgresGovernanceAuditSink(AsyncPool(sink_connection))
    await sink.record(
        context=make_context(),
        config=config,
        tool_name="read",
        decision=Decision.ALLOW,
        reason="safe",
    )
    assert any("audit_logs" in call[1][0] for call in sink_connection.calls)


@pytest.mark.asyncio
async def test_tool_execution_ledger_terminal_and_fencing_edges() -> None:
    turn_id = str(uuid4())
    succeeded = {
        "turn_id": turn_id,
        "status": ExecutionStatus.SUCCEEDED.value,
        "lease_owner": "worker",
        "lease_epoch": 1,
    }
    idem = AsyncConnection(rows=[succeeded, *active_turn_rows(turn_id)])
    record = await PostgresExecutionLedger(AsyncPool(idem)).begin(
        "key",
        tenant_id="tenant",
        turn_id=turn_id,
        tool_name="read",
        risk=ToolRisk.IDEMPOTENT,
        arguments_hash="h",
        owner_id="worker",
        fencing_token=1,
    )
    assert record.replay_terminal
    non_idem = AsyncConnection(rows=[succeeded])
    record = await PostgresExecutionLedger(AsyncPool(non_idem)).begin(
        "key",
        tenant_id="tenant",
        turn_id=turn_id,
        tool_name="write",
        risk=ToolRisk.NON_IDEMPOTENT,
        arguments_hash="h",
        owner_id="worker",
        fencing_token=1,
    )
    assert record.status == ExecutionStatus.SUCCEEDED

    same = AsyncConnection(
        rows=[
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.STARTED.value,
                "lease_owner": "worker",
                "lease_epoch": 1,
            },
            *active_turn_rows(turn_id),
        ]
    )
    existing = await PostgresExecutionLedger(AsyncPool(same)).begin(
        "key",
        tenant_id="tenant",
        turn_id=turn_id,
        tool_name="read",
        risk=ToolRisk.IDEMPOTENT,
        arguments_hash="h",
        owner_id="worker",
        fencing_token=1,
    )
    assert not existing.fresh

    crossed = AsyncConnection(
        rows=[
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.STARTED.value,
                "lease_owner": "old",
                "lease_epoch": 1,
            },
            *active_turn_rows(turn_id),
        ]
    )
    with pytest.raises(ToolExecutionConflict, match="crossed"):
        await PostgresExecutionLedger(AsyncPool(crossed)).begin(
            "key",
            tenant_id="tenant",
            turn_id=turn_id,
            tool_name="write",
            risk=ToolRisk.NON_IDEMPOTENT,
            arguments_hash="h",
            owner_id="worker",
            fencing_token=1,
        )

    claim_none = AsyncConnection(
        values=[None],
        rows=[
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.STARTED.value,
                "lease_owner": "old",
                "lease_epoch": 1,
            },
            *active_turn_rows(turn_id, epoch=2),
        ],
    )
    with pytest.raises(ToolExecutionConflict, match="changed"):
        await PostgresExecutionLedger(AsyncPool(claim_none)).begin(
            "key",
            tenant_id="tenant",
            turn_id=turn_id,
            tool_name="read",
            risk=ToolRisk.IDEMPOTENT,
            arguments_hash="h",
            owner_id="worker",
            fencing_token=2,
        )
    claim_ok = AsyncConnection(
        values=["key"],
        rows=[
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.FAILED.value,
                "lease_owner": "old",
                "lease_epoch": 1,
            },
            *active_turn_rows(turn_id, epoch=2),
        ],
    )
    assert (
        await PostgresExecutionLedger(AsyncPool(claim_ok)).begin(
            "key",
            tenant_id="tenant",
            turn_id=turn_id,
            tool_name="read",
            risk=ToolRisk.IDEMPOTENT,
            arguments_hash="h",
            owner_id="worker",
            fencing_token=2,
        )
    ).fresh


@pytest.mark.asyncio
async def test_tool_execution_ledger_concurrent_insert_resolution_and_finish() -> None:
    turn_id = str(uuid4())
    # A concurrent insert can resolve to a terminal success or a current same-fence row.
    success_race = AsyncConnection(
        values=[None],
        rows=[
            None,
            *active_turn_rows(turn_id),
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.SUCCEEDED.value,
                "lease_owner": "worker",
                "lease_epoch": 1,
            },
            *active_turn_rows(turn_id),
        ],
    )
    result = await PostgresExecutionLedger(AsyncPool(success_race)).begin(
        "key",
        tenant_id="tenant",
        turn_id=turn_id,
        tool_name="read",
        risk=ToolRisk.IDEMPOTENT,
        arguments_hash="h",
        owner_id="worker",
        fencing_token=1,
    )
    assert result.replay_terminal
    missing_race = AsyncConnection(values=[None], rows=[None, *active_turn_rows(turn_id), None])
    with pytest.raises(RuntimeError, match="disappeared"):
        await PostgresExecutionLedger(AsyncPool(missing_race)).begin(
            "key",
            tenant_id="tenant",
            turn_id=turn_id,
            tool_name="read",
            risk=ToolRisk.IDEMPOTENT,
            arguments_hash="h",
            owner_id="worker",
            fencing_token=1,
        )
    start_race = AsyncConnection(
        values=[None, "key"],
        rows=[
            None,
            *active_turn_rows(turn_id),
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.STARTED.value,
                "lease_owner": "worker",
                "lease_epoch": 1,
            },
            *active_turn_rows(turn_id),
        ],
    )
    same = await PostgresExecutionLedger(AsyncPool(start_race)).begin(
        "key",
        tenant_id="tenant",
        turn_id=turn_id,
        tool_name="read",
        risk=ToolRisk.IDEMPOTENT,
        arguments_hash="h",
        owner_id="worker",
        fencing_token=1,
    )
    assert same.status == ExecutionStatus.STARTED

    finish_missing = AsyncConnection(rows=[None])
    with pytest.raises(RuntimeError, match="does not exist"):
        await PostgresExecutionLedger(AsyncPool(finish_missing)).finish(
            "key",
            tenant_id="tenant",
            status=ExecutionStatus.FAILED,
            owner_id="worker",
            fencing_token=1,
        )
    finish_bad_status = AsyncConnection(
        rows=[{"turn_id": turn_id, "status": "failed", "lease_owner": "worker", "lease_epoch": 1}]
    )
    with pytest.raises(RuntimeError, match="does not exist"):
        await PostgresExecutionLedger(AsyncPool(finish_bad_status)).finish(
            "key",
            tenant_id="tenant",
            status=ExecutionStatus.FAILED,
            owner_id="worker",
            fencing_token=1,
        )
    finish_fence = AsyncConnection(
        rows=[{"turn_id": turn_id, "status": "started", "lease_owner": "old", "lease_epoch": 1}]
    )
    with pytest.raises(ToolExecutionConflict, match="crossed"):
        await PostgresExecutionLedger(AsyncPool(finish_fence)).finish(
            "key",
            tenant_id="tenant",
            status=ExecutionStatus.FAILED,
            owner_id="worker",
            fencing_token=1,
        )
    finish_update = AsyncConnection(
        values=[None],
        rows=[
            {"turn_id": turn_id, "status": "started", "lease_owner": "worker", "lease_epoch": 1},
            *active_turn_rows(turn_id),
        ],
    )
    with pytest.raises(RuntimeError, match="does not exist"):
        await PostgresExecutionLedger(AsyncPool(finish_update)).finish(
            "key",
            tenant_id="tenant",
            status=ExecutionStatus.FAILED,
            owner_id="worker",
            fencing_token=1,
        )
    with pytest.raises(ValueError, match="terminal"):
        await PostgresExecutionLedger(AsyncPool(AsyncConnection())).finish(
            "key",
            tenant_id="tenant",
            status=ExecutionStatus.STARTED,
            owner_id="worker",
            fencing_token=1,
        )


class RedisValues:
    def __init__(self) -> None:
        tenant = base64.urlsafe_b64encode(b"tenant").rstrip(b"=").decode()
        session = base64.urlsafe_b64encode(b"s-v2").rstrip(b"=").decode()
        self.values: dict[str, tuple[str, object]] = {
            f"trpc:projection:session:v2:{tenant}.{session}": (
                "string",
                '{"app_id":"app","principal_id":"user","state":{},"events":[]}',
            ),
            "trpc:memory:tenant:m-hash": (
                "hash",
                {"payload": '{"principal_id":"u","memory":{"x":1}}'},
            ),
            "trpc:memory:tenant:m-fields": ("hash", {"principal_id": '"u"', "memory": '{"x":2}'}),
            "trpc:memory:tenant:m-none": ("none", None),
            "trpc:memory:tenant:m-bad": ("list", None),
            "trpc:memory:other:ignore": ("string", "{}"),
            "trpc:memory:tenant:": ("string", "{}"),
        }

    async def scan_iter(self, *, match: str, count: int = 1000):
        del count
        prefix = match.removesuffix("*").replace("\\*", "*")
        for key in sorted(self.values):
            if key.startswith(prefix) or (
                match.endswith("v2:*") and key.startswith("trpc:projection:session:v2:")
            ):
                yield key.encode()

    async def type(self, key: str) -> str:
        return self.values[key][0]

    async def get(self, key: str) -> object:
        return self.values[key][1]

    async def hget(self, key: str, field: str) -> object:
        value = self.values[key][1]
        return value.get(field) if isinstance(value, dict) else None

    async def hgetall(self, key: str) -> dict[str, object]:
        value = self.values[key][1]
        return value if isinstance(value, dict) else {}


@pytest.mark.asyncio
async def test_redis_source_v2_discovery_snapshot_and_value_shapes() -> None:
    redis = RedisValues()
    source = RedisMigrationSource(redis, kinds=("session",))
    records, cursor = await source.fetch("tenant", cursor=None, limit=2)
    assert len(records) == 1 and cursor is None
    snapshot = await source.snapshot("tenant")
    assert snapshot.source_count == 1
    # Cache path and both hash payload branches are exercised on the second read.
    assert await source._tenant_keys("tenant")
    assert await source._read_value("trpc:memory:tenant:m-hash") == {
        "principal_id": "u",
        "memory": {"x": 1},
    }
    assert await source._read_value("trpc:memory:tenant:m-fields") == {
        "principal_id": "u",
        "memory": {"x": 2},
    }
    assert await source._read_value("trpc:memory:tenant:m-none") is None
    with pytest.raises(ValueError, match="unsupported"):
        await source._read_value("trpc:memory:tenant:m-bad")
    with pytest.raises(ValueError, match="JSON object"):
        await source._canonical_payload("memory", "tenant", "none", None)
    with pytest.raises(ValueError, match="positive"):
        await source.fetch("tenant", cursor=None, limit=0)


def valid_manifest(*, app_id: str = "app") -> MigrationScopeManifest:
    return MigrationScopeManifest(
        tenant_id="tenant",
        migration_id="migration",
        source_kind="redis",
        kinds=("memory", "session"),
        source_snapshot_id="snapshot",
        source_count=1,
        source_checksum="0" * 64,
        app_id=app_id,
        app_revision=1,
        config_version=1,
        binding_id="binding",
        binding_revision=1,
    )


def lease_row(
    *, owner: str = "owner", epoch: int = 1, expires: object | None = None
) -> dict[str, object]:
    return {
        "tenant_id": "tenant",
        "migration_id": "migration",
        "owner_id": owner,
        "owner_instance": "instance",
        "lease_epoch": epoch,
        "expires_at": expires or datetime.now(UTC) + timedelta(minutes=2),
    }


@pytest.mark.asyncio
async def test_migration_target_scope_context_and_record_paths() -> None:
    manifest = valid_manifest()
    session = MigrationRecord(
        kind="session",
        resource_id="session",
        payload={
            "app_id": "app",
            "principal_id": "user",
            "state": {},
            "events": [
                {
                    "sequence": 1,
                    "event_id": "event",
                    "author": "agent",
                    "timestamp": 1.0,
                    "event": {"x": 1},
                    "state_delta": {},
                }
            ],
        },
    )
    target_connection = AsyncConnection(
        values=[None, 1, 1],
        rows=[
            {"binding_id": "binding", "channel": "feishu", "account_id": "acct"},
            {"binding_id": "binding", "channel": "feishu", "account_id": "acct"},
        ],
    )
    target = PostgresMigrationTarget(AsyncPool(target_connection), manifest=manifest)
    await target.prepare("tenant")
    await target.upsert("tenant", session)
    with pytest.raises(MigrationManifestConflict, match="tenant"):
        await target.upsert("other", session)
    with pytest.raises(MigrationManifestConflict, match="kind"):
        await target.upsert("tenant", MigrationRecord(kind="artifact", resource_id="x", payload={}))
    with pytest.raises(MigrationManifestConflict, match="app_id"):
        await target.upsert(
            "tenant", MigrationRecord(kind="session", resource_id="x", payload={"app_id": "other"})
        )

    mismatch_target = PostgresMigrationTarget(
        AsyncPool(AsyncConnection(rows=[{"app_id": "other"}])), manifest=manifest
    )
    assert await mismatch_target.read("tenant", "session", "x") is None
    with pytest.raises(ValueError, match="manifest"):
        await target.read("tenant", "artifact", "x")
    with pytest.raises(ValueError, match="limit"):
        await target.list_records("tenant", "session", limit=0)
    with pytest.raises(ValueError, match="unsupported"):
        await target.list_records("tenant", "artifact")

    list_connection = AsyncConnection(
        rows=[
            {
                "app_id": "app",
                "principal_id": "u",
                "version": 1,
                "next_sequence": 1,
                "state_json": "{}",
            }
        ],
        fetches=[[{"session_id": "session"}], []],
    )
    listed = await PostgresMigrationTarget(AsyncPool(list_connection)).list_records(
        "tenant", "session"
    )
    assert listed and listed[0].resource_id == "session"
    memory_id = str(uuid4())
    memory_connection = AsyncConnection(
        rows=[
            None,
            {
                "principal_id": "u",
                "session_id": None,
                "source_sequence": None,
                "memory_json": "{}",
                "projection_status": "projected",
                "source_record_id": None,
            },
        ],
    )
    restored = await PostgresMigrationTarget(AsyncPool(memory_connection)).read(
        "tenant", "memory", memory_id
    )
    assert restored is not None
    with pytest.raises(RuntimeError, match="config revision"):
        await target._migration_context(AsyncConnection(values=[None]), "tenant", "app")
    with pytest.raises(MigrationManifestConflict, match="outside"):
        await target._migration_context(AsyncConnection(), "other", "app")


@pytest.mark.asyncio
async def test_checkpoint_store_and_migration_guard_lifecycle_edges() -> None:
    checkpoint_row = {
        "phase": "verify",
        "cursor": None,
        "source_count": 2,
        "target_count": 2,
        "checksum": None,
        "differences": "[]",
        "status": "completed",
    }
    checkpoint_connection = AsyncConnection(rows=[checkpoint_row, None])
    store = PostgresMigrationCheckpointStore(AsyncPool(checkpoint_connection))
    loaded = await store.load("tenant", "migration")
    assert loaded is not None and loaded.completed and loaded.checksum == "0" * 64
    assert await store.load("tenant", "missing") is None
    await store.save(
        MigrationCheckpoint(
            tenant_id="tenant", migration_id="migration", phase=MigrationPhase.VERIFY
        )
    )

    with pytest.raises(ValueError, match="between"):
        PostgresMigrationGuard(AsyncPool(AsyncConnection()), max_lease_for=timedelta(0))
    manifest = valid_manifest()
    stored = lease_row()
    target_rows = [
        {"table_name": table_name, "row_count": 0}
        for table_name in migration_module._TARGET_EMPTY_TABLES
    ]
    new_guard_connection = AsyncConnection(
        rows=[manifest.model_dump(), None, stored, {"tenant_id": "tenant"}],
        fetches=[target_rows],
        values=[None],
    )
    guard = PostgresMigrationGuard(AsyncPool(new_guard_connection))
    acquired, target_preflight = await guard.acquire_with_target_preflight(
        manifest, "owner", lease_for=timedelta(seconds=10)
    )
    assert acquired.owner_id == "owner"
    assert target_preflight.empty
    with pytest.raises(MigrationLeaseConflict):
        await PostgresMigrationGuard(
            AsyncPool(
                AsyncConnection(
                    rows=[manifest.model_dump(), lease_row(owner="other")], values=[None]
                )
            )
        ).acquire(manifest, "owner")
    renewed_row = lease_row()
    same_connection = AsyncConnection(
        rows=[manifest.model_dump(), lease_row(), renewed_row], values=[None]
    )
    with pytest.raises(MigrationLeaseConflict, match="active migration lease"):
        await PostgresMigrationGuard(AsyncPool(same_connection)).acquire(manifest, "owner")
    lost_connection = AsyncConnection(
        rows=[manifest.model_dump(), lease_row(), None], values=[None]
    )
    with pytest.raises(MigrationLeaseConflict, match="active migration lease"):
        await PostgresMigrationGuard(AsyncPool(lost_connection)).acquire(manifest, "owner")
    cas_connection = AsyncConnection(rows=[manifest.model_dump(), None, None], values=[None])
    with pytest.raises(MigrationLeaseConflict, match="CAS"):
        await PostgresMigrationGuard(AsyncPool(cas_connection)).acquire(manifest, "owner")

    renew_connection = AsyncConnection(
        rows=[
            lease_row(),
        ],
        values=[None],
    )
    # renew and release use their own transaction and lock paths.
    renewed = await PostgresMigrationGuard(AsyncPool(renew_connection)).renew(acquired)
    assert renewed.owner_id == "owner"
    release_connection = AsyncConnection(
        rows=[{"tenant_id": "tenant"}, {"tenant_id": "tenant"}], values=[None]
    )
    assert await PostgresMigrationGuard(AsyncPool(release_connection)).release(acquired)
    with pytest.raises(MigrationLeaseLost):
        await PostgresMigrationGuard(AsyncPool(AsyncConnection(rows=[None], values=[None]))).renew(
            acquired
        )
    with pytest.raises(MigrationLeaseLost):
        await PostgresMigrationGuard(
            AsyncPool(AsyncConnection(rows=[None], values=[None]))
        ).release(acquired)


class TinySource:
    def __init__(self, *, snapshot: MigrationSourceSnapshot | None = None) -> None:
        self.records = [MigrationRecord(kind="memory", resource_id="m", payload={"x": 1})]
        self.snapshot_value = snapshot

    async def snapshot(self, _tenant: str) -> MigrationSourceSnapshot:
        if self.snapshot_value is None:
            raise AssertionError("snapshot not configured")
        return self.snapshot_value

    async def fetch(self, _tenant: str, *, cursor: str | None, limit: int):
        del limit
        if cursor is None:
            return tuple(self.records), None
        return (), None


class TinyTarget:
    def __init__(self) -> None:
        self.records: dict[str, MigrationRecord] = {}
        self.actions: list[object] = []

    async def prepare(self, _tenant: str) -> None:
        self.actions.append("prepare")

    async def upsert(self, _tenant: str, record: MigrationRecord) -> None:
        self.records[record.resource_id] = record

    async def read(self, _tenant: str, _kind: str, resource_id: str) -> MigrationRecord | None:
        return self.records.get(resource_id)

    async def set_dual_write(self, _tenant: str, enabled: bool) -> None:
        self.actions.append(("dual", enabled))

    async def cutover(self, _tenant: str) -> None:
        self.actions.append("cutover")

    async def cleanup(self, _tenant: str) -> None:
        self.actions.append("cleanup")

    async def rollback(self, _tenant: str) -> None:
        self.actions.append("rollback")


class NoSnapshotSource:
    async def fetch(self, _tenant: str, *, cursor: str | None, limit: int):
        del cursor, limit
        return (), None


@pytest.mark.asyncio
async def test_migration_manifest_target_and_compare_validation_edges() -> None:
    """Exercise guarded migration rejection and target-only comparison branches."""

    manifest = valid_manifest()
    unsupported = manifest.model_copy(update={"kinds": ("artifact",)})
    with pytest.raises(ValueError, match="does not implement"):
        await PostgresMigrationTarget(AsyncPool(AsyncConnection()), manifest=unsupported).prepare(
            "tenant"
        )

    target = PostgresMigrationTarget(
        AsyncPool(
            AsyncConnection(
                fetches=[
                    [],
                    [{"memory_id": "missing", "source_record_id": None}],
                ],
                rows=[None],
            )
        ),
        manifest=manifest,
    )
    # A manifest-scoped session uses the app-filtered query; memory exercises
    # the alternate target enumeration and the read-none filtering path.
    assert await target.list_records("tenant", "session") == ()
    assert await target.list_records("tenant", "memory") == ()
    with pytest.raises(MigrationManifestConflict, match="record kind"):
        target._validate_record_scope(
            "tenant", MigrationRecord(kind="artifact", resource_id="x", payload={})
        )
    target._validate_record_scope(
        "tenant", MigrationRecord(kind="memory", resource_id="m", payload={})
    )

    class ListedTarget(TinyTarget):
        async def list_records(self, _tenant: str, kind: str):
            if kind == "session":
                return ()
            return (MigrationRecord(kind="memory", resource_id="extra", payload={"x": 2}),)

        async def read(self, _tenant: str, kind: str, resource_id: str):
            if kind == "memory" and resource_id == "m":
                return MigrationRecord(kind="memory", resource_id="m", payload={"x": 1})
            return None

    source = TinySource(
        snapshot=MigrationSourceSnapshot(
            source_snapshot_id="snapshot", source_count=1, source_checksum="0" * 64
        )
    )
    compare_manifest = manifest.model_copy(
        update={"source_snapshot_id": "snapshot", "source_count": 1, "source_checksum": "0" * 64}
    )
    checkpoint = MigrationCheckpoint(
        tenant_id="tenant", migration_id="migration", phase=MigrationPhase.SHADOW_READ
    )
    compared = await MigrationCoordinator(
        source,
        ListedTarget(),
        InMemoryMigrationCheckpointStore(),
        manifest=compare_manifest,
    )._compare_all(checkpoint, reject_differences=False)
    assert "target-only:memory/extra" in compared.differences

    wrong_source = TinySource(
        snapshot=MigrationSourceSnapshot(
            source_snapshot_id="changed", source_count=1, source_checksum="1" * 64
        )
    )
    coordinator = MigrationCoordinator(
        wrong_source,
        TinyTarget(),
        InMemoryMigrationCheckpointStore(),
        manifest=compare_manifest,
    )
    with pytest.raises(MigrationManifestConflict, match="source changed"):
        await coordinator._validate_source_snapshot("tenant")

    class RenewGuard:
        async def renew(self, lease: MigrationLease, *, lease_for: timedelta) -> MigrationLease:
            del lease_for
            return lease

    lease = MigrationLease(
        tenant_id="tenant",
        migration_id="migration",
        owner_id="owner",
        lease_epoch=1,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    guarded = MigrationCoordinator(
        source,
        TinyTarget(),
        InMemoryMigrationCheckpointStore(),
        guard=RenewGuard(),  # type: ignore[arg-type]
        lease=lease,
        manifest=compare_manifest,
    )
    await guarded._save_checkpoint(checkpoint)

    assert MigrationScopeManifest._accept_stable_field_aliases(None) is None
    assert MigrationLease._accept_fencing_alias(None) is None

    guard = PostgresMigrationGuard(
        AsyncPool(AsyncConnection(fetches=[[{"table_name": "sessions", "row_count": 0}]]))
    )
    with pytest.raises(MigrationGuardError, match="did not return"):
        await guard.preflight_target_empty("tenant")
    with pytest.raises(MigrationGuardError, match="not persisted"):
        await guard._ensure_manifest(AsyncConnection(rows=[None]), manifest)
    with pytest.raises(ValueError, match="safety bound"):
        guard._validate_lease_for(timedelta(days=1))

    class RowLike:
        def __getitem__(self, key: str) -> object:
            return {"value": 1}[key]

    assert migration_module._row_value(RowLike(), "value") == 1
    encoded_other = base64.urlsafe_b64encode(b"other").decode().rstrip("=")
    encoded_session = base64.urlsafe_b64encode(b"session").decode().rstrip("=")
    assert (
        migration_module._decode_projection_v2_key(
            f"trpc:projection:session:v2:{encoded_other}.{encoded_session}", "tenant"
        )
        is None
    )


@pytest.mark.asyncio
async def test_coordinator_manifest_snapshot_and_phase_edges() -> None:
    record = MigrationRecord(kind="memory", resource_id="m", payload={"x": 1})
    source_checksum = migration_module._rolling_checksum("0" * 64, record.checksum)
    snapshot = MigrationSourceSnapshot(
        source_snapshot_id=record.checksum, source_count=1, source_checksum=source_checksum
    )
    manifest = valid_manifest()
    manifest = manifest.model_copy(
        update={
            "source_snapshot_id": snapshot.source_snapshot_id,
            "source_checksum": snapshot.source_checksum,
        }
    )
    source = TinySource(snapshot=snapshot)
    target = TinyTarget()
    checkpoints = InMemoryMigrationCheckpointStore()
    coordinator = MigrationCoordinator(source, target, checkpoints, manifest=manifest)
    with pytest.raises(MigrationManifestConflict, match="outside"):
        await coordinator.run("other", "migration", MigrationPhase.PREPARE)
    prepared = await coordinator.run("tenant", "migration", MigrationPhase.PREPARE)
    assert prepared.gate == "pass"
    backed = await coordinator.run("tenant", "migration", MigrationPhase.BACKFILL)
    assert backed.gate == "pass"
    await coordinator.run("tenant", "migration", MigrationPhase.SHADOW_READ)
    await coordinator.run("tenant", "migration", MigrationPhase.DUAL_WRITE)
    await coordinator.run("tenant", "migration", MigrationPhase.CUTOVER)
    await coordinator.run("tenant", "migration", MigrationPhase.VERIFY)
    await coordinator.run("tenant", "migration", MigrationPhase.CLEANUP)
    assert "cleanup" in target.actions

    with pytest.raises(ValueError, match="guard and lease"):
        MigrationCoordinator(source, target, checkpoints, guard=object())  # type: ignore[arg-type]
    with pytest.raises(MigrationManifestConflict, match="lease"):
        MigrationCoordinator(
            source,
            target,
            checkpoints,
            manifest=manifest,
            lease=MigrationLease(
                tenant_id="other",
                migration_id="migration",
                owner_id="o",
                lease_epoch=1,
                expires_at=datetime.now(UTC),
            ),
            guard=object(),  # type: ignore[arg-type]
        )
    with pytest.raises(MigrationGuardError, match="snapshot"):
        await MigrationCoordinator(
            NoSnapshotSource(),
            target,
            checkpoints,
            manifest=manifest,  # type: ignore[arg-type]
        )._validate_source_snapshot("tenant")


def test_migration_helpers_and_preflight_model_edges() -> None:
    assert TargetEmptyPreflight(
        tenant_id="t", checked_tables=("a",), table_counts={"a": 0}, non_empty_tables=()
    ).empty
    assert (
        migration_module._lease_from_row(
            {
                "tenant_id": "t",
                "migration_id": "m",
                "owner_id": "o",
                "lease_epoch": 1,
                "expires_at": "2026-01-01T00:00:00",
            }
        ).expires_at.tzinfo
        == UTC
    )
    assert migration_module._manifest_from_row(
        {**valid_manifest().model_dump(), "kinds": json.dumps(["session", "memory"])}
    ).kinds == ("session", "memory")
    with pytest.raises(ValueError, match="1-256"):
        migration_module._validate_identifier("", "id")
    with pytest.raises(ValueError, match="1-256"):
        migration_module._validate_identifier("x" * 257, "id")
    assert migration_module._decode_projection_v2_key("not-v2", "tenant") is None
    assert (
        migration_module._decode_projection_v2_key("trpc:projection:session:v2:a.", "tenant")
        is None
    )
    assert (
        migration_module._decode_projection_v2_key(
            "trpc:projection:session:v2:not-base64.bad", "tenant"
        )
        is None
    )
    assert migration_module._json_value(1) == 1


@pytest.mark.asyncio
async def test_postgres_outbox_claim_and_delivery_state_edges() -> None:
    record = make_outbox()
    row = {
        "outbox_id": record.outbox_id,
        "tenant_id": "tenant",
        "event_type": "outbound.message",
        "aggregate_id": record.aggregate_id,
        "payload_json": "{}",
        "trace_headers": "{}",
        "attempts": 1,
    }
    claimed = AsyncConnection(fetches=[[row]])
    repository = __import__(
        "trpc_service.storage.postgres", fromlist=["PostgresRuntimeRepository"]
    ).PostgresRuntimeRepository(AsyncPool(claimed))
    assert (
        await repository.claim_outbox(
            event_type="outbound.message",
            owner_id="worker",
            limit=1,
            lease_for=timedelta(seconds=10),
        )
    )[0].outbox_id == record.outbox_id
    mark = AsyncConnection(executes=["SET", "UPDATE 1"])
    await repository.__class__(AsyncPool(mark)).mark_outbox_published(
        "tenant", record.outbox_id, owner_id="worker"
    )
    mark_bad = AsyncConnection(executes=["SET", "UPDATE 0"])
    with pytest.raises(Exception, match="claim"):
        await repository.__class__(AsyncPool(mark_bad)).mark_outbox_published(
            "tenant", record.outbox_id, owner_id="worker"
        )
    release_bad = AsyncConnection(executes=["SET", "UPDATE 0"])
    with pytest.raises(Exception, match="claim"):
        await repository.__class__(AsyncPool(release_bad)).release_outbox(
            "tenant",
            record.outbox_id,
            owner_id="worker",
            delay=timedelta(seconds=1),
            error_type="down",
        )
    release = AsyncConnection(executes=["SET", "UPDATE 1"])
    await repository.__class__(AsyncPool(release)).release_outbox(
        "tenant", record.outbox_id, owner_id="worker", delay=timedelta(seconds=1), error_type="down"
    )
    dead_bad = AsyncConnection(executes=["SET", "UPDATE 0"])
    with pytest.raises(Exception, match="claim"):
        await repository.__class__(AsyncPool(dead_bad)).dead_letter_outbox(
            record, owner_id="worker", reason="bad"
        )
    dead = AsyncConnection(executes=["SET", "UPDATE 1", "OK"])
    await repository.__class__(AsyncPool(dead)).dead_letter_outbox(
        record, owner_id="worker", reason="bad"
    )

    receipt = DeliveryReceipt(
        outbound_id=record.aggregate_id, status=DeliveryStatus.DELIVERED, provider_message_id="p"
    )
    delivery = AsyncConnection(values=[1], executes=["SET", "OK", "UPDATE 1", "OK"])
    await repository.__class__(AsyncPool(delivery)).record_delivery("tenant", receipt)
    retry_receipt = DeliveryReceipt(
        outbound_id=record.aggregate_id,
        status=DeliveryStatus.FAILED,
        retryable=True,
        provider_code="busy",
    )
    delivery_retry = AsyncConnection(values=[None])
    with pytest.raises(LookupError, match="outbound"):
        await repository.__class__(AsyncPool(delivery_retry)).record_delivery(
            "tenant", retry_receipt
        )


@pytest.mark.asyncio
async def test_postgres_begin_and_finish_delivery_all_terminal_paths() -> None:
    record = make_outbox()
    owner = "worker"
    outbox_row = {"claimed_by": owner, "published_at": None}
    outbound_row = {"status": "pending"}
    begin_conn = AsyncConnection(
        rows=[outbox_row, outbound_row], values=[1], executes=["SET", "OK", "UPDATE 1"]
    )
    attempt = await PostgresRuntimeRepository(AsyncPool(begin_conn)).begin_delivery(
        record, owner_id=owner
    )
    assert attempt.attempt_number == 1
    for status, message in (
        ("sending", "still unresolved"),
        ("delivered", "already delivered"),
        ("ambiguous", "manual replay"),
    ):
        values = [2] if status == "sending" else []
        with pytest.raises(Exception, match=message):
            await PostgresRuntimeRepository(
                AsyncPool(AsyncConnection(rows=[outbox_row, {"status": status}], values=values))
            ).begin_delivery(record, owner_id=owner)
    with pytest.raises(Exception, match="claim"):
        await PostgresRuntimeRepository(AsyncPool(AsyncConnection(rows=[None]))).begin_delivery(
            record, owner_id=owner
        )
    with pytest.raises(Exception, match="does not exist"):
        await PostgresRuntimeRepository(
            AsyncPool(AsyncConnection(rows=[outbox_row, None]))
        ).begin_delivery(record, owner_id=owner)

    delivered = DeliveryReceipt(
        outbound_id=record.aggregate_id, status=DeliveryStatus.DELIVERED, provider_message_id="p"
    )
    finish_rows = [outbox_row, {"status": "sending"}]
    finish_conn = AsyncConnection(
        rows=finish_rows, executes=["SET", "UPDATE 1", "UPDATE 1", "UPDATE 1"]
    )
    await PostgresRuntimeRepository(AsyncPool(finish_conn)).finish_delivery(
        record, owner_id=owner, attempt_number=1, receipt=delivered
    )
    failed_retry = DeliveryReceipt(
        outbound_id=record.aggregate_id,
        status=DeliveryStatus.FAILED,
        retryable=True,
        provider_code="busy",
        retry_after_seconds=2.5,
    )
    retry_conn = AsyncConnection(
        rows=finish_rows, executes=["SET", "UPDATE 1", "UPDATE 1", "UPDATE 1"]
    )
    await PostgresRuntimeRepository(AsyncPool(retry_conn)).finish_delivery(
        record, owner_id=owner, attempt_number=1, receipt=failed_retry
    )
    attempt_update = next(
        call
        for call in retry_conn.calls
        if call[0] == "execute" and "UPDATE delivery_attempts" in str(call[1][0])
    )
    assert attempt_update[1][-1] == 2.5
    failed_final = DeliveryReceipt(
        outbound_id=record.aggregate_id, status=DeliveryStatus.AMBIGUOUS, provider_code="unknown"
    )
    final_conn = AsyncConnection(
        rows=finish_rows, executes=["SET", "UPDATE 1", "UPDATE 1", "UPDATE 1", "OK"]
    )
    await PostgresRuntimeRepository(AsyncPool(final_conn)).finish_delivery(
        record, owner_id=owner, attempt_number=1, receipt=failed_final
    )
    with pytest.raises(ValueError, match="does not match"):
        await PostgresRuntimeRepository(AsyncPool(AsyncConnection())).finish_delivery(
            record,
            owner_id=owner,
            attempt_number=1,
            receipt=DeliveryReceipt(outbound_id=str(uuid4()), status=DeliveryStatus.DELIVERED),
        )
    with pytest.raises(ValueError, match="non-negative"):
        await PostgresRuntimeRepository(AsyncPool(AsyncConnection())).finish_delivery(
            record,
            owner_id=owner,
            attempt_number=1,
            receipt=delivered,
            retry_delay=timedelta(seconds=-1),
        )
