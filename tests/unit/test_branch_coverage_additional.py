from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

import trpc_service.agent.wecom_manager as wecom_manager
import trpc_service.storage.production_migration_control as migration_control
from tests.conftest import binding, envelope, repository
from trpc_service.agent.wecom_manager import WeComConnectionManager
from trpc_service.queue.session_ready import (
    SessionReady,
    SessionReadyCodec,
    SessionReadyDelivery,
    SessionReadyQueue,
    SessionReadyReclaimer,
)
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.migration import MigrationLease
from trpc_service.storage.models import (
    MailboxClaimStatus,
    OutboxRecord,
    SessionLease,
    SessionMailbox,
    SessionSnapshot,
    TurnCommit,
)
from trpc_service.storage.postgres import PostgresRuntimeRepository
from trpc_service.storage.projector import PostTurnProjector
from trpc_service.storage.protocols import FencingConflict
from trpc_service.tool.confirmation import (
    ConfirmationError,
    ConfirmationScope,
    ConfirmationTokenService,
    InMemoryConfirmationLedger,
    arguments_hash,
)


class AsyncConnection:
    def __init__(
        self,
        *,
        rows: list[Any] | None = None,
        values: list[Any] | None = None,
        fetches: list[list[Any]] | None = None,
        executes: list[str] | None = None,
    ) -> None:
        self.rows = list(rows or [])
        self.values = list(values or [])
        self.fetches = list(fetches or [])
        self.executes = list(executes or [])
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self) -> AsyncConnection:
        return self

    async def __aenter__(self) -> AsyncConnection:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, query: str, *args: Any) -> str:
        self.calls.append(("execute", (query, *args)))
        return self.executes.pop(0) if self.executes else "UPDATE 1"

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self.calls.append(("fetchrow", (query, *args)))
        return self.rows.pop(0) if self.rows else None

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.calls.append(("fetchval", (query, *args)))
        value = self.values.pop(0) if self.values else None
        if isinstance(value, BaseException):
            raise value
        return value

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        self.calls.append(("fetch", (query, *args)))
        return self.fetches.pop(0) if self.fetches else []


class AsyncPool:
    def __init__(self, connection: AsyncConnection) -> None:
        self.connection = connection

    def acquire(self) -> AsyncConnection:
        return self.connection


def _control_state(**updates: Any) -> dict[str, Any]:
    state: dict[str, Any] = {
        "state_version": 1,
        "tenant_id": "tenant-a",
        "migration_id": "migration-a",
        "source_config_version": 1,
        "target_config_version": 2,
        "source_profile_id": "source",
        "target_profile_id": "target",
        "app_control_version": 7,
        "tenant_control_version": 11,
        "dual_write": False,
        "active_profile": "source",
        "cleaned": False,
        "rolled_back": False,
        "mailbox_v2": "ready",
        "atomic_cutover": False,
        "rollback_verified": True,
    }
    state.update(updates)
    return state


def _scope(**updates: Any) -> dict[str, Any]:
    scope: dict[str, Any] = {
        "manifest": {"app_id": "assistant", "app_revision": 7},
        "app": {
            "active_config_version": 1,
            "candidate_config_version": 2,
            "candidate_percent": 0,
            "control_version": 7,
        },
        "source_config_version": 1,
        "target_config_version": 2,
        "source_profile_id": "source",
        "target_profile_id": "target",
        "app_control_version": 7,
        "tenant_control_version": 11,
    }
    scope.update(updates)
    return scope


def _notice(**updates: Any) -> SessionReady:
    value: dict[str, Any] = {
        "event_id": "event-1",
        "tenant_id": "tenant-a",
        "session_id": "session-1",
        "generation": 1,
        "priority": 0,
        "trace_id": "trace-1",
        "created_at": datetime(2026, 8, 26, tzinfo=UTC),
    }
    value.update(updates)
    return SessionReady(**value)


async def _accepted() -> Any:
    memory_repository = repository()
    inbound_envelope = envelope("postgres-extra")
    return await TenantRuntime(memory_repository, routing_key=b"p" * 32).accept(
        "binding-unpredictable-a", inbound_envelope
    )


def _inbound_row(accepted: Any, *, status: str = "accepted") -> dict[str, Any]:
    return {
        "inbound_id": accepted.inbound_id,
        "tenant_id": accepted.context.tenant_id,
        "app_id": accepted.context.app_id,
        "config_version": accepted.context.config_version,
        "binding_id": accepted.context.channel_binding_id,
        "principal_id": accepted.context.principal_id,
        "session_id": accepted.context.session_id,
        "request_id": accepted.context.request_id,
        "trace_id": accepted.context.trace_id,
        "envelope_json": accepted.envelope.model_dump_json(),
        "status": status,
        "accepted_at": datetime.now(UTC),
    }


def _mailbox_row(
    accepted: Any,
    *,
    status: str = "RUNNING",
    generation: int = 1,
    accepted_sequence: int = 1,
    resolved_sequence: int = 0,
    processing_sequence: int | None = 1,
    expires_at: datetime | None = None,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    return {
        "tenant_id": accepted.context.tenant_id,
        "session_id": accepted.context.session_id,
        "status": status,
        "accepted_sequence": accepted_sequence,
        "resolved_sequence": resolved_sequence,
        "processing_sequence": processing_sequence,
        "processing_inbound_id": accepted.inbound_id if processing_sequence else None,
        "queue_generation": generation,
        "lease_owner": "worker",
        "lease_epoch": 3,
        "lease_expires_at": expires_at or now + timedelta(minutes=1),
        "retry_count": 0,
        "attempt": 1,
        "priority": 0,
        "retry_at": None,
        "updated_at": now,
    }


def _session_lease(accepted: Any, *, worker_id: str = "worker", epoch: int = 3) -> SessionLease:
    return SessionLease(
        tenant_id=accepted.context.tenant_id,
        session_id=accepted.context.session_id,
        turn_id=str(uuid4()),
        inbound_id=accepted.inbound_id,
        worker_id=worker_id,
        fencing_token=epoch,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
        snapshot=SessionSnapshot(
            tenant_id=accepted.context.tenant_id,
            app_id=accepted.context.app_id,
            session_id=accepted.context.session_id,
            principal_id=accepted.context.principal_id,
        ),
    )


def _scope_token() -> ConfirmationScope:
    return ConfirmationScope(
        tenant_id="tenant-a",
        principal_id="principal-a",
        session_id="session-a",
        tool_name="write",
        arguments_hash=arguments_hash({"x": 1}),
    )


def test_production_control_constructor_and_unfenced_paths() -> None:
    with pytest.raises(ValueError, match="too long"):
        migration_control.create(
            pool=type("Pool", (), {"acquire": lambda _self: None})(),
            tenant_id="tenant-a",
            migration_id="m" * 250,
        )
    with pytest.raises(ValueError, match="tenant id"):
        migration_control.create(
            pool=type("Pool", (), {"acquire": lambda _self: None})(),
            tenant_id="",
            migration_id="migration-a",
        )
    with pytest.raises(TypeError, match="asyncpg pool"):
        migration_control.create(pool=object(), tenant_id="tenant-a", migration_id="migration-a")

    control = migration_control.create(
        pool=type("Pool", (), {"acquire": lambda _self: None})(),
        tenant_id="tenant-a",
        migration_id="migration-a",
    )
    for name, operation in (
        ("set_dual_write", control.set_dual_write),
        ("cutover", control.cutover),
        ("cleanup", control.cleanup),
        ("rollback", control.rollback),
    ):
        with pytest.raises(migration_control.MigrationLeaseLost):
            if name == "set_dual_write":
                asyncio.run(operation("tenant-a", True))
            else:
                asyncio.run(operation("tenant-a"))


def test_production_control_state_transition_edges() -> None:
    with pytest.raises(migration_control.MigrationGuardError, match="rollback"):
        migration_control._set_dual_write(_control_state(rolled_back=True), True)
    with pytest.raises(migration_control.MigrationGuardError, match="cleanup"):
        migration_control._set_dual_write(_control_state(cleaned=True), True)
    same, switched = migration_control._set_dual_write(_control_state(), False)
    assert same["mailbox_v2"] == "ready" and not switched
    enabled, switched = migration_control._set_dual_write(_control_state(), True)
    assert enabled["dual_write"] and enabled["mailbox_v2"] == "dual-write" and switched
    with pytest.raises(migration_control.MigrationGuardError, match="source profile"):
        migration_control._set_dual_write(_control_state(active_profile="target"), True)
    source_rollback, _ = migration_control._set_dual_write(
        _control_state(dual_write=True, rolled_back=True), False
    )
    assert source_rollback["mailbox_v2"] == "source"
    target_disabled, _ = migration_control._set_dual_write(
        _control_state(dual_write=True, active_profile="target", atomic_cutover=True), False
    )
    assert target_disabled["mailbox_v2"] == "target"

    unchanged, switched = migration_control._cutover_state(
        _control_state(active_profile="target", atomic_cutover=True)
    )
    assert unchanged["active_profile"] == "target" and not switched
    for updates, message in (
        ({"rolled_back": True}, "terminal"),
        ({"cleaned": True}, "terminal"),
        ({}, "dual-write"),
        ({"dual_write": True, "active_profile": "target"}, "dual-write"),
    ):
        with pytest.raises(migration_control.MigrationGuardError, match=message):
            migration_control._cutover_state(_control_state(**updates))
    cutover, switched = migration_control._cutover_state(_control_state(dual_write=True))
    assert cutover["atomic_cutover"] and switched

    cleaned, switched = migration_control._cleanup_state(
        _control_state(active_profile="target", atomic_cutover=True, cleaned=True)
    )
    assert not switched and cleaned["cleaned"]
    with pytest.raises(migration_control.MigrationGuardError, match="rolled-back"):
        migration_control._cleanup_state(_control_state(rolled_back=True))
    with pytest.raises(migration_control.MigrationGuardError, match="atomic"):
        migration_control._cleanup_state(_control_state())
    valid_cleanup, switched = migration_control._cleanup_state(
        _control_state(active_profile="target", atomic_cutover=True)
    )
    assert valid_cleanup["cleaned"] and switched

    rolled, switched = migration_control._rollback_state(_control_state(rolled_back=True))
    assert not switched and rolled["rolled_back"]
    rollback, switched = migration_control._rollback_state(_control_state())
    assert rollback["active_profile"] == "source" and switched


def test_production_control_state_validation_edges() -> None:
    required = _control_state()
    invalids = [
        (dict(required, extra=True), "schema"),
        (dict(required, tenant_id="other"), "scope"),
        (dict(required, state_version=2), "version"),
        (dict(required, active_profile="invalid"), "version"),
        (dict(required, mailbox_v2="invalid"), "mailbox"),
        (dict(required, dual_write=1), "boolean"),
        (dict(required, source_config_version=True), "revision"),
        (dict(required, source_config_version=0), "revision"),
        (dict(required, source_config_version=2, target_config_version=2), "differ"),
        (dict(required, source_profile_id=""), "profile"),
        (dict(required, source_profile_id="target"), "profile"),
    ]
    for state, message in invalids:
        with pytest.raises(
            (migration_control.MigrationGuardError, migration_control.MigrationManifestConflict),
            match=message,
        ):
            migration_control._validate_state(state, "tenant-a", "migration-a")
    wrong_scope_lease = type("Lease", (), {"tenant_id": "other", "migration_id": "migration-a"})()
    for bad_lease in (object(), wrong_scope_lease):
        with pytest.raises((TypeError, migration_control.MigrationLeaseLost)):
            migration_control._validate_lease_scope(bad_lease, "tenant-a", "migration-a")  # type: ignore[arg-type]
    real_wrong_scope = MigrationLease(
        tenant_id="other",
        migration_id="migration-a",
        owner_id="worker",
        lease_epoch=1,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    with pytest.raises(migration_control.MigrationLeaseLost, match="outside"):
        migration_control._validate_lease_scope(real_wrong_scope, "tenant-a", "migration-a")
    assert migration_control._is_zero_percent("0")
    assert not migration_control._is_zero_percent(None)
    assert not migration_control._is_zero_percent("not-a-number")
    for value in (None, "", "x" * 257):
        with pytest.raises(ValueError, match="1-256"):
            migration_control._validate_identifier(value, "id")  # type: ignore[arg-type]
    with pytest.raises(migration_control.MigrationGuardError, match="omitted"):
        migration_control._row({}, "missing")


@pytest.mark.asyncio
async def test_production_control_load_state_decode_and_checksum_edges() -> None:
    state = _control_state()
    encoded = json.dumps(state)
    checksum = migration_control._state_checksum(state)
    for raw, stored_checksum, expected in (
        (encoded, checksum, state),
        (state, checksum, state),
    ):
        connection = AsyncConnection(rows=[{"differences": raw, "checksum": stored_checksum}])
        control = migration_control.create(
            pool=AsyncPool(connection), tenant_id="tenant-a", migration_id="migration-a"
        )
        assert await control._load_state(connection) == expected
    for raw, checksum_value, message in (
        ("{bad", checksum, "invalid"),
        ("[]", checksum, "object"),
        (state, "bad", "checksum"),
    ):
        connection = AsyncConnection(rows=[{"differences": raw, "checksum": checksum_value}])
        control = migration_control.create(
            pool=AsyncPool(connection), tenant_id="tenant-a", migration_id="migration-a"
        )
        with pytest.raises(migration_control.MigrationGuardError, match=message):
            await control._load_state(connection)
    empty = AsyncConnection(rows=[None])
    control = migration_control.create(
        pool=AsyncPool(empty), tenant_id="tenant-a", migration_id="migration-a"
    )
    assert await control._load_state(empty) is None


@pytest.mark.asyncio
async def test_production_control_scope_loader_guards() -> None:
    manifest = {
        "tenant_id": "tenant-a",
        "migration_id": "migration-a",
        "app_id": "assistant",
        "app_revision": 7,
        "config_version": 1,
        "binding_id": "binding-a",
        "binding_revision": 3,
    }
    app = {
        "active_config_version": 1,
        "candidate_config_version": 2,
        "candidate_percent": 0,
        "control_version": 7,
    }
    source = {"version": 1, "profile_id": "source"}
    target = {"version": 2, "profile_id": "target"}
    binding_row = {"app_id": "assistant", "control_version": 3, "enabled": True}

    async def expect(rows: list[Any], values: list[Any], message: str) -> None:
        connection = AsyncConnection(rows=rows, values=values)
        control = migration_control.create(
            pool=AsyncPool(connection), tenant_id="tenant-a", migration_id="migration-a"
        )
        with pytest.raises(
            (migration_control.MigrationGuardError, migration_control.MigrationManifestConflict),
            match=message,
        ):
            await control._load_scope(connection)

    await expect([None], [], "manifest")
    await expect([manifest, None], [], "application")
    await expect([manifest, app, None], [], "source config")
    await expect(
        [manifest, {**app, "candidate_config_version": None}, source, None], [], "candidate"
    )
    await expect(
        [
            manifest,
            {**app, "candidate_config_version": None},
            source,
            {
                "differences": _control_state(target_config_version=None),
                "checksum": migration_control._state_checksum(
                    _control_state(target_config_version=None)
                ),
            },
        ],
        [],
        "candidate",
    )
    await expect([manifest, app, source, None], [], "target config")
    await expect([manifest, app, {**source, "profile_id": None}, target], [], "source config")
    await expect([manifest, app, source, {**target, "profile_id": ""}], [], "target config")
    await expect([manifest, app, source, {**target, "profile_id": "source"}], [], "must differ")
    await expect([manifest, app, source, target], [False], "not ready")
    await expect([manifest, app, source, target, None], [True], "binding")
    await expect(
        [manifest, app, source, target, {**binding_row, "app_id": "other"}], [True], "binding"
    )
    await expect(
        [manifest, app, source, target, {**binding_row, "control_version": 4}], [True], "binding"
    )
    await expect(
        [manifest, app, source, target, {**binding_row, "enabled": False}], [True], "binding"
    )
    await expect([manifest, app, source, target, binding_row], [True, None], "mailbox")
    await expect(
        [manifest, app, source, target, binding_row],
        [True, "session_mailboxes", None],
        "tenant",
    )

    good = AsyncConnection(
        rows=[manifest, app, source, target, binding_row],
        values=[True, "session_mailboxes", 11],
    )
    control = migration_control.create(
        pool=AsyncPool(good), tenant_id="tenant-a", migration_id="migration-a"
    )
    loaded = await control._load_scope(good)
    assert loaded["source_profile_id"] == "source"


@pytest.mark.asyncio
async def test_production_control_persistence_switch_and_initial_state_guards() -> None:
    control = migration_control.create(
        pool=AsyncPool(AsyncConnection()), tenant_id="tenant-a", migration_id="migration-a"
    )
    state = _control_state()
    with pytest.raises(migration_control.MigrationGuardError, match="not persisted"):
        await control._insert_state(AsyncConnection(rows=[None]), state)
    with pytest.raises(migration_control.MigrationGuardError, match="disappeared"):
        await control._persist_state(AsyncConnection(rows=[None]), state)
    with pytest.raises(migration_control.MigrationManifestConflict, match="scope"):
        control._check_scope("other", "migration-a")

    scope = _scope()
    with pytest.raises(migration_control.MigrationManifestConflict, match="tenant control"):
        await control._switch_profile(AsyncConnection(rows=[None]), scope, state, target=True)
    with pytest.raises(migration_control.MigrationManifestConflict, match="pointer changed"):
        await control._switch_profile(
            AsyncConnection(rows=[{"control_version": 12}, None]), scope, state, target=True
        )
    with pytest.raises(migration_control.MigrationManifestConflict, match="did not switch"):
        await control._switch_profile(
            AsyncConnection(
                rows=[{"control_version": 12}, {"active_config_version": 99, "control_version": 8}]
            ),
            scope,
            state,
            target=True,
        )

    valid_lease = MigrationLease(
        tenant_id="tenant-a",
        migration_id="migration-a",
        owner_id="worker",
        lease_epoch=1,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    with pytest.raises(TypeError, match="boolean"):
        await control.set_dual_write_fenced(
            "tenant-a",
            1,
            lease=valid_lease,  # type: ignore[arg-type]
        )

    for app_updates, message in (
        ({"active_config_version": 2}, "active revision"),
        ({"candidate_config_version": None}, "int|staged"),
        ({"candidate_config_version": 3}, "staged"),
        ({"candidate_percent": 10}, "receiving"),
    ):
        with pytest.raises(
            (
                TypeError,
                migration_control.MigrationManifestConflict,
                migration_control.MigrationGuardError,
            ),
            match=message,
        ):
            migration_control._initial_state(
                _scope(app={**scope["app"], **app_updates}), "tenant-a", "migration-a"
            )
    with pytest.raises(
        (migration_control.MigrationManifestConflict, migration_control.MigrationGuardError),
        match="control revision",
    ):
        migration_control._initial_state(
            _scope(manifest={"app_id": "assistant", "app_revision": 8}),
            "tenant-a",
            "migration-a",
        )

    runtime_checks = (
        ({"app": {**scope["app"], "control_version": 8}}, "application control"),
        ({"tenant_control_version": 12}, "tenant control"),
        ({"app": {**scope["app"], "candidate_percent": 1}}, "rollout"),
        ({"app": {**scope["app"], "active_config_version": 2}}, "active config"),
        ({"app": {**scope["app"], "candidate_config_version": None}}, "candidate config"),
        ({"source_profile_id": "changed"}, "source storage"),
        ({"target_profile_id": "changed"}, "target storage"),
    )
    for updates, message in runtime_checks:
        checked_scope = _scope(**updates)
        with pytest.raises(migration_control.MigrationManifestConflict, match=message):
            migration_control._assert_runtime_state(checked_scope, state)
    rolled_state = _control_state(rolled_back=True, dual_write=False, mailbox_v2="source")
    rolled_scope = _scope(app={**scope["app"], "candidate_config_version": None})
    migration_control._assert_runtime_state(rolled_scope, rolled_state)
    with pytest.raises(
        migration_control.MigrationManifestConflict, match="candidate config does not match"
    ):
        migration_control._assert_runtime_state(
            _scope(app={**scope["app"], "candidate_config_version": None}),
            _control_state(rolled_back=False, active_profile="source"),
        )
    with pytest.raises(migration_control.MigrationManifestConflict, match="after cutover"):
        migration_control._assert_runtime_state(
            _scope(app={**scope["app"], "active_config_version": 2, "candidate_config_version": 2}),
            _control_state(active_profile="target", atomic_cutover=True),
        )
    with pytest.raises(migration_control.MigrationManifestConflict, match="rollback left"):
        migration_control._assert_runtime_state(
            _scope(), _control_state(rolled_back=True, mailbox_v2="source")
        )


@pytest.mark.asyncio
async def test_postgres_claim_session_ready_rejects_stale_and_retry_cases() -> None:
    accepted = await _accepted()
    event_id = str(uuid4())
    event = {
        "outbox_id": event_id,
        "tenant_id": accepted.context.tenant_id,
        "aggregate_type": "session",
        "aggregate_id": accepted.context.session_id,
        "event_type": "session.ready.v2",
        "payload_json": {"generation": 1},
    }
    repository = PostgresRuntimeRepository(AsyncPool(AsyncConnection()))
    with pytest.raises(ValueError, match="positive"):
        await repository.claim_session_ready(
            accepted.context.tenant_id,
            accepted.context.session_id,
            owner_id="worker",
            lease_for=timedelta(seconds=0),
            expected_event_id=event_id,
        )
    missing_event = await PostgresRuntimeRepository(
        AsyncPool(AsyncConnection(rows=[None]))
    ).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker",
        lease_for=timedelta(seconds=10),
        expected_event_id=event_id,
    )
    assert missing_event.status == MailboxClaimStatus.STALE

    for mailbox, expected in (
        (None, MailboxClaimStatus.STALE),
        (_mailbox_row(accepted, generation=2), MailboxClaimStatus.STALE),
        (
            _mailbox_row(
                accepted,
                accepted_sequence=0,
                resolved_sequence=0,
                processing_sequence=None,
                generation=1,
                status="QUEUED",
            ),
            MailboxClaimStatus.EMPTY,
        ),
    ):
        rows = [event, mailbox]
        values: list[Any] = []
        if mailbox is not None and mailbox["queue_generation"] == 1:
            values = [datetime.now(UTC)]
            if mailbox["accepted_sequence"] == 0:
                mailbox["lease_expires_at"] = datetime.now(UTC) - timedelta(seconds=1)
        result = await PostgresRuntimeRepository(
            AsyncPool(AsyncConnection(rows=rows, values=values))
        ).claim_session_ready(
            accepted.context.tenant_id,
            accepted.context.session_id,
            owner_id="worker",
            lease_for=timedelta(seconds=10),
            expected_generation=1,
            expected_event_id=event_id,
        )
        assert result.status == expected

    now = datetime.now(UTC)
    item = {
        "inbound_id": accepted.inbound_id,
        "retry_at": now + timedelta(minutes=1),
        "attempt": 0,
        "retry_count": 0,
        "priority": 0,
    }
    retry_row = _mailbox_row(
        accepted,
        status="RETRY_WAIT",
        expires_at=now - timedelta(seconds=1),
    )
    retry_row["retry_at"] = item["retry_at"]
    retry_connection = AsyncConnection(
        rows=[
            event,
            _mailbox_row(accepted, expires_at=now - timedelta(seconds=1)),
            item,
            retry_row,
        ],
        values=[now],
    )
    retry_result = await PostgresRuntimeRepository(AsyncPool(retry_connection)).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker",
        lease_for=timedelta(seconds=10),
        expected_event_id=event_id,
    )
    assert retry_result.status == MailboxClaimStatus.EMPTY

    no_inbound = AsyncConnection(
        rows=[
            event,
            _mailbox_row(accepted, expires_at=now - timedelta(seconds=1)),
            {**item, "retry_at": None},
            None,
        ],
        values=[now],
    )
    no_inbound_result = await PostgresRuntimeRepository(AsyncPool(no_inbound)).claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker",
        lease_for=timedelta(seconds=10),
        expected_event_id=event_id,
    )
    assert no_inbound_result.status == MailboxClaimStatus.EMPTY

    committed = AsyncConnection(
        rows=[
            event,
            _mailbox_row(accepted, expires_at=now - timedelta(seconds=1)),
            {**item, "retry_at": None},
            {"status": "committed"},
        ],
        values=[now],
    )
    repo = PostgresRuntimeRepository(AsyncPool(committed))

    async def resolve(*_args: Any, **_kwargs: Any) -> SessionMailbox:
        return SessionMailbox(
            tenant_id=accepted.context.tenant_id,
            session_id=accepted.context.session_id,
        )

    repo._resolve_mailbox_item = resolve  # type: ignore[method-assign]
    committed_result = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker",
        lease_for=timedelta(seconds=10),
        expected_event_id=event_id,
    )
    assert committed_result.status == MailboxClaimStatus.EMPTY


@pytest.mark.asyncio
async def test_postgres_accept_v2_duplicate_and_mailbox_status_edges() -> None:
    accepted = await _accepted()
    with pytest.raises(ValueError, match="priority"):
        await PostgresRuntimeRepository(AsyncPool(AsyncConnection())).accept_inbound_v2(
            context=accepted.context,
            envelope=accepted.envelope,
            trace_headers={},
            priority=-1,
        )
    duplicate = await PostgresRuntimeRepository(
        AsyncPool(AsyncConnection(rows=[None, _inbound_row(accepted)]))
    ).accept_inbound_v2(
        context=accepted.context,
        envelope=accepted.envelope,
        trace_headers={},
    )
    assert duplicate.duplicate
    now = datetime.now(UTC)
    base_item = {
        "priority": 0,
        "trace_id": "trace",
        "inbound_id": accepted.inbound_id,
    }
    for mailbox_status, retry_at, expected_status in (
        ("RETRY_WAIT", now + timedelta(minutes=1), "RETRY_WAIT"),
        ("RETRY_WAIT", now - timedelta(minutes=1), "QUEUED"),
        ("IDLE", now + timedelta(minutes=1), "RETRY_WAIT"),
        ("IDLE", now - timedelta(minutes=1), "QUEUED"),
        ("RUNNING", None, "RUNNING"),
    ):
        mailbox = {
            "tenant_id": accepted.context.tenant_id,
            "session_id": accepted.context.session_id,
            "status": mailbox_status,
            "accepted_sequence": 0,
            "resolved_sequence": 0,
            "processing_sequence": None,
            "processing_inbound_id": None,
            "queue_generation": 0,
            "lease_owner": None,
            "lease_epoch": 0,
            "lease_expires_at": None,
            "retry_count": 0,
            "attempt": 0,
            "priority": 0,
            "retry_at": retry_at,
            "updated_at": now,
        }
        # For a due RETRY_WAIT item, the prior timer no longer gates this
        # append; an IDLE mailbox follows the new item's retry time directly.
        changed = {**mailbox, "status": expected_status, "accepted_sequence": 1}
        connection = AsyncConnection(
            rows=[_inbound_row(accepted), mailbox, {**base_item, "retry_at": retry_at}, changed],
            values=[now],
        )
        result = await PostgresRuntimeRepository(AsyncPool(connection)).accept_inbound_v2(
            context=accepted.context,
            envelope=accepted.envelope,
            trace_headers={},
            retry_at=retry_at,
        )
        assert result.inbound_id == accepted.inbound_id


@pytest.mark.asyncio
async def test_postgres_session_ready_renew_and_resolve_mailbox_edges() -> None:
    accepted = await _accepted()
    lease = _session_lease(accepted)
    updated = datetime.now(UTC) + timedelta(minutes=2)
    success = await PostgresRuntimeRepository(
        AsyncPool(AsyncConnection(values=[updated, updated]))
    ).renew_session_ready(lease, lease_for=timedelta(seconds=30))
    assert success.expires_at == updated
    with pytest.raises(FencingConflict, match="session lease"):
        await PostgresRuntimeRepository(
            AsyncPool(AsyncConnection(values=[updated, None]))
        ).renew_session_ready(lease, lease_for=timedelta(seconds=30))
    with pytest.raises(ValueError, match="positive"):
        await PostgresRuntimeRepository(AsyncPool(AsyncConnection())).renew_session_ready(
            lease, lease_for=timedelta(seconds=0)
        )

    repository = PostgresRuntimeRepository(AsyncPool(AsyncConnection()))
    now = datetime.now(UTC)
    updated_mailbox = _mailbox_row(accepted, status="IDLE", generation=2, processing_sequence=None)
    for next_item in (
        None,
        {"retry_at": now + timedelta(minutes=1), "inbound_id": accepted.inbound_id},
        {"retry_at": now - timedelta(minutes=1), "inbound_id": accepted.inbound_id},
    ):
        connection = AsyncConnection(rows=[next_item, updated_mailbox])
        result = await repository._resolve_mailbox_item(
            connection,
            tenant_id=accepted.context.tenant_id,
            session_id=accepted.context.session_id,
            sequence=1,
            server_now=now,
            expected_lease_owner="worker",
            expected_lease_epoch=3,
        )
        assert result.session_id == accepted.context.session_id


def _commit_rows(accepted: Any, now: datetime, *, next_item: Any = None) -> list[Any]:
    mailbox = _mailbox_row(
        accepted, status="RUNNING", generation=1, expires_at=now + timedelta(minutes=1)
    )
    session = {
        "app_id": accepted.context.app_id,
        "principal_id": accepted.context.principal_id,
        "version": 0,
        "next_sequence": 1,
        "state_json": "{}",
        "lease_owner": "worker",
        "lease_epoch": 3,
        "lease_expires_at": now + timedelta(minutes=1),
    }
    turn = {"inbound_id": accepted.inbound_id, "status": "processing", "fencing_token": 3}
    item = {"inbound_id": accepted.inbound_id}
    inbound = {"status": "processing"}
    updated_session = {"session_id": accepted.context.session_id}
    updated_mailbox = _mailbox_row(
        accepted,
        status="IDLE" if next_item is None else "QUEUED",
        generation=1 if next_item is None else 2,
        processing_sequence=None,
    )
    return [mailbox, session, turn, item, inbound, updated_session, next_item, updated_mailbox]


@pytest.mark.asyncio
async def test_postgres_commit_session_ready_terminal_paths_and_fault_hook() -> None:
    accepted = await _accepted()
    lease = _session_lease(accepted)
    now = datetime.now(UTC)
    commit = TurnCommit(context=accepted.context, lease=lease, state={}, events=())
    for next_item in (
        None,
        {"retry_at": now + timedelta(minutes=1), "inbound_id": accepted.inbound_id},
        {"retry_at": now - timedelta(minutes=1), "inbound_id": accepted.inbound_id},
    ):
        connection = AsyncConnection(
            rows=_commit_rows(accepted, now, next_item=next_item), values=[now, now, now]
        )
        result = await PostgresRuntimeRepository(AsyncPool(connection)).commit_session_ready(commit)
        assert result.first_sequence is None and result.last_sequence is None

    class Faults:
        async def checkpoint(self, _event: Any) -> None:
            raise RuntimeError("fault marker")

    faulty = PostgresRuntimeRepository(
        AsyncPool(AsyncConnection(rows=_commit_rows(accepted, now), values=[now, now])),
        fault_stages=Faults(),  # type: ignore[arg-type]
    )
    with pytest.raises(RuntimeError, match="fault marker"):
        await faulty.commit_session_ready(commit)
    invalid_rows = _commit_rows(accepted, now)
    invalid_rows[2] = None
    with pytest.raises(FencingConflict, match="stale"):
        await PostgresRuntimeRepository(
            AsyncPool(AsyncConnection(rows=invalid_rows, values=[now, now, now]))
        ).commit_session_ready(commit)


@pytest.mark.asyncio
async def test_postgres_retry_and_fail_session_ready_fence_and_next_item_edges() -> None:
    accepted = await _accepted()
    lease = _session_lease(accepted)
    now = datetime.now(UTC)
    for method_name, _message in (("retry_session_ready", "retry"), ("fail_session_ready", "fail")):
        method = getattr(PostgresRuntimeRepository(AsyncPool(AsyncConnection())), method_name)
        with pytest.raises(FencingConflict, match="stale"):
            if method_name == "retry_session_ready":
                await method(lease, error_type="x", delay=timedelta(seconds=1))
            else:
                await method(lease, error_type="x")
    with pytest.raises(ValueError, match="non-negative"):
        await PostgresRuntimeRepository(AsyncPool(AsyncConnection())).retry_session_ready(
            lease, error_type="x", delay=timedelta(seconds=-1)
        )
    mailbox = _mailbox_row(accepted, status="RUNNING", expires_at=now + timedelta(minutes=1))
    session = {
        "lease_owner": "worker",
        "lease_epoch": 3,
        "lease_expires_at": now + timedelta(minutes=1),
    }
    turn = {"status": "processing", "fencing_token": 3}
    item = {"inbound_id": accepted.inbound_id}
    inbound = {"status": "processing"}
    for next_item in (
        None,
        {"retry_at": now + timedelta(minutes=1), "inbound_id": accepted.inbound_id},
        {"retry_at": now - timedelta(minutes=1), "inbound_id": accepted.inbound_id},
    ):
        updated = _mailbox_row(
            accepted,
            status="IDLE" if next_item is None else "QUEUED",
            generation=1 if next_item is None else 2,
            processing_sequence=None,
        )
        conn = AsyncConnection(
            rows=[mailbox, session, turn, item, inbound, next_item, updated], values=[now]
        )
        await PostgresRuntimeRepository(AsyncPool(conn)).fail_session_ready(lease, error_type="x")


class ProjectionRepository:
    def __init__(self, records: list[OutboxRecord]) -> None:
        self.records = records
        self.snapshot: SessionSnapshot | None = None
        self.published: list[str] = []
        self.released: list[tuple[str, str]] = []
        self.dead: list[str] = []
        self.fail_cleanup = False

    async def claim_outbox(self, **_kwargs: Any) -> tuple[OutboxRecord, ...]:
        return tuple(self.records)

    async def get_session_snapshot(
        self, _tenant_id: str, _session_id: str
    ) -> SessionSnapshot | None:
        return self.snapshot

    async def mark_outbox_published(self, _tenant: str, outbox_id: str, *, owner_id: str) -> None:
        del owner_id
        self.published.append(outbox_id)

    async def release_outbox(self, _tenant: str, outbox_id: str, **_kwargs: Any) -> None:
        if self.fail_cleanup:
            raise RuntimeError("cleanup failed")
        self.released.append((outbox_id, str(_kwargs.get("error_type"))))

    async def dead_letter_outbox(self, record: OutboxRecord, **_kwargs: Any) -> None:
        if self.fail_cleanup:
            raise RuntimeError("cleanup failed")
        self.dead.append(record.outbox_id)


class ProjectionStore:
    def __init__(self) -> None:
        self.values: list[tuple[str, str, int]] = []

    async def put_session(
        self, tenant: str, session: str, *, sequence: int, value: dict[str, Any]
    ) -> None:
        self.values.append((tenant, session, sequence))
        assert value["session_id"] == session


@pytest.mark.asyncio
async def test_post_turn_projector_success_failure_retry_and_run_edges() -> None:
    record = OutboxRecord(
        outbox_id="outbox-1",
        tenant_id="tenant-a",
        event_type="post_turn.ready",
        aggregate_id="turn-1",
        payload={"session_id": "session-1"},
        attempts=0,
    )
    repo = ProjectionRepository([record])
    repo.snapshot = SessionSnapshot(
        tenant_id="tenant-a",
        app_id="app",
        session_id="session-1",
        principal_id="user",
        version=1,
        next_sequence=3,
        state={},
        events=(),
    )
    projection = ProjectionStore()
    projector = PostTurnProjector(repo, projection, owner_id="projector")  # type: ignore[arg-type]
    assert await projector.project_once() == 1
    assert projection.values == [("tenant-a", "session-1", 2)]
    repo.snapshot = None
    assert await projector.project_once() == 0
    assert repo.released
    record = record.model_copy(update={"attempts": 5})
    repo.records = [record]
    assert await projector.project_once() == 0
    assert repo.dead
    repo.fail_cleanup = True
    assert await projector.project_once() == 0

    stop = asyncio.Event()
    calls = 0

    async def project_once(*, stop_event: asyncio.Event | None = None) -> int:
        nonlocal calls
        del stop_event
        calls += 1
        stop.set()
        return 0

    projector.project_once = project_once  # type: ignore[method-assign]
    stop.set()
    await projector.run(poll_seconds=0, stop_event=stop)
    assert calls == 0
    stop.clear()
    await projector.run(poll_seconds=0, stop_event=stop)
    assert calls == 1

    async def claimed_once(*, stop_event: asyncio.Event | None = None) -> int:
        assert stop_event is not None
        stop_event.set()
        return 1

    projector.project_once = claimed_once  # type: ignore[method-assign]
    stop.clear()
    await projector.run(poll_seconds=1, stop_event=stop)

    async def cancelled(*, stop_event: asyncio.Event | None = None) -> int:
        del stop_event
        raise asyncio.CancelledError

    projector.project_once = cancelled  # type: ignore[method-assign]
    with pytest.raises(asyncio.CancelledError):
        await projector.run(stop_event=asyncio.Event())


@pytest.mark.asyncio
async def test_confirmation_tokens_cover_invalid_payloads_and_one_time_scope() -> None:
    with pytest.raises(ValueError, match="at least 32"):
        ConfirmationTokenService(b"short", InMemoryConfirmationLedger())
    service = ConfirmationTokenService(b"k" * 32, InMemoryConfirmationLedger(), ttl_seconds=10)
    expected = _scope_token()
    token = await service.issue(expected)
    await service.consume(token, expected)
    with pytest.raises(ConfirmationError, match="already used"):
        await service.consume(token, expected)
    for malformed in (None, "", ".", "a", "a.b.c", "x" * 16_385, "not-base64.signature"):
        with pytest.raises(ConfirmationError, match="invalid"):
            await service.consume(malformed, expected)  # type: ignore[arg-type]

    other = await service.issue(expected)
    with pytest.raises(ConfirmationError, match="invalid"):
        await service.consume(other.split(".", 1)[0], expected)
    payload, signature = other.split(".", 1)
    with pytest.raises(ConfirmationError, match="invalid"):
        await service.consume(f"{payload}.{signature[:-1]}x", expected)

    changed = ConfirmationScope(
        "tenant-a", "principal-a", "session-a", "delete", expected.arguments_hash
    )
    with pytest.raises(ConfirmationError, match="scope mismatch"):
        await service.consume(other, changed)

    ledger = InMemoryConfirmationLedger()
    expiring = ConfirmationTokenService(b"k" * 32, ledger, ttl_seconds=-1)
    expired = await expiring.issue(expected)
    with pytest.raises(ConfirmationError, match="expired"):
        await expiring.consume(expired, expected)

    def token_with_payload(payload: object) -> str:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        import base64
        import hmac

        encoded_b64 = base64.urlsafe_b64encode(encoded.encode()).rstrip(b"=").decode()
        sig = (
            base64.urlsafe_b64encode(
                hmac.new(b"k" * 32, encoded_b64.encode(), hashlib.sha256).digest()
            )
            .rstrip(b"=")
            .decode()
        )
        return f"{encoded_b64}.{sig}"

    for payload, message in (
        ([], "invalid"),
        ({"exp": True}, "invalid"),
        ({"exp": int(time.time()) - 1}, "expired"),
        ({"exp": int(time.time()) + 100, "jti": "x"}, "scope"),
        (
            {
                "exp": int(time.time()) + 100,
                "jti": "",
                "tenant_id": expected.tenant_id,
                "principal_id": expected.principal_id,
                "session_id": expected.session_id,
                "tool_name": expected.tool_name,
                "arguments_hash": expected.arguments_hash,
            },
            "invalid",
        ),
    ):
        with pytest.raises(ConfirmationError, match=message):
            await service.consume(token_with_payload(payload), expected)

    import trpc_service.tool.confirmation as confirmation_module

    with pytest.raises(ValueError):
        confirmation_module._decode("")
    with pytest.raises(ValueError):
        confirmation_module._decode("not-valid!")


class BindingRepository:
    def __init__(self, bindings: tuple[Any, ...]) -> None:
        self.bindings = bindings

    async def list_bindings(self, _channel: Any) -> tuple[Any, ...]:
        return self.bindings

    async def resolve_binding(self, _binding_id: str) -> Any:
        return None


@pytest.mark.asyncio
async def test_wecom_manager_signatures_backoff_and_emergency_edges() -> None:
    value = binding(channel="wecom_ai_bot")
    assert wecom_manager._binding_signature(value) == wecom_manager._binding_signature(value)
    assert wecom_manager._accepts_stop_event(lambda a, b, c: None)
    assert not wecom_manager._accepts_stop_event(object())
    assert wecom_manager._accepts_emergency_sink(lambda a, b, c, d: None)
    assert not wecom_manager._accepts_emergency_sink(object())
    assert inspect.Parameter.VAR_POSITIONAL
    await wecom_manager._wait_or_stop(None, 0)
    stop = asyncio.Event()
    stop.set()
    await wecom_manager._wait_or_stop(stop, 1)

    with pytest.raises(ValueError, match="between"):
        WeComConnectionManager(BindingRepository(()), object(), object(), reconnect_jitter_ratio=2)  # type: ignore[arg-type]
    manager = WeComConnectionManager(
        BindingRepository(()),
        object(),
        object(),
        random_fn=lambda: 2.0,  # type: ignore[arg-type]
    )
    assert manager._backoff_delay(100) == 60
    with pytest.raises(RuntimeError, match="not configured"):
        await manager._emergency_for_binding("missing", object())  # type: ignore[arg-type]
    emergency_calls: list[Any] = []

    async def emergency(route: Any, envelope: Any) -> None:
        emergency_calls.append((route, envelope))

    route = type("Route", (), {"binding": value, "tenant_active": True})()
    manager = WeComConnectionManager(
        BindingRepository(()),
        object(),
        object(),
        emergency_sink=emergency,  # type: ignore[arg-type]
    )
    with pytest.raises(RuntimeError, match="unavailable"):
        await manager._emergency_for_binding("missing", object())  # type: ignore[arg-type]
    manager._routes["binding-unpredictable-a"] = route  # type: ignore[assignment]
    await manager._emergency_for_binding("binding-unpredictable-a", "envelope")  # type: ignore[arg-type]
    assert emergency_calls == [(route, "envelope")]


@pytest.mark.asyncio
async def test_wecom_manager_reconcile_and_runner_signature_edges() -> None:
    value = binding(channel="wecom_ai_bot")
    repository = BindingRepository((value,))
    started = asyncio.Event()

    class Connector:
        async def run(self, _binding: Any, _sink: Any, stop_event: asyncio.Event) -> None:
            started.set()
            stop_event.set()

    manager = WeComConnectionManager(repository, Connector(), object())  # type: ignore[arg-type]
    await manager.reconcile_once()
    await asyncio.wait_for(started.wait(), timeout=1)
    await manager.reconcile_once()
    await manager._tasks[value.binding_id]

    # A completed task with an exception is logged and removed; this exercises
    # the done/error cleanup branch independently of connector reconciliation.
    async def failed_task() -> None:
        raise RuntimeError("connector stopped")

    task = asyncio.create_task(failed_task())
    await asyncio.gather(task, return_exceptions=True)
    manager._tasks["gone"] = task
    await manager.reconcile_once()
    assert "gone" not in manager._tasks

    repository.bindings = ()
    await manager.reconcile_once()
    assert value.binding_id not in manager._tasks

    class NoStopConnector:
        def __init__(self) -> None:
            self.calls = 0

        async def run(self, _binding: Any, _sink: Any) -> None:
            self.calls += 1

    connector = NoStopConnector()
    manager = WeComConnectionManager(BindingRepository(()), connector, object())  # type: ignore[arg-type]
    manager._stop_event.set()
    await manager._run_binding(value)
    assert connector.calls == 0

    connector = NoStopConnector()
    manager = WeComConnectionManager(BindingRepository(()), connector, object())  # type: ignore[arg-type]
    manager._stop_event.clear()
    original_wait_for = wecom_manager.asyncio.wait_for
    calls = 0

    async def timeout_once(awaitable: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            if hasattr(awaitable, "close"):
                awaitable.close()
            raise TimeoutError
        result = await original_wait_for(awaitable, *args, **kwargs)
        return result

    async def stop_after_two(*_args: Any) -> None:
        connector.calls += 1
        if connector.calls >= 2:
            manager._stop_event.set()

    connector.run = stop_after_two  # type: ignore[method-assign]
    wecom_manager.asyncio.wait_for = timeout_once  # type: ignore[assignment]
    try:
        await manager._run_binding(value)
    finally:
        wecom_manager.asyncio.wait_for = original_wait_for  # type: ignore[assignment]

    manager = WeComConnectionManager(BindingRepository(()), object(), object())  # type: ignore[arg-type]
    stop_event = asyncio.Event()
    stop_event.set()
    await manager.run(stop_event=stop_event, refresh_seconds=0)


class ReadyRedis:
    def __init__(self) -> None:
        self.xdel_calls = 0
        self.xack_calls = 0
        self.xdel_results: list[Any] = []
        self.xautoclaim_results: list[Any] = []
        self.group_error: BaseException | None = None

    async def xgroup_create(self, *_args: Any, **_kwargs: Any) -> None:
        if self.group_error:
            raise self.group_error

    async def xadd(self, *_args: Any, **_kwargs: Any) -> bytes:
        return b"1-0"

    async def xreadgroup(self, *_args: Any, **_kwargs: Any) -> Any:
        return []

    async def xack(self, *_args: Any, **_kwargs: Any) -> int:
        self.xack_calls += 1
        return 1

    async def xdel(self, *_args: Any, **_kwargs: Any) -> int:
        self.xdel_calls += 1
        if self.xdel_results:
            result = self.xdel_results.pop(0)
            if isinstance(result, BaseException):
                raise result
        return 1

    async def xautoclaim(self, *_args: Any, **_kwargs: Any) -> Any:
        return self.xautoclaim_results.pop(0) if self.xautoclaim_results else (b"0-0", [], [])


@pytest.mark.asyncio
async def test_session_ready_quarantine_delete_and_reclaimer_edges() -> None:
    redis = ReadyRedis()
    queue = SessionReadyQueue(redis, xdel_attempts=2, xdel_retry_delay_seconds=0)
    redis.xdel_results = [RuntimeError("temporary"), None]
    assert await queue.ack(SessionReadyDelivery("1-0", _notice()))
    assert redis.xdel_calls == 2
    redis.xdel_results = [asyncio.CancelledError()]
    with pytest.raises(asyncio.CancelledError):
        await queue.ack(SessionReadyDelivery("1-0", _notice()))

    redis.xdel_results = [RuntimeError("temporary"), None]
    sleep_calls: list[float] = []
    original_sleep = asyncio.sleep

    async def record_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    asyncio.sleep = record_sleep  # type: ignore[assignment]
    try:
        assert await queue._delete_after_ack("1-0") is None
    finally:
        asyncio.sleep = original_sleep  # type: ignore[assignment]
    assert sleep_calls == []
    queue_with_delay = SessionReadyQueue(redis, xdel_attempts=2, xdel_retry_delay_seconds=0.1)
    redis.xdel_results = [RuntimeError("temporary"), None]
    sleep_calls.clear()
    asyncio.sleep = record_sleep  # type: ignore[assignment]
    try:
        await queue_with_delay._delete_after_ack("1-0")
    finally:
        asyncio.sleep = original_sleep  # type: ignore[assignment]
    assert sleep_calls == [0.1]

    malformed_rows = [
        None,
        ("stream", None),
        ("stream", [(None, {})]),
        ("stream", [(b"2-0", {b"wrong": b"field"})]),
    ]
    connection = redis
    queue._redis = connection
    assert await queue._decode_rows_safely(malformed_rows) == ()

    class NoDelete:
        async def xack(self, *_args: Any, **_kwargs: Any) -> int:
            raise RuntimeError("ack failed")

    reclaimer = SessionReadyReclaimer(NoDelete(), consumer="worker")  # type: ignore[arg-type]
    assert await reclaimer._delete_reclaimed("1-0") is None
    permit_false = SessionReadyReclaimer(
        redis, consumer="worker", permit=lambda: _false(), poll_seconds=0
    )
    assert await permit_false.reclaim() == ()
    redis.xautoclaim_results = [(b"1-0", [(b"bad", {b"bad": b"field"})], [])]
    assert await reclaimer._decode_entries_safely([]) == ()
    reclaimer = SessionReadyReclaimer(redis, consumer="worker")
    assert await reclaimer.reclaim() == ()

    import trpc_service.queue.session_ready as ready_module

    encoded = SessionReadyCodec.encode(_notice())
    assert ready_module._decode_rows([]) == ()
    assert ready_module._decode_rows([("stream", [("1-0", encoded)])])[0].stream_id == "1-0"
    assert ready_module._decode_entries([("1-0", encoded)])[0].message == _notice()


async def _false() -> bool:
    return False
