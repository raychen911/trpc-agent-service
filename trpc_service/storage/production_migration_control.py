"""Database-backed production migration control hooks.

The migration coordinator owns data movement and leases.  This module owns
the small control-plane transition that makes a candidate tenant config
authoritative.  Every transition is persisted in PostgreSQL and is fenced by
the lease that the coordinator already acquired; there is deliberately no
process-local state or unguarded production path.

The control checkpoint uses a reserved migration id so it cannot be confused
with a coordinator checkpoint.  ``agent_apps`` remains the authoritative
routing pointer.  The checkpoint is only the durable, observable record of
the dual-write/mailbox state and the exact revisions needed for rollback.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from trpc_service.storage.migration import (
    MigrationGuardError,
    MigrationLease,
    MigrationLeaseLost,
    MigrationManifestConflict,
)

_CONTROL_SUFFIX = "::production-control"
_CONTROL_PHASE = "production-control"
_CONTROL_BATCH = "state"
_STATE_VERSION = 1
_ACTIVE_PROFILES = frozenset({"source", "target"})
_MAILBOX_STATES = frozenset({"ready", "dual-write", "target", "source"})


class PostgresMigrationControl:
    """Fenced, durable control-plane adapter for one migration scope.

    The factory intentionally does not open a second connection or accept a
    DSN.  The caller supplies the already-authenticated PostgreSQL pool used
    by the migration target.  A candidate config revision and both referenced
    storage profiles must already exist before the first observable state can
    be returned.
    """

    def __init__(self, pool: asyncpg.Pool, *, tenant_id: str, migration_id: str) -> None:
        _validate_identifier(tenant_id, "tenant id")
        _validate_identifier(migration_id, "migration id")
        if len(migration_id) + len(_CONTROL_SUFFIX) > 256:
            raise ValueError("migration id is too long for the durable control checkpoint")
        if not callable(getattr(pool, "acquire", None)):
            raise TypeError("production migration control requires an asyncpg pool")
        self._pool = pool
        self._tenant_id = tenant_id
        self._migration_id = migration_id
        self._control_migration_id = migration_id + _CONTROL_SUFFIX

    # The loader requires the unfenced names for interface compatibility.  A
    # production caller must never be able to use them as a lease bypass.
    async def set_dual_write(self, tenant_id: str, enabled: bool) -> None:
        del tenant_id, enabled
        raise MigrationLeaseLost("production migration control requires a fenced lease")

    async def cutover(self, tenant_id: str) -> None:
        del tenant_id
        raise MigrationLeaseLost("production migration control requires a fenced lease")

    async def cleanup(self, tenant_id: str) -> None:
        del tenant_id
        raise MigrationLeaseLost("production migration control requires a fenced lease")

    async def rollback(self, tenant_id: str) -> None:
        del tenant_id
        raise MigrationLeaseLost("production migration control requires a fenced lease")

    async def set_dual_write_fenced(
        self, tenant_id: str, enabled: bool, *, lease: MigrationLease
    ) -> None:
        if not isinstance(enabled, bool):
            raise TypeError("dual-write state must be a boolean")
        await self._transition(tenant_id, lease, "set_dual_write", enabled=enabled)

    async def cutover_fenced(self, tenant_id: str, *, lease: MigrationLease) -> None:
        await self._transition(tenant_id, lease, "cutover")

    async def cleanup_fenced(self, tenant_id: str, *, lease: MigrationLease) -> None:
        await self._transition(tenant_id, lease, "cleanup")

    async def rollback_fenced(self, tenant_id: str, *, lease: MigrationLease) -> None:
        await self._transition(tenant_id, lease, "rollback")

    async def read_state(self, tenant_id: str, migration_id: str) -> dict[str, Any]:
        """Read the durable state and verify it still matches live pointers."""

        self._check_scope(tenant_id, migration_id)
        async with self._transaction(tenant_id) as connection:
            scope = await self._load_scope(connection)
            state = await self._load_state(connection)
            if state is None:
                state = _initial_state(scope, self._tenant_id, self._migration_id)
                await self._insert_state(connection, state)
            else:
                _validate_state(state, self._tenant_id, self._migration_id)
            _assert_runtime_state(scope, state)
            return dict(state)

    async def _transition(
        self,
        tenant_id: str,
        lease: MigrationLease,
        operation: str,
        *,
        enabled: bool | None = None,
    ) -> None:
        self._check_scope(tenant_id, lease.migration_id)
        _validate_lease_scope(lease, self._tenant_id, self._migration_id)
        async with self._transaction(tenant_id, lease=lease) as connection:
            scope = await self._load_scope(connection)
            state = await self._load_state(connection)
            if state is None:
                state = _initial_state(scope, self._tenant_id, self._migration_id)
                await self._insert_state(connection, state)
            else:
                _validate_state(state, self._tenant_id, self._migration_id)
            _assert_runtime_state(scope, state)

            if operation == "set_dual_write":
                assert enabled is not None
                state, switched = _set_dual_write(state, enabled)
            elif operation == "cutover":
                state, switched = _cutover_state(state)
            elif operation == "cleanup":
                state, switched = _cleanup_state(state)
            elif operation == "rollback":
                state, switched = _rollback_state(state)
            else:  # pragma: no cover - private callers cannot reach this
                raise AssertionError(f"unknown migration control operation: {operation}")

            if switched:
                if operation == "cutover":
                    app_version, tenant_version = await self._switch_profile(
                        connection,
                        scope,
                        state,
                        target=True,
                    )
                elif operation == "rollback":
                    app_version, tenant_version = await self._switch_profile(
                        connection,
                        scope,
                        state,
                        target=False,
                    )
                else:
                    app_version = None
                    tenant_version = None
                if app_version is not None and tenant_version is not None:
                    state["app_control_version"] = app_version
                    state["tenant_control_version"] = tenant_version
            await self._persist_state(connection, state)

    @asynccontextmanager
    async def _transaction(
        self, tenant_id: str, *, lease: MigrationLease | None = None
    ) -> AsyncIterator[asyncpg.Connection]:
        self._check_scope(tenant_id, self._migration_id)
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            await connection.fetchval(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", tenant_id
            )
            if lease is not None:
                await connection.execute(
                    """
                    SELECT set_config('app.migration_id', $1, true),
                           set_config('app.migration_owner_instance', $2, true),
                           set_config('app.migration_lease_epoch', $3, true)
                    """,
                    lease.migration_id,
                    lease.owner_instance,
                    str(lease.lease_epoch),
                )
                await self._assert_fence(connection, lease)
            yield connection

    async def _assert_fence(self, connection: asyncpg.Connection, lease: MigrationLease) -> None:
        row = await connection.fetchrow(
            """
            UPDATE migration_leases AS l
               SET updated_at=clock_timestamp()
              FROM migration_write_barriers AS b
             WHERE l.tenant_id=$1 AND l.migration_id=$2 AND l.owner_id=$3
               AND l.owner_instance=$4 AND l.lease_epoch=$5
               AND l.expires_at > clock_timestamp()
               AND b.tenant_id=l.tenant_id AND b.migration_id=l.migration_id
               AND b.owner_instance=l.owner_instance AND b.lease_epoch=l.lease_epoch
               AND b.mode='active'
            RETURNING l.tenant_id
            """,
            lease.tenant_id,
            lease.migration_id,
            lease.owner_id,
            lease.owner_instance,
            lease.lease_epoch,
        )
        if row is None:
            raise MigrationLeaseLost("migration lease or write barrier is stale or missing")

    async def _load_scope(self, connection: asyncpg.Connection) -> dict[str, Any]:
        manifest = await connection.fetchrow(
            """
            SELECT tenant_id,migration_id,app_id,app_revision,config_version,binding_id,
                   binding_revision
              FROM migration_scope_manifests
             WHERE tenant_id=$1 AND migration_id=$2
            """,
            self._tenant_id,
            self._migration_id,
        )
        if manifest is None:
            raise MigrationManifestConflict("production migration manifest is missing")

        app = await connection.fetchrow(
            """
            SELECT active_config_version,candidate_config_version,candidate_percent,control_version
              FROM agent_apps
             WHERE tenant_id=$1 AND app_id=$2
             FOR UPDATE
            """,
            self._tenant_id,
            _row(manifest, "app_id"),
        )
        if app is None:
            raise MigrationManifestConflict("migration application is missing")

        source_version = int(_row(manifest, "config_version"))
        source = await connection.fetchrow(
            """
            SELECT version, config_json #>> '{storage,profile_id}' AS profile_id
              FROM config_revisions
             WHERE tenant_id=$1 AND app_id=$2 AND version=$3
            """,
            self._tenant_id,
            _row(manifest, "app_id"),
            source_version,
        )
        if source is None:
            raise MigrationManifestConflict("migration source config revision is missing")

        target_version = _row(app, "candidate_config_version")
        if target_version is None:
            existing = await self._load_state(connection)
            if existing is None:
                raise MigrationGuardError("migration candidate config revision is not staged")
            target_version = existing.get("target_config_version")
        if target_version is None:
            raise MigrationGuardError("migration candidate config revision is not staged")
        target_version = int(target_version)
        target = await connection.fetchrow(
            """
            SELECT version, config_json #>> '{storage,profile_id}' AS profile_id
              FROM config_revisions
             WHERE tenant_id=$1 AND app_id=$2 AND version=$3
            """,
            self._tenant_id,
            _row(manifest, "app_id"),
            target_version,
        )
        if target is None:
            raise MigrationManifestConflict("migration target config revision is missing")

        source_profile = _row(source, "profile_id")
        target_profile = _row(target, "profile_id")
        if not isinstance(source_profile, str) or not source_profile:
            raise MigrationGuardError("migration source config has no storage profile")
        if not isinstance(target_profile, str) or not target_profile:
            raise MigrationGuardError("migration target config has no storage profile")
        if source_profile == target_profile:
            raise MigrationGuardError("migration source and target storage profiles must differ")
        profiles_ready = await connection.fetchval(
            """
            SELECT count(*)=2
              FROM storage_profiles
             WHERE tenant_id=$1 AND profile_id = ANY($2::text[])
            """,
            self._tenant_id,
            [source_profile, target_profile],
        )
        if profiles_ready is not True:
            raise MigrationGuardError("migration source and target storage profiles are not ready")

        binding = await connection.fetchrow(
            """
            SELECT app_id,control_version,enabled
              FROM channel_bindings
             WHERE tenant_id=$1 AND binding_id=$2
            """,
            self._tenant_id,
            _row(manifest, "binding_id"),
        )
        if (
            binding is None
            or _row(binding, "app_id") != _row(manifest, "app_id")
            or int(_row(binding, "control_version")) != int(_row(manifest, "binding_revision"))
            or _row(binding, "enabled") is not True
        ):
            raise MigrationManifestConflict("migration binding revision is not current")

        mailbox = await connection.fetchval("SELECT to_regclass('public.session_mailboxes')")
        if mailbox is None:
            raise MigrationGuardError("session mailbox v2 table is missing")
        tenant_version = await connection.fetchval(
            "SELECT control_version FROM tenants WHERE tenant_id=$1 FOR UPDATE", self._tenant_id
        )
        if tenant_version is None:
            raise MigrationManifestConflict("migration tenant is missing")
        return {
            "manifest": manifest,
            "app": app,
            "source_config_version": source_version,
            "target_config_version": target_version,
            "source_profile_id": source_profile,
            "target_profile_id": target_profile,
            "app_control_version": int(_row(app, "control_version")),
            "tenant_control_version": int(tenant_version),
        }

    async def _load_state(self, connection: asyncpg.Connection) -> dict[str, Any] | None:
        row = await connection.fetchrow(
            """
            SELECT differences,checksum
              FROM migration_checkpoints
             WHERE tenant_id=$1 AND migration_id=$2
               AND phase=$3 AND batch_key=$4
             FOR UPDATE
            """,
            self._tenant_id,
            self._control_migration_id,
            _CONTROL_PHASE,
            _CONTROL_BATCH,
        )
        if row is None:
            return None
        raw = _row(row, "differences")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError as error:
                raise MigrationGuardError(
                    "production migration control state is invalid"
                ) from error
        if not isinstance(raw, Mapping):
            raise MigrationGuardError("production migration control state is not an object")
        state = dict(raw)
        checksum = _row(row, "checksum")
        if checksum != _state_checksum(state):
            raise MigrationGuardError("production migration control state checksum mismatch")
        return state

    async def _insert_state(self, connection: asyncpg.Connection, state: dict[str, Any]) -> None:
        encoded = _encode_state(state)
        await connection.execute(
            """
            INSERT INTO migration_checkpoints (
                tenant_id,migration_id,phase,batch_key,source_count,target_count,
                checksum,differences,status
            ) VALUES ($1,$2,$3,$4,0,0,$5,$6::jsonb,'completed')
            ON CONFLICT (tenant_id,migration_id,phase,batch_key) DO NOTHING
            """,
            self._tenant_id,
            self._control_migration_id,
            _CONTROL_PHASE,
            _CONTROL_BATCH,
            _state_checksum(state),
            encoded,
        )
        stored = await self._load_state(connection)
        if stored is None:
            raise MigrationGuardError("production migration control state was not persisted")
        _validate_state(stored, self._tenant_id, self._migration_id)

    async def _persist_state(self, connection: asyncpg.Connection, state: dict[str, Any]) -> None:
        encoded = _encode_state(state)
        row = await connection.fetchrow(
            """
            UPDATE migration_checkpoints
               SET checksum=$5,differences=$6::jsonb,status='completed',updated_at=clock_timestamp()
             WHERE tenant_id=$1 AND migration_id=$2 AND phase=$3 AND batch_key=$4
            RETURNING migration_id
            """,
            self._tenant_id,
            self._control_migration_id,
            _CONTROL_PHASE,
            _CONTROL_BATCH,
            _state_checksum(state),
            encoded,
        )
        if row is None:
            raise MigrationGuardError("production migration control state disappeared")

    async def _switch_profile(
        self,
        connection: asyncpg.Connection,
        scope: Mapping[str, Any],
        state: Mapping[str, Any],
        *,
        target: bool,
    ) -> tuple[int, int]:
        expected_app_version = int(state["app_control_version"])
        expected_tenant_version = int(state["tenant_control_version"])
        source_version = int(state["source_config_version"])
        target_version = int(state["target_config_version"])
        next_version = target_version if target else source_version
        # A rollback may be requested while the candidate is only staged.  In
        # that case the source pointer is already active; the compare-and-set
        # must clear the candidate instead of pretending the target was active.
        current_active_version = int(_row(scope["app"], "active_config_version"))
        expected_active = source_version if target else current_active_version
        tenant = await connection.fetchrow(
            """
            UPDATE tenants
               SET control_version=control_version+1,updated_at=clock_timestamp()
             WHERE tenant_id=$1 AND control_version=$2
            RETURNING control_version
            """,
            self._tenant_id,
            expected_tenant_version,
        )
        if tenant is None:
            raise MigrationManifestConflict("tenant control version changed during migration")
        if target:
            updated = await connection.fetchrow(
                """
                UPDATE agent_apps
                   SET active_config_version=$3,candidate_config_version=NULL,
                       candidate_percent=0,control_version=control_version+1,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND app_id=$2 AND active_config_version=$4
                   AND control_version=$5 AND candidate_config_version=$3
                RETURNING control_version,active_config_version,candidate_config_version
                """,
                self._tenant_id,
                _row(scope["manifest"], "app_id"),
                next_version,
                expected_active,
                expected_app_version,
            )
        else:
            updated = await connection.fetchrow(
                """
                UPDATE agent_apps
                   SET active_config_version=$3,candidate_config_version=NULL,
                       candidate_percent=0,control_version=control_version+1,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=$1 AND app_id=$2 AND active_config_version=$4
                   AND control_version=$5
                   AND (candidate_config_version IS NULL OR candidate_config_version=$6)
                RETURNING control_version,active_config_version,candidate_config_version
                """,
                self._tenant_id,
                _row(scope["manifest"], "app_id"),
                next_version,
                expected_active,
                expected_app_version,
                target_version,
            )
        if updated is None:
            raise MigrationManifestConflict(
                "application config pointer changed or candidate is no longer staged"
            )
        # Keep this local check explicit: the query above is deliberately
        # compare-and-set and never silently activates an arbitrary revision.
        if int(_row(updated, "active_config_version")) != next_version:
            raise MigrationManifestConflict("application config pointer did not switch atomically")
        return int(_row(updated, "control_version")), int(_row(tenant, "control_version"))

    def _check_scope(self, tenant_id: str, migration_id: str) -> None:
        if tenant_id != self._tenant_id or migration_id != self._migration_id:
            raise MigrationManifestConflict("migration control scope does not match the factory")


def create(*, pool: asyncpg.Pool, tenant_id: str, migration_id: str) -> PostgresMigrationControl:
    """Allowlisted factory used by ``scripts.migrate_data``."""

    return PostgresMigrationControl(pool, tenant_id=tenant_id, migration_id=migration_id)


def _initial_state(scope: Mapping[str, Any], tenant_id: str, migration_id: str) -> dict[str, Any]:
    manifest = scope["manifest"]
    app = scope["app"]
    source_version = int(scope["source_config_version"])
    target_version = int(scope["target_config_version"])
    if int(_row(app, "active_config_version")) != source_version:
        raise MigrationManifestConflict("migration source config is not the active revision")
    if int(_row(app, "candidate_config_version")) != target_version:
        raise MigrationGuardError("migration target config is not staged as the candidate")
    if not _is_zero_percent(_row(app, "candidate_percent")):
        raise MigrationGuardError("migration target config is already receiving traffic")
    if int(scope["app_control_version"]) != int(_row(manifest, "app_revision")):
        raise MigrationManifestConflict("migration app control revision changed before start")
    return {
        "state_version": _STATE_VERSION,
        "tenant_id": tenant_id,
        "migration_id": migration_id,
        "source_config_version": source_version,
        "target_config_version": target_version,
        "source_profile_id": str(scope["source_profile_id"]),
        "target_profile_id": str(scope["target_profile_id"]),
        "app_control_version": int(scope["app_control_version"]),
        "tenant_control_version": int(scope["tenant_control_version"]),
        "dual_write": False,
        "active_profile": "source",
        "cleaned": False,
        "rolled_back": False,
        "mailbox_v2": "ready",
        "atomic_cutover": False,
        # The immutable source pointer and target revision were checked before
        # this state was persisted, so rollback is an observable capability,
        # not a boolean supplied by the caller.
        "rollback_verified": True,
    }


def _set_dual_write(state: dict[str, Any], enabled: bool) -> tuple[dict[str, Any], bool]:
    if state["rolled_back"] and enabled:
        raise MigrationGuardError("cannot enable dual-write after rollback")
    if state["cleaned"] and enabled:
        raise MigrationGuardError("cannot enable dual-write after cleanup")
    if state["dual_write"] is enabled:
        return state, False
    if enabled and state["active_profile"] != "source":
        raise MigrationGuardError("dual-write must begin while the source profile is active")
    next_state = dict(state)
    next_state["dual_write"] = enabled
    if enabled:
        next_state["mailbox_v2"] = "dual-write"
    elif next_state["active_profile"] == "source":
        next_state["mailbox_v2"] = "source" if next_state["rolled_back"] else "ready"
    else:
        next_state["mailbox_v2"] = "target"
    return next_state, True


def _cutover_state(state: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    if state["active_profile"] == "target" and state["atomic_cutover"]:
        return state, False
    if state["rolled_back"] or state["cleaned"]:
        raise MigrationGuardError("cannot cut over a terminal migration state")
    if not state["dual_write"] or state["active_profile"] != "source":
        raise MigrationGuardError("cutover requires source-active dual-write state")
    next_state = dict(state)
    next_state.update(active_profile="target", mailbox_v2="target", atomic_cutover=True)
    return next_state, True


def _cleanup_state(state: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    if state["cleaned"]:
        return state, False
    if state["rolled_back"]:
        raise MigrationGuardError("cannot cleanup a rolled-back migration")
    if state["active_profile"] != "target" or not state["atomic_cutover"]:
        raise MigrationGuardError("cleanup requires an atomic target cutover")
    next_state = dict(state)
    next_state.update(dual_write=False, cleaned=True, mailbox_v2="target")
    return next_state, True


def _rollback_state(state: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    if state["rolled_back"]:
        return state, False
    next_state = dict(state)
    # Clear a staged candidate even when rollback is requested before
    # cutover; this makes the source pointer terminal and unambiguous.
    switched = True
    next_state.update(
        dual_write=False,
        active_profile="source",
        cleaned=False,
        rolled_back=True,
        mailbox_v2="source",
    )
    return next_state, switched


def _assert_runtime_state(scope: Mapping[str, Any], state: Mapping[str, Any]) -> None:
    app = scope["app"]
    if int(_row(app, "control_version")) != int(state["app_control_version"]):
        raise MigrationManifestConflict("application control version changed outside migration")
    if int(scope["tenant_control_version"]) != int(state["tenant_control_version"]):
        raise MigrationManifestConflict("tenant control version changed outside migration")
    source = int(state["source_config_version"])
    target = int(state["target_config_version"])
    active = int(_row(app, "active_config_version"))
    candidate = _row(app, "candidate_config_version")
    if not _is_zero_percent(_row(app, "candidate_percent")):
        raise MigrationManifestConflict("application candidate rollout changed outside migration")
    expected_active = source if state["active_profile"] == "source" else target
    if active != expected_active:
        raise MigrationManifestConflict("application active config does not match control state")
    if state["active_profile"] == "source":
        if state["rolled_back"]:
            if candidate is not None:
                raise MigrationManifestConflict("rollback left a candidate config staged")
        elif candidate is None or int(candidate) != target:
            raise MigrationManifestConflict(
                "application candidate config does not match control state"
            )
    if state["active_profile"] == "target" and candidate is not None:
        raise MigrationManifestConflict("application candidate config was changed after cutover")
    if state["source_profile_id"] != scope["source_profile_id"]:
        raise MigrationManifestConflict("migration source storage profile changed")
    if state["target_profile_id"] != scope["target_profile_id"]:
        raise MigrationManifestConflict("migration target storage profile changed")


def _validate_state(state: Mapping[str, Any], tenant_id: str, migration_id: str) -> None:
    required = {
        "state_version",
        "tenant_id",
        "migration_id",
        "source_config_version",
        "target_config_version",
        "source_profile_id",
        "target_profile_id",
        "app_control_version",
        "tenant_control_version",
        "dual_write",
        "active_profile",
        "cleaned",
        "rolled_back",
        "mailbox_v2",
        "atomic_cutover",
        "rollback_verified",
    }
    if set(state) != required:
        raise MigrationGuardError("production migration control state schema is invalid")
    if state["tenant_id"] != tenant_id or state["migration_id"] != migration_id:
        raise MigrationManifestConflict("production migration control state scope changed")
    if state["state_version"] != _STATE_VERSION or state["active_profile"] not in _ACTIVE_PROFILES:
        raise MigrationGuardError("production migration control state version is invalid")
    if state["mailbox_v2"] not in _MAILBOX_STATES:
        raise MigrationGuardError("production migration mailbox state is invalid")
    for key in ("dual_write", "cleaned", "rolled_back", "atomic_cutover", "rollback_verified"):
        if type(state[key]) is not bool:
            raise MigrationGuardError("production migration control boolean state is invalid")
    for key in (
        "source_config_version",
        "target_config_version",
        "app_control_version",
        "tenant_control_version",
    ):
        if isinstance(state[key], bool) or not isinstance(state[key], int) or state[key] < 1:
            raise MigrationGuardError("production migration control revision state is invalid")
    if state["source_config_version"] == state["target_config_version"]:
        raise MigrationGuardError("production migration config revisions must differ")
    if (
        not isinstance(state["source_profile_id"], str)
        or not state["source_profile_id"]
        or not isinstance(state["target_profile_id"], str)
        or not state["target_profile_id"]
        or state["source_profile_id"] == state["target_profile_id"]
    ):
        raise MigrationGuardError("production migration profile state is invalid")


def _validate_lease_scope(lease: MigrationLease, tenant_id: str, migration_id: str) -> None:
    if not isinstance(lease, MigrationLease):
        raise TypeError("fenced migration control requires a MigrationLease")
    if lease.tenant_id != tenant_id or lease.migration_id != migration_id:
        raise MigrationLeaseLost("migration lease is outside the control scope")


def _is_zero_percent(value: Any) -> bool:
    try:
        return value is not None and float(value) == 0.0
    except (TypeError, ValueError, OverflowError):
        return False


def _validate_identifier(value: str, label: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValueError(f"{label} must be 1-256 characters")


def _row(row: Any, key: str) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError, TypeError) as error:
        raise MigrationGuardError(f"production migration query omitted {key}") from error


def _encode_state(state: Mapping[str, Any]) -> str:
    return json.dumps(dict(state), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _state_checksum(state: Mapping[str, Any]) -> str:
    return hashlib.sha256(_encode_state(state).encode("utf-8")).hexdigest()


ProductionMigrationControl = PostgresMigrationControl

__all__ = ["PostgresMigrationControl", "ProductionMigrationControl", "create"]
