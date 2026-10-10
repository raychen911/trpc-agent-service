from __future__ import annotations

import json
import re
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest

from trpc_service.storage.migration import MigrationLease, MigrationLeaseLost
from trpc_service.storage.production_migration_control import create


class _Connection:
    def __init__(self, database: _Database) -> None:
        self.database = database

    def transaction(self) -> _Connection:
        return self

    async def __aenter__(self) -> _Connection:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, query: str, *args: object) -> None:
        self._check_bind_count(query, args)
        normalized = " ".join(query.split())
        self.database.queries.append((normalized, args))
        if normalized.startswith("INSERT INTO migration_checkpoints"):
            self.database.control_state = json.loads(str(args[-1]))

    async def fetchval(self, query: str, *args: object) -> object:
        self._check_bind_count(query, args)
        normalized = " ".join(query.split())
        self.database.queries.append((normalized, args))
        if "pg_advisory_xact_lock" in normalized:
            return 0
        if "count(*)=2" in normalized:
            return True
        if "to_regclass('public.session_mailboxes')" in normalized:
            return "session_mailboxes"
        if "SELECT control_version FROM tenants" in normalized:
            return self.database.tenant_control_version
        raise AssertionError(f"unexpected fetchval: {normalized}")

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        self._check_bind_count(query, args)
        normalized = " ".join(query.split())
        self.database.queries.append((normalized, args))
        if normalized.startswith("UPDATE migration_leases AS l"):
            return {"tenant_id": self.database.tenant_id} if self.database.lease_valid else None
        if "FROM migration_scope_manifests" in normalized:
            return dict(self.database.manifest)
        if "FROM agent_apps" in normalized and normalized.startswith("SELECT"):
            return dict(self.database.app)
        if "FROM config_revisions" in normalized:
            version = int(args[-1])
            return {"version": version, "profile_id": self.database.configs[version]}
        if "FROM channel_bindings" in normalized:
            return dict(self.database.binding)
        if "FROM migration_checkpoints" in normalized:
            if self.database.control_state is None:
                return None
            return {
                "differences": dict(self.database.control_state),
                "checksum": self._checksum(self.database.control_state),
            }
        if normalized.startswith("UPDATE tenants"):
            expected = int(args[-1])
            if expected != self.database.tenant_control_version:
                return None
            self.database.tenant_control_version += 1
            return {"control_version": self.database.tenant_control_version}
        if normalized.startswith("UPDATE agent_apps"):
            tenant, app_id, next_version, expected_active, expected_control = args[:5]
            if (
                tenant != self.database.tenant_id
                or app_id != self.database.manifest["app_id"]
                or int(expected_active) != int(self.database.app["active_config_version"])
                or int(expected_control) != int(self.database.app["control_version"])
            ):
                return None
            if "candidate_config_version=$3" in normalized:
                if self.database.app["candidate_config_version"] != next_version:
                    return None
            else:
                target_version = int(args[5])
                candidate = self.database.app["candidate_config_version"]
                if candidate not in (None, target_version):
                    return None
            self.database.app.update(
                active_config_version=int(next_version),
                candidate_config_version=None,
                candidate_percent=0,
                control_version=int(self.database.app["control_version"]) + 1,
            )
            return {
                "control_version": self.database.app["control_version"],
                "active_config_version": self.database.app["active_config_version"],
                "candidate_config_version": None,
            }
        if normalized.startswith("UPDATE migration_checkpoints"):
            if self.database.control_state is None:
                return None
            self.database.control_state = json.loads(str(args[-1]))
            return {"migration_id": args[1]}
        raise AssertionError(f"unexpected fetchrow: {normalized}")

    @staticmethod
    def _checksum(value: dict[str, object]) -> str:
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        import hashlib

        return hashlib.sha256(encoded.encode()).hexdigest()

    @staticmethod
    def _check_bind_count(query: str, args: tuple[object, ...]) -> None:
        placeholders = {int(value) for value in re.findall(r"\$(\d+)", query)}
        if not placeholders:
            if args:
                raise AssertionError("SQL supplied arguments without placeholders")
            return
        expected = set(range(1, max(placeholders) + 1))
        if placeholders != expected or len(args) != max(placeholders):
            raise AssertionError(
                f"SQL bind mismatch: placeholders={sorted(placeholders)} args={len(args)}"
            )


class _Pool:
    def __init__(self, database: _Database) -> None:
        self.database = database

    @asynccontextmanager
    async def acquire(self):
        yield _Connection(self.database)


class _Database:
    tenant_id = "tenant-a"
    migration_id = "redis-pg-a"

    def __init__(self) -> None:
        self.manifest = {
            "tenant_id": self.tenant_id,
            "migration_id": self.migration_id,
            "app_id": "assistant",
            "app_revision": 7,
            "config_version": 1,
            "binding_id": "binding-a",
            "binding_revision": 3,
        }
        self.app = {
            "active_config_version": 1,
            "candidate_config_version": 2,
            "candidate_percent": 0,
            "control_version": 7,
        }
        self.configs = {1: "redis-source", 2: "postgres-target"}
        self.binding = {"app_id": "assistant", "control_version": 3, "enabled": True}
        self.tenant_control_version = 11
        self.control_state: dict[str, object] | None = None
        self.lease_valid = True
        self.queries: list[tuple[str, tuple[object, ...]]] = []

    def pool(self) -> _Pool:
        return _Pool(self)


def _lease(database: _Database) -> MigrationLease:
    return MigrationLease(
        tenant_id=database.tenant_id,
        migration_id=database.migration_id,
        owner_id="migration-worker",
        owner_instance="run-instance",
        lease_epoch=4,
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )


@pytest.mark.asyncio
async def test_factory_persists_observable_state_and_unfenced_path_fails_closed() -> None:
    database = _Database()
    first = create(
        pool=database.pool(), tenant_id=database.tenant_id, migration_id=database.migration_id
    )
    second = create(
        pool=database.pool(), tenant_id=database.tenant_id, migration_id=database.migration_id
    )

    with pytest.raises(MigrationLeaseLost, match="fenced lease"):
        await first.set_dual_write(database.tenant_id, True)

    state = await first.read_state(database.tenant_id, database.migration_id)
    assert state["active_profile"] == "source"
    assert state["rollback_verified"] is True
    assert database.control_state is not None
    # A second factory reads the committed PostgreSQL-shaped record rather
    # than inheriting any object-local state.
    assert await second.read_state(database.tenant_id, database.migration_id) == state
    assert any("INSERT INTO migration_checkpoints" in query for query, _ in database.queries)


@pytest.mark.asyncio
async def test_fenced_lifecycle_switches_real_pointers_and_is_idempotent() -> None:
    database = _Database()
    control = create(
        pool=database.pool(), tenant_id=database.tenant_id, migration_id=database.migration_id
    )
    lease = _lease(database)

    await control.set_dual_write_fenced(database.tenant_id, True, lease=lease)
    assert database.control_state is not None
    assert database.control_state["mailbox_v2"] == "dual-write"

    await control.cutover_fenced(database.tenant_id, lease=lease)
    assert database.app["active_config_version"] == 2
    assert database.app["candidate_config_version"] is None
    assert database.control_state["active_profile"] == "target"
    assert database.control_state["atomic_cutover"] is True

    await control.cleanup_fenced(database.tenant_id, lease=lease)
    await control.set_dual_write_fenced(database.tenant_id, False, lease=lease)
    assert database.control_state["cleaned"] is True
    assert database.control_state["dual_write"] is False

    await control.rollback_fenced(database.tenant_id, lease=lease)
    await control.rollback_fenced(database.tenant_id, lease=lease)
    assert database.app["active_config_version"] == 1
    assert database.app["candidate_config_version"] is None
    assert database.control_state["active_profile"] == "source"
    assert database.control_state["rolled_back"] is True
    assert database.control_state["rollback_verified"] is True


@pytest.mark.asyncio
async def test_rollback_before_cutover_clears_staged_candidate() -> None:
    database = _Database()
    control = create(
        pool=database.pool(), tenant_id=database.tenant_id, migration_id=database.migration_id
    )

    await control.rollback_fenced(database.tenant_id, lease=_lease(database))

    assert database.app["active_config_version"] == 1
    assert database.app["candidate_config_version"] is None
    assert database.control_state is not None
    assert database.control_state["active_profile"] == "source"
    assert database.control_state["rolled_back"] is True
    assert database.control_state["mailbox_v2"] == "source"


@pytest.mark.asyncio
async def test_stale_owner_or_epoch_is_rejected_before_state_write() -> None:
    database = _Database()
    control = create(
        pool=database.pool(), tenant_id=database.tenant_id, migration_id=database.migration_id
    )
    database.lease_valid = False

    with pytest.raises(MigrationLeaseLost, match="stale or missing"):
        await control.set_dual_write_fenced(database.tenant_id, True, lease=_lease(database))
    assert database.control_state is None
    assert not any("UPDATE migration_checkpoints" in query for query, _ in database.queries)
