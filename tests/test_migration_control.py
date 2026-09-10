from __future__ import annotations

import unittest
from contextlib import contextmanager
from datetime import timedelta
from unittest.mock import MagicMock, patch

from trpc_service.storage.base import now_utc
from trpc_service.storage.migration_control import (
    MigrationCheckpointConflict,
    MigrationControlError,
    MigrationLeaseBusy,
    MigrationLeaseLost,
    PostgresMigrationControl,
    _connection_error,
    _lease_seconds,
    ensure_migration_control_schema,
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self._result = None
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        normalized = " ".join(str(query).split())
        self._result = None
        self.rowcount = 0
        state = self.connection.state

        if normalized.startswith("INSERT INTO migration_lease"):
            key = (params[0], params[1])
            if key not in state["leases"]:
                state["leases"][key] = {
                    "owner_id": params[2],
                    "owner_instance": params[3],
                    "lease_epoch": 1,
                    "expires_at": params[4],
                }
                self.rowcount = 1
            return
        if normalized.startswith("SELECT owner_id,owner_instance,lease_epoch,expires_at"):
            item = state["leases"][(params[0], params[1])]
            self._result = (
                item["owner_id"],
                item["owner_instance"],
                item["lease_epoch"],
                item["expires_at"],
            )
            return
        if normalized.startswith("UPDATE migration_lease SET owner_id="):
            key = (params[5], params[6])
            item = state["leases"][key]
            item.update(
                owner_id=params[0],
                owner_instance=params[1],
                lease_epoch=params[2],
                expires_at=params[3],
            )
            self.rowcount = 1
            return
        if normalized.startswith("INSERT INTO migration_write_barrier"):
            key = (params[0], params[1])
            state["barriers"][key] = {
                "owner_instance": params[2],
                "lease_epoch": params[3],
                "mode": "active",
            }
            self.rowcount = 1
            return
        if normalized.startswith("SELECT 1 FROM migration_lease AS l JOIN migration_write_barrier"):
            if "l.owner_id=%s" in normalized:
                tenant_id, migration_id, owner_id, owner_instance, epoch, current_time = params
            else:
                tenant_id, migration_id, owner_instance, epoch, current_time = params
                owner_id = None
            lease = state["leases"].get((tenant_id, migration_id))
            barrier = state["barriers"].get((tenant_id, migration_id))
            if (
                lease
                and barrier
                and (owner_id is None or lease["owner_id"] == owner_id)
                and lease["owner_instance"] == owner_instance
                and lease["lease_epoch"] == epoch
                and lease["expires_at"] > current_time
                and barrier["owner_instance"] == owner_instance
                and barrier["lease_epoch"] == epoch
                and barrier["mode"] == "active"
            ):
                self._result = (1,)
            return
        if normalized.startswith("SELECT mode FROM migration_write_barrier"):
            barrier = state["barriers"].get((params[0], params[1]))
            if barrier and barrier["owner_instance"] == params[2] and barrier["lease_epoch"] == params[3]:
                self._result = (barrier["mode"],)
            return
        if normalized.startswith("UPDATE migration_lease SET expires_at="):
            key = (params[2], params[3])
            item = state["leases"].get(key)
            if item and (
                item["owner_id"],
                item["owner_instance"],
                item["lease_epoch"],
            ) == (params[4], params[5], params[6]):
                item["expires_at"] = params[0]
                self.rowcount = 1
            return
        if normalized.startswith("UPDATE migration_write_barrier SET mode='released'"):
            key = (params[1], params[2])
            barrier = state["barriers"].get(key)
            if barrier and (
                barrier["owner_instance"],
                barrier["lease_epoch"],
            ) == (params[3], params[4]):
                barrier["mode"] = "released"
                self.rowcount = 1
            return
        if normalized.startswith("INSERT INTO migration_checkpoint"):
            key = (params[0], params[1], params[2], params[3])
            state["checkpoints"][key] = {
                "cursor": params[4],
                "source_count": params[5],
                "target_count": params[6],
                "status": params[7],
                "checksum": params[8],
                "updated_at": params[9],
            }
            self.rowcount = 1
            return
        if normalized.startswith("SELECT cursor_json,source_count,target_count,status,checksum,updated_at"):
            item = state["checkpoints"].get((params[0], params[1], params[2], params[3]))
            if item:
                self._result = (
                    item["cursor"],
                    item["source_count"],
                    item["target_count"],
                    item["status"],
                    item["checksum"],
                    item["updated_at"],
                )
            return
        raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchone(self):
        return self._result


class FakeConnection:
    def __init__(self):
        self.state = {"leases": {}, "barriers": {}, "checkpoints": {}}

    @contextmanager
    def transaction(self):
        yield self

    def cursor(self):
        return FakeCursor(self)


class MigrationControlTests(unittest.TestCase):
    def setUp(self):
        self.connection = FakeConnection()
        self.control = PostgresMigrationControl(self.connection)

    def test_live_lease_is_exclusive_and_expired_lease_is_fenced(self):
        first = self.control.acquire("tenant", "migration", "owner-a", "instance-a")
        with self.assertRaises(MigrationLeaseBusy):
            self.control.acquire("tenant", "migration", "owner-b", "instance-b")

        self.connection.state["leases"][("tenant", "migration")]["expires_at"] = (
            now_utc() - timedelta(seconds=1)
        )
        second = self.control.acquire("tenant", "migration", "owner-b", "instance-b")
        self.assertEqual(second.lease_epoch, first.lease_epoch + 1)
        with self.assertRaises(MigrationLeaseLost):
            self.control.assert_fence(first)

    def test_write_barrier_rejects_expired_authority(self):
        lease = self.control.acquire("tenant", "migration", "owner-a", "instance-a")
        self.control.assert_write_allowed("tenant", "migration", "instance-a", lease.lease_epoch)
        self.connection.state["leases"][("tenant", "migration")]["expires_at"] = (
            now_utc() - timedelta(seconds=1)
        )
        with self.assertRaises(MigrationLeaseLost):
            self.control.assert_write_allowed("tenant", "migration", "instance-a", lease.lease_epoch)

    def test_checkpoint_checksum_detects_corruption(self):
        lease = self.control.acquire("tenant", "migration", "owner-a", "instance-a")
        checkpoint = self.control.save_checkpoint(
            lease,
            "backfill",
            "batch-1",
            {"last_id": "42"},
            source_count=10,
            target_count=10,
        )
        loaded = self.control.load_checkpoint("tenant", "migration", "backfill", "batch-1")
        self.assertEqual(loaded, checkpoint)

        stored = self.connection.state["checkpoints"][("tenant", "migration", "backfill", "batch-1")]
        stored["cursor"] = '{"last_id": "43"}'
        with self.assertRaises(MigrationCheckpointConflict):
            self.control.load_checkpoint("tenant", "migration", "backfill", "batch-1")

    def test_renew_release_validation_and_input_guards(self):
        with self.assertRaises(ValueError):
            self.control.acquire("", "migration", "owner", "instance")
        with self.assertRaises(ValueError):
            self.control.acquire("tenant", "", "owner", "instance")
        with self.assertRaises(ValueError):
            self.control.acquire("tenant", "migration", "", "instance")
        with self.assertRaises(ValueError):
            self.control.acquire("tenant", "migration", "owner", "instance", lease_seconds=4)
        with self.assertRaises(ValueError):
            self.control.acquire("tenant", "migration", "owner", "instance", lease_seconds="bad")

        lease = self.control.acquire("tenant", "migration", "owner", "instance")
        renewed = self.control.renew(lease, lease_seconds=5)
        self.assertEqual(renewed.lease_epoch, lease.lease_epoch)
        self.control.assert_fence(renewed)
        self.control.release(renewed)
        with self.assertRaises(MigrationLeaseLost):
            self.control.assert_fence(renewed)

        with self.assertRaises(TypeError):
            self.control.save_checkpoint(renewed, "phase", "batch", [])
        with self.assertRaises(ValueError):
            self.control.save_checkpoint(renewed, "phase", "batch", {}, source_count=-1)
        with self.assertRaises(ValueError):
            self.control.save_checkpoint(renewed, "", "batch", {})
        with self.assertRaises(ValueError):
            self.control.save_checkpoint(renewed, "phase", "", {})
        self.assertIsNone(self.control.load_checkpoint("tenant", "migration", "missing", "batch"))

        stored = self.connection.state["checkpoints"]
        stored["bad"] = None
        self.assertTrue(_connection_error(type("OperationalError", (Exception,), {"__module__": "psycopg.errors"})()))
        self.assertFalse(_connection_error(RuntimeError("ordinary")))
        self.assertEqual(_lease_seconds(5), 5)

    def test_schema_initialization_uses_advisory_lock(self):
        connection = MagicMock()
        cursor = MagicMock()
        cursor.__enter__.return_value = cursor
        connection.cursor.return_value = cursor
        @contextmanager
        def no_op():
            yield
        with patch("trpc_service.storage.migration_control.postgres_advisory_lock", return_value=no_op()):
            ensure_migration_control_schema(connection)
        cursor.execute.assert_called_once()

        with self.assertRaises(MigrationControlError):
            # The guard is also exercised through an acquisition with a missing row.
            class MissingCursor(FakeCursor):
                def execute(self, query, params=None):
                    if "SELECT owner_id,owner_instance,lease_epoch,expires_at" in " ".join(str(query).split()):
                        self._result = None
                        return
                    return super().execute(query, params)

                def fetchone(self):
                    return None
            class MissingConnection(FakeConnection):
                def cursor(self):
                    return MissingCursor(self)
            PostgresMigrationControl(MissingConnection()).acquire("tenant", "migration", "owner", "instance")


if __name__ == "__main__":
    unittest.main()
