from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from threading import Thread
from unittest.mock import patch

import trpc_service.migrate as migration_module
from trpc_service.migrate import (
    _counts,
    _migration_manifest,
    _snapshot_fingerprint,
    execute_migration,
    export_tenant,
    import_tenant,
)
from trpc_service.migration_state import MigrationPhase, new_migration
from trpc_service.storage.base import SessionEvent
from trpc_service.storage.factory import create_storage
from trpc_service.storage.in_memory import InMemoryStorage
from trpc_service.storage.locking import SessionLeaseLost
from trpc_service.storage.sql_store import SQLiteStorage
from trpc_service.storage.tool_governance import ApprovalStatus, arguments_hash


class ProductionDepthTests(unittest.TestCase):
    def test_memory_mailbox_serializes_session_and_fences_recovery(self):
        storage = InMemoryStorage()
        first = storage.mailbox.enqueue("tenant", "session", "m1", "k1", {"text": "one"})
        second = storage.mailbox.enqueue("tenant", "session", "m2", "k2", {"text": "two"})
        claim_one = storage.mailbox.claim_next("tenant", "session", "worker-a", 10)
        self.assertEqual([first.sequence, second.sequence], [1, 2])
        self.assertEqual(claim_one.sequence, 1)
        self.assertIsNone(storage.mailbox.claim_next("tenant", "session", "worker-b", 10))
        storage.mailbox.complete(claim_one)
        claim_two = storage.mailbox.claim_next("tenant", "session", "worker-b", 10)
        self.assertEqual(claim_two.sequence, 2)
        with self.assertRaises(SessionLeaseLost):
            storage.mailbox.complete(claim_one)

    def test_sqlite_mailbox_persists_order_and_reclaims_expired_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = SQLiteStorage(Path(directory) / "state.sqlite3")
            storage.mailbox.enqueue("tenant", "session", "m1", "k1", {"text": "one"})
            claimed = storage.mailbox.claim_next("tenant", "session", "worker-a", 1)
            self.assertEqual(claimed.sequence, 1)
            time.sleep(1.05)
            storage.mailbox.recover_expired("tenant", "session")
            reclaimed = storage.mailbox.claim_next("tenant", "session", "worker-b", 10)
            self.assertEqual(reclaimed.sequence, 1)
            with self.assertRaises(SessionLeaseLost):
                storage.mailbox.complete(claimed)
            storage.mailbox.complete(reclaimed)
            storage.close()

    def test_sqlite_tool_approval_detects_ambiguity_and_budget_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = SQLiteStorage(Path(directory) / "state.sqlite3")
            governance = storage.tool_governance
            args_hash = arguments_hash({"target": "ops"})
            record = governance.create_or_get(
                "tenant", "approval-1", "session", "request-1", "send", args_hash
            )
            self.assertEqual(record.status, ApprovalStatus.PENDING)
            governance.consume("tenant", "approval-1", "request-1", args_hash)
            ambiguous = governance.create_or_get(
                "tenant",
                "approval-1",
                "session",
                "request-2",
                "send",
                arguments_hash({"target": "finance"}),
            )
            self.assertEqual(ambiguous.status, ApprovalStatus.AMBIGUOUS)
            first = governance.reserve_call("tenant", "request-1", "call-1", True, 1, 1)
            second = governance.reserve_call("tenant", "request-1", "call-1", True, 1, 1)
            self.assertEqual(first.total_calls, second.total_calls)
            self.assertEqual(second.side_effect_calls, 1)
            storage.close()

    def test_sqlite_tool_execution_ledger_replays_success_and_rejects_identity_conflicts(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = SQLiteStorage(Path(directory) / "state.sqlite3")
            governance = storage.tool_governance
            execution = governance.begin_execution(
                "tenant", "execution-1", "request-1", "session", "send",
                "call-1", arguments_hash({"target": "ops"}), True, 3,
            )
            self.assertEqual(execution.status, "running")
            completed = governance.complete_execution(
                "tenant", "call-1", {"content": "sent", "metadata": {}}, 3,
            )
            self.assertEqual(completed.status, "succeeded")
            replay = governance.begin_execution(
                "tenant", "execution-2", "request-1", "session", "send",
                "call-1", arguments_hash({"target": "ops"}), True, 3,
            )
            self.assertEqual(replay.status, "succeeded")
            conflict = governance.begin_execution(
                "tenant", "execution-3", "request-1", "session", "send",
                "call-1", arguments_hash({"target": "finance"}), True, 3,
            )
            self.assertEqual(conflict.status, "ambiguous")
            storage.close()

    def test_mailbox_sequence_assignment_is_atomic_in_memory(self):
        storage = InMemoryStorage()
        results = []

        def enqueue(index):
            results.append(
                storage.mailbox.enqueue(
                    "tenant", "parallel", f"m-{index}", f"k-{index}", {"index": index}
                ).sequence
            )

        threads = [Thread(target=enqueue, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(results), list(range(1, 9)))

    def test_migration_runner_executes_cutover_and_persists_state(self):
        with tempfile.TemporaryDirectory() as directory:
            source = SQLiteStorage(Path(directory) / "data" / "source" / "trpc_service.sqlite3")
            source.session.append_event(
                SessionEvent(
                    "tenant",
                    "session",
                    "event-1",
                    "user_message",
                    {"text": "hello"},
                    "trace",
                    "message-1",
                )
            )
            source.mailbox.enqueue("tenant", "session", "message-1", "message-1", {"text": "hello"})
            source.close()
            result = execute_migration(
                "tenant",
                "sql",
                "sql",
                state_path=Path(directory) / "migration.json",
                snapshot_path=Path(directory) / "snapshot.json",
                data_dir=Path(directory) / "data",
            )
            self.assertEqual(result["phase"], "cleanup")
            self.assertEqual(result["metadata"]["verification"]["ok"], True)
            self.assertTrue((Path(directory) / "snapshot.json").exists())

    def test_migration_runner_records_deterministic_failure_and_rollback(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(RuntimeError, msg="injected migration fault should fail the run"):
                execute_migration(
                    "tenant",
                    "sql",
                    "sql",
                    state_path=Path(directory) / "migration.json",
                    snapshot_path=Path(directory) / "snapshot.json",
                    data_dir=Path(directory) / "data",
                    inject_failure_phase="verify",
                )
            state = json.loads((Path(directory) / "migration.json").read_text(encoding="utf-8"))
            transitions = {(item["from"], item["to"]) for item in state["history"]}
            self.assertEqual(state["phase"], "rolled_back")
            self.assertIn(("failed", "rolled_back"), transitions)

    def test_migration_rejects_nonempty_target_before_backfill(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = SQLiteStorage(root / "data" / "target" / "trpc_service.sqlite3")
            target.mailbox.enqueue("tenant", "session", "existing", "existing", {"text": "old"})
            target.close()

            with self.assertRaisesRegex(RuntimeError, "target tenant is not empty"):
                execute_migration(
                    "tenant",
                    "sql",
                    "sql",
                    state_path=root / "migration.json",
                    snapshot_path=root / "snapshot.json",
                    data_dir=root / "data",
                )

            state = json.loads((root / "migration.json").read_text(encoding="utf-8"))
            self.assertEqual(state["phase"], "rolled_back")
            self.assertIn(("failed", "rolled_back"), {
                (item["from"], item["to"]) for item in state["history"]
            })

    def test_migration_rejects_state_manifest_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = new_migration(
                "tenant:sql:sql",
                "tenant",
                "sql",
                "sql",
                metadata={"manifest": _migration_manifest("other-tenant", "sql", "sql")},
            )
            (root / "migration.json").write_text(
                json.dumps(state.to_dict(), indent=2),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "manifest is immutable"):
                execute_migration(
                    "tenant",
                    "sql",
                    "sql",
                    state_path=root / "migration.json",
                    snapshot_path=root / "snapshot.json",
                    data_dir=root / "data",
                )

    def test_migration_resumes_from_persisted_shadow_read_phase(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = create_storage(migration_module._profile("sql", "", ""), root / "data" / "source")
            source.session.append_event(
                SessionEvent(
                    "tenant",
                    "session",
                    "event-1",
                    "user_message",
                    {"text": "resume"},
                    "trace",
                    "message-1",
                )
            )
            snapshot = export_tenant(source, "tenant")
            source.close()
            target = create_storage(migration_module._profile("sql", "", ""), root / "data" / "target")
            import_tenant(target, snapshot)
            target.close()

            state = new_migration(
                "tenant:sql:sql",
                "tenant",
                "sql",
                "sql",
                metadata={
                    "manifest": _migration_manifest("tenant", "sql", "sql"),
                    "snapshot_fingerprint": _snapshot_fingerprint(snapshot),
                    "snapshot_counts": _counts(snapshot),
                },
            )
            state.transition(MigrationPhase.BACKFILL, actor="test")
            state.transition(MigrationPhase.SHADOW_READ, actor="test")
            state_path = root / "migration.json"
            state_path.write_text(json.dumps(state.to_dict(), indent=2), encoding="utf-8")
            snapshot_path = root / "snapshot.json"
            snapshot_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")

            result = execute_migration(
                "tenant",
                "sql",
                "sql",
                state_path=state_path,
                snapshot_path=snapshot_path,
                data_dir=root / "data",
            )
            self.assertEqual(result["phase"], "cleanup")
            self.assertTrue(result["metadata"]["verification"]["ok"])

    def test_migration_rolls_back_when_source_changes_after_dual_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_path = (root / "data" / "source" / "trpc_service.sqlite3").resolve()
            source = create_storage(migration_module._profile("sql", "", ""), root / "data" / "source")
            source.session.append_event(
                SessionEvent(
                    "tenant",
                    "session",
                    "event-1",
                    "user_message",
                    {"text": "baseline"},
                    "trace",
                    "message-1",
                )
            )
            source.close()

            original_export = migration_module.export_tenant
            source_exports = 0

            def export_and_mutate(storage, tenant_id):
                nonlocal source_exports
                result = original_export(storage, tenant_id)
                if Path(storage.structured.path).resolve() == source_path:
                    source_exports += 1
                    if source_exports == 2:
                        changed = create_storage(
                            migration_module._profile("sql", "", ""),
                            root / "data" / "source",
                        )
                        changed.session.append_event(
                            SessionEvent(
                                "tenant",
                                "session",
                                "event-2",
                                "user_message",
                                {"text": "changed after baseline"},
                                "trace-2",
                                "message-2",
                            )
                        )
                        changed.close()
                return result

            with patch.object(migration_module, "export_tenant", side_effect=export_and_mutate):
                with self.assertRaisesRegex(RuntimeError, "source changed"):
                    execute_migration(
                        "tenant",
                        "sql",
                        "sql",
                        state_path=root / "migration.json",
                        snapshot_path=root / "snapshot.json",
                        data_dir=root / "data",
                    )

            state = json.loads((root / "migration.json").read_text(encoding="utf-8"))
            self.assertEqual(state["phase"], "rolled_back")
            self.assertIn(("failed", "rolled_back"), {
                (item["from"], item["to"]) for item in state["history"]
            })


if __name__ == "__main__":
    unittest.main()
