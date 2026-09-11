from __future__ import annotations

import unittest
from pathlib import Path


class ProductionMigrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sql = (Path(__file__).parents[1] / "migrations" / "0001_production.sql").read_text(encoding="utf-8")

    def test_control_role_has_explicit_cross_tenant_policy(self) -> None:
        self.assertIn("CREATE POLICY tenant_control", self.sql)
        self.assertIn("FOR ALL TO trpc_control USING (true) WITH CHECK (true)", self.sql)
        self.assertNotIn("ALTER ROLE trpc_control BYPASSRLS", self.sql)

    def test_inbox_reconciliation_index_covers_queued_messages(self) -> None:
        self.assertIn("DROP INDEX IF EXISTS ix_inbound_reconcile", self.sql)
        self.assertIn("WHERE status IN ('accepted','queued')", self.sql)
