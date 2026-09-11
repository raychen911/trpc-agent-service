"""An unknown provider charge blocks summary retries without affecting another tenant."""

import pytest
from tests.reliability.test_budget import seed, reserve
from trpc_service.persistence import Database
from trpc_service.service_runtime import ServiceRuntime
from trpc_service.governance.budget import BudgetDenied


def test_summary_cannot_hide_or_repeat_an_unreconciled_model_call(tmp_path):
    database = Database("sqlite:///" + (tmp_path / "summary-budget.db").as_posix())
    database.create_schema()
    try:
        ledger = seed(database)
        reserve(ledger)
        runtime = object.__new__(ServiceRuntime)
        runtime.database = database
        ledger.mark_sent("tenant_acme", "attempt-1")
        for _ in range(2):
            with pytest.raises(BudgetDenied, match="reconciled"):
                runtime.require_settled_summary_source("tenant_acme", "tenant_acme-execution")
            ledger.pending("tenant_acme", "attempt-1")
        runtime.require_settled_summary_source("another_tenant", "tenant_acme-execution")
        ledger.settle("tenant_acme", "attempt-1", 10, 5, "b" * 64)
        runtime.require_settled_summary_source("tenant_acme", "tenant_acme-execution")
    finally:
        database.close()
