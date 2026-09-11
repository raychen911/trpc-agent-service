"""A provider receipt is immutable evidence, never caller-supplied free usage."""

import pytest
from sqlalchemy import select

from tests.reliability.test_budget import budget as budget_fixture, reserve, TENANT
from trpc_service.governance.budget import BudgetConflict
from trpc_service.governance.reconciliation import reconcile
from trpc_service.persistence.models import AuditLogRow

budget = budget_fixture


def test_receipt_requires_dispatch_and_cannot_be_changed(budget):
    reserve(budget)
    with pytest.raises(BudgetConflict):
        budget.record_provider_receipt(TENANT, "attempt-1", "gen-test-one")
    budget.mark_sent(TENANT, "attempt-1")
    budget.record_provider_receipt(TENANT, "attempt-1", "gen-test-one")
    budget.record_provider_receipt(TENANT, "attempt-1", "gen-test-one")
    with pytest.raises(BudgetConflict):
        budget.record_provider_receipt(TENANT, "attempt-1", "gen-test-other")
    with budget.database.sessions() as session:
        rows = list(session.scalars(select(AuditLogRow).where(AuditLogRow.decision == "model_provider_receipt")))
        assert len(rows) == 1 and rows[0].metadata_json["generation_id"] == "gen-test-one"


@pytest.mark.asyncio
async def test_unknown_without_receipt_cannot_be_settled(budget):
    reserve(budget)
    budget.mark_sent(TENANT, "attempt-1")
    budget.pending(TENANT, "attempt-1")
    with pytest.raises(BudgetConflict, match="receipt unavailable"):
        await reconcile(budget.database, TENANT, "attempt-1", "operator")
