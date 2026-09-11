"""Financial state transitions, tenant boundaries, and cancellation under contention."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import threading

import pytest
from sqlalchemy import select, func

from trpc_service.persistence import Database
from trpc_service.persistence.models import (TenantRow, InboundMessageRow, ModelAttemptRow, BudgetAccountRow,
                                             BudgetPolicyRow, AuditLogRow)
from trpc_service.governance.budget import BudgetLedger, BudgetDenied, BudgetConflict, money

TENANT = "tenant_acme"


def seed(database, tenant=TENANT):
    with database.sessions.begin() as session:
        session.add(TenantRow(tenant_id=tenant, name="Test", status="active", active_config_version=1))
        session.add(
            InboundMessageRow(inbound_message_id=tenant + "-input",
                              tenant_id=tenant,
                              channel_binding_id="binding_test",
                              external_message_id="event-1",
                              payload_hash="a" * 64,
                              payload_json={},
                              request_id="request-1",
                              trace_id="b" * 32,
                              execution_id=tenant + "-execution",
                              partition_key=tenant + "-scope",
                              config_version=1,
                              storage_revision=1,
                              status="pending"))
    ledger = BudgetLedger(database)
    ledger.register_price(tenant, "price-v1", "model-test", "CNY", "1", "2", actor="operator")
    ledger.configure(tenant, "CNY", "1", "10", "Asia/Shanghai", 0, actor="operator")
    return ledger


@pytest.fixture
def budget(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'budget.db'}")
    db.create_schema()
    ledger = seed(db)
    yield ledger
    db.close()


def reserve(ledger, attempt="attempt-1", tenant=TENANT, **kwargs):
    params = dict(max_input_tokens=200_000, max_output_tokens=200_000, max_calls=20)
    params.update(kwargs)
    return ledger.reserve(tenant, attempt, tenant + "-execution", "price-v1", "model-test", "a" * 64, **params)


def accounts(ledger, tenant=TENANT):
    with ledger.database.sessions() as session:
        return list(
            session.scalars(
                select(BudgetAccountRow).where(BudgetAccountRow.tenant_id == tenant).order_by(BudgetAccountRow.period)))


def test_daily_and_monthly_are_atomic_and_settlement_is_idempotent(budget):
    result = reserve(budget)
    assert Decimal(result["reserved_amount"]) == Decimal("0.6")
    assert reserve(budget)["attempt_id"] == result["attempt_id"]
    with pytest.raises(BudgetDenied):
        reserve(budget, "attempt-2")
    assert all(row.reserved == Decimal("0.6") for row in accounts(budget))
    with budget.database.sessions() as session:
        assert session.scalar(select(func.count()).select_from(ModelAttemptRow)) == 1
    budget.mark_sent(TENANT, "attempt-1")
    with pytest.raises(BudgetConflict):
        budget.mark_sent(TENANT, "attempt-1")
    for _ in range(2):
        result = budget.settle(TENANT, "attempt-1", 100_000, 50_000, "b" * 64)
    assert Decimal(result["actual_amount"]) == Decimal("0.2")
    assert all(row.reserved == 0 and row.settled == Decimal("0.2") for row in accounts(budget))
    with pytest.raises(BudgetConflict):
        budget.settle(TENANT, "attempt-1", 100_000, 50_000, "c" * 64)
    with budget.database.sessions() as session:
        assert session.scalar(
            select(func.count()).select_from(AuditLogRow).where(AuditLogRow.decision == "model_budget_settled")) == 1


def test_monthly_failure_rolls_back_daily_creation(budget):
    budget.configure(TENANT, "CNY", "10", "0.5", "Asia/Shanghai", 1, actor="operator")
    with pytest.raises(BudgetDenied):
        reserve(budget)
    assert accounts(budget) == []


def test_unknown_cost_survives_restart_and_only_unsent_can_cancel(budget):
    reserve(budget)
    budget.mark_sent(TENANT, "attempt-1")
    reopened = Database(budget.database.url)
    recovered = BudgetLedger(reopened)
    try:
        assert recovered.pending(TENANT, "attempt-1")["status"] == "pending_reconciliation"
        with pytest.raises(BudgetConflict):
            recovered.cancel_unsent(TENANT, "attempt-1")
        assert all(row.reserved == Decimal("0.6") and row.settled == 0 for row in accounts(recovered))
        with pytest.raises(BudgetDenied):
            reserve(recovered, "another-attempt")
        recovered.settle(TENANT, "attempt-1", 0, 100_000, "d" * 64)
        reserve(recovered, "another-attempt")
        recovered.cancel_unsent(TENANT, "another-attempt")
        recovered.cancel_unsent(TENANT, "another-attempt")
        assert all(row.reserved == 0 and row.settled == Decimal("0.2") for row in accounts(recovered))
    finally:
        reopened.close()


def test_actual_overrun_is_not_capped_and_blocks_even_after_limit_increase(budget):
    reserve(budget)
    budget.mark_sent(TENANT, "attempt-1")
    result = budget.settle(TENANT, "attempt-1", 200_000, 600_000, "b" * 64)
    assert result["overrun"] and Decimal(result["actual_amount"]) == Decimal("1.4")
    budget.configure(TENANT, "CNY", "20", "20", "Asia/Shanghai", 1, actor="operator")
    with pytest.raises(BudgetDenied):
        reserve(budget, "new-attempt")
    assert all(row.settled == Decimal("1.4") for row in accounts(budget))


def test_identity_call_limit_price_and_cross_tenant_checks(budget):
    reserve(budget, max_calls=1)
    with pytest.raises(BudgetConflict):
        reserve(budget, max_input_tokens=1)
    with pytest.raises(BudgetDenied, match="call limit"):
        reserve(budget, "attempt-2", max_calls=1)
    with pytest.raises(BudgetConflict):
        budget.register_price(TENANT, "price-v1", "model-test", "CNY", "2", "2", actor="operator")
    seed(budget.database, "tenant_other")
    with pytest.raises(LookupError):
        budget.cancel_unsent("tenant_other", "attempt-1")
    reserve(budget, tenant="tenant_other")
    assert all(row.reserved == Decimal("0.6") for row in accounts(budget, "tenant_other"))
    with pytest.raises(BudgetConflict):
        budget.configure(TENANT, "CNY", "0.1", "10", "Asia/Shanghai", 1, actor="operator")
    with pytest.raises(BudgetConflict):
        budget.configure(TENANT, "CNY", "1", "10", "UTC", 1, actor="operator")


def test_period_rollover_never_releases_old_unknown_and_does_not_reprice(budget, monkeypatch):
    clock = [datetime(2026, 9, 30, 15, 59, tzinfo=timezone.utc)]
    monkeypatch.setattr("trpc_service.governance.budget.database_now", lambda _: clock[0])
    original = reserve(budget)
    budget.mark_sent(TENANT, "attempt-1")
    budget.pending(TENANT, "attempt-1")
    clock[0] = datetime(2026, 9, 30, 16, 1, tzinfo=timezone.utc)
    assert reserve(budget)["periods"] == original["periods"]
    reserve(budget, "october-attempt")
    assert len(accounts(budget)) == 4
    budget.settle(TENANT, "attempt-1", 0, 100_000, "b" * 64)
    rows = {row.period: row for row in accounts(budget)}
    assert rows["day:Asia/Shanghai:2026-09-30"].settled == Decimal("0.2")
    assert rows["day:Asia/Shanghai:2026-10-01"].reserved == Decimal("0.6")


def contend(ledger):
    barrier = threading.Barrier(2)

    def worker(index):
        barrier.wait(timeout=5)
        try:
            reserve(ledger, f"worker-{index}")
            return "approved"
        except BudgetDenied:
            return "denied"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(worker, [1, 2]))
    assert sorted(results) == ["approved", "denied"]
    assert all(row.reserved == Decimal("0.6") for row in accounts(ledger))


def test_independent_worker_connections_cannot_overspend(budget):
    contend(budget)


@pytest.mark.parametrize("value", [0.1, True, "NaN", "Infinity", "-0.1", "0.00000000001", "1000000001"])
def test_money_rejects_ambiguous_or_unbounded_values(value):
    with pytest.raises(ValueError):
        money(value)


def test_suspended_tenant_cannot_dispatch_existing_reservation(budget):
    reserve(budget)
    with budget.database.sessions.begin() as session:
        session.get(TenantRow, TENANT).status = "suspended"
    with pytest.raises(BudgetDenied):
        budget.mark_sent(TENANT, "attempt-1")
    budget.cancel_unsent(TENANT, "attempt-1")
    with budget.database.sessions() as session:
        assert not session.get(BudgetPolicyRow, (TENANT, "CNY")).blocked


def test_im_membership_revoked_after_reservation_prevents_dispatch(budget):
    from trpc_service.persistence.models import ChannelBindingRow, IMMembershipRow
    route = dict(tenant_id=TENANT,
                 config_version=1,
                 storage_revision=1,
                 agent_app_id="app_test",
                 channel_binding_id="binding_test",
                 actor_id="usr_test",
                 internal_user_id="scp_test",
                 session_id="ses_test",
                 partition_key="test")
    with budget.database.sessions.begin() as session:
        session.scalar(select(InboundMessageRow)).payload_json = {"route": route, "authz_epoch": 1}
        session.add(
            ChannelBindingRow(tenant_id=TENANT,
                              binding_id="binding_test",
                              channel="telegram",
                              webhook_public_id="callback-test",
                              external_account_id="1234",
                              agent_app_id="app_test",
                              credential_ref="env://BOT_TOKEN",
                              enabled=True))
        session.add(
            IMMembershipRow(tenant_id=TENANT,
                            binding_id="binding_test",
                            actor_id="usr_test",
                            active=True,
                            authz_epoch=1,
                            grants_json=["chat"]))
    reserve(budget)
    with budget.database.sessions.begin() as session:
        session.get(IMMembershipRow, (TENANT, "binding_test", "usr_test")).authz_epoch = 2
    with pytest.raises(PermissionError):
        budget.mark_sent(TENANT, "attempt-1")
    budget.cancel_unsent(TENANT, "attempt-1")
    assert all(row.reserved == 0 and row.settled == 0 for row in accounts(budget))


def test_different_currencies_are_not_combined(budget):
    budget.configure(TENANT, "USD", "0.1", "0.1", "Asia/Shanghai", 0, actor="operator")
    budget.register_price(TENANT, "price-usd", "model-test", "USD", "1", "2", actor="operator")
    reserve(budget)
    with pytest.raises(BudgetDenied):
        budget.reserve(TENANT, "usd-attempt", TENANT + "-execution", "price-usd", "model-test", "a" * 64, 200_000,
                       200_000)
    assert len(accounts(budget)) == 2 and {row.currency for row in accounts(budget)} == {"CNY"}
