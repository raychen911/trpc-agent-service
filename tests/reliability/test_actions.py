"""Two-stage approvals across SDK, durable callbacks, action worker and IM delivery."""

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, func

from tests.reliability.test_im_ingress import system as system_fixture, make_event, grant
from trpc_service.governance.actions import ActionService, ActionConflict, conversation_hash
from trpc_service.governance.action_worker import ActionDefinition, ActionWorker
from trpc_service.governance.action_delivery import ActionNoticeWorker
from trpc_service.persistence.models import (TenantConfigVersionRow, InboundMessageRow, PendingActionRow, ActionJobRow,
                                             ActionNoticeRow, IMMembershipRow, IMEventReceiptRow, ToolInvocationRow,
                                             ToolResultRow, utcnow)
from trpc_service.channels.events import DeliveryResult

system = system_fixture


@pytest.fixture
def action_system(system, tenant):
    db, binding, router, cipher, accounts, lease, ingress = system
    event = make_event(binding)
    route = router.route_message(event.message)
    grant(db, route)
    with db.sessions.begin() as session:
        member = session.get(IMMembershipRow, (route.tenant_id, binding.binding_id, route.actor_id))
        member.grants_json = ["chat", "approve"]
        session.add(
            TenantConfigVersionRow(tenant_id=route.tenant_id,
                                   config_version=route.config_version,
                                   status="published",
                                   config_json=tenant.model_dump(mode="json"),
                                   content_hash="test",
                                   created_by="test"))
    receipt = ingress.record(event, binding, lease, route=route, next_offset=51)
    with db.sessions() as session:
        execution = session.get(InboundMessageRow, receipt.inbound_id).execution_id
    service = ActionService(db, cipher)
    return system, service, route, execution


def proposal(service, route, execution):
    return service.propose(route.tenant_id, execution, "call-refund", "execute_refund", "v1",
                           {"order_id": "opaque-order"}, {"version": 7}, {
                               "operation": "refund",
                               "target": "opaque-order",
                               "amount": "1.00",
                               "currency": "USD"
                           })


def command(service, action_id):
    with service.database.sessions() as session:
        notice = session.scalar(
            select(ActionNoticeRow).where(ActionNoticeRow.action_id == action_id,
                                          ActionNoticeRow.kind == "confirmation"))
        value = service.cipher.open((notice.tenant_id, "notice", notice.notice_id), notice.encrypted_payload)
        return value["text"].split("批准：/action ")[1].splitlines()[0]


def approve(service, route, action):
    nonce = command(service, action["action_id"]).split(":")[2]
    return service.decide(route.tenant_id, action["action_id"], route.actor_id, "approve", nonce, 1)


def spec():
    return ActionDefinition("execute_refund", "v1", lambda args: (args, {
        "version": 7
    }, {
        "operation": "refund"
    }), AsyncMock(return_value=True), AsyncMock(return_value={"receipt": "proof-1"}))


def worker(service, route, definition):
    return ActionWorker(service, {(route.tenant_id, definition.name, definition.revision): definition})


def test_callback_is_atomic_idempotent_and_never_a_new_agent_input(action_system):
    system, service, route, execution = action_system
    db, binding, _, _, accounts, lease, ingress = system
    action = proposal(service, route, execution)
    assert proposal(service, route, execution) == action
    token = command(service, action["action_id"])
    event = make_event(binding, 51, text="/action " + token)
    assert event.kind == "action"
    result = ingress.record(event, binding, lease, next_offset=52)
    assert result.disposition == "action_approved"
    assert ingress.record(event, binding, lease, next_offset=52).duplicate
    assert ingress.record(make_event(binding, 52, text="/action " + token), binding, lease,
                          next_offset=53).disposition == "action_approved"
    assert accounts.offset(lease) == 53
    with db.sessions() as session:
        assert session.scalar(select(func.count()).select_from(ActionJobRow)) == 1
        assert session.scalar(select(func.count()).select_from(InboundMessageRow)) == 1
        assert session.get(IMEventReceiptRow, (route.tenant_id, binding.binding_id, "51")).context_ciphertext is None
        row = session.get(PendingActionRow, (route.tenant_id, action["action_id"]))
        assert row.revision == 2 and token.split(":")[2] not in repr(service.view(row))
        assert "opaque-order" not in row.encrypted_payload


@pytest.mark.parametrize("violation", ["nonce", "actor", "conversation", "revision", "epoch", "tenant"])
def test_approval_is_bound_to_current_identity_and_exact_action(action_system, violation):
    system, service, route, execution = action_system
    action = proposal(service, route, execution)
    nonce = command(service, action["action_id"]).split(":")[2]
    if violation == "epoch":
        with service.database.sessions.begin() as session:
            session.get(IMMembershipRow, (route.tenant_id, route.channel_binding_id, route.actor_id)).authz_epoch += 1
    with pytest.raises((PermissionError, ActionConflict, LookupError)):
        service.decide("tenant_other" if violation == "tenant" else route.tenant_id,
                       action["action_id"],
                       "stranger" if violation == "actor" else route.actor_id,
                       "approve",
                       "invalid" if violation == "nonce" else nonce,
                       2 if violation == "revision" else 1,
                       conversation=conversation_hash("telegram", "other") if violation == "conversation" else None)
    with service.database.sessions() as session:
        assert session.scalar(select(func.count()).select_from(ActionJobRow)) == 0


def test_decision_failure_rolls_back_job_and_notice_but_records_refusal(action_system, monkeypatch):
    system, service, route, execution = action_system
    db, binding, _, _, accounts, lease, ingress = system
    action = proposal(service, route, execution)
    event = make_event(binding, 51, text="/action " + command(service, action["action_id"]))
    monkeypatch.setattr(ActionService, "notice", lambda *args: (_ for _ in ()).throw(ValueError("synthetic failure")))
    assert ingress.record(event, binding, lease, next_offset=52).disposition == "rejected"
    with db.sessions() as session:
        assert session.scalar(select(func.count()).select_from(ActionJobRow)) == 0
        assert session.get(PendingActionRow, (route.tenant_id, action["action_id"])).status == "pending"
    assert accounts.offset(lease) == 52


@pytest.mark.asyncio
async def test_worker_executes_once_only_after_approval_and_sends_durable_notice(action_system):
    system, service, route, execution = action_system
    db, _, _, cipher, _, lease, _ = system
    action = proposal(service, route, execution)
    definition = spec()
    consumer = worker(service, route, definition)
    assert not await consumer.run_once()
    definition.execute.assert_not_awaited()
    approve(service, route, action)
    assert await consumer.run_once()
    assert not await consumer.run_once()
    definition.execute.assert_awaited_once_with({"order_id": "opaque-order"}, {"version": 7}, action["action_id"])
    with db.sessions() as session:
        assert session.get(PendingActionRow, (route.tenant_id, action["action_id"])).status == "succeeded"
    adapter = type(
        "Adapter", (), {
            "send_text":
            AsyncMock(return_value=DeliveryResult(
                outcome="accepted", receipt_kind="message_id", external_message_id="notice-1"))
        })()
    delivery = ActionNoticeWorker(db, lease, adapter, cipher)
    while await delivery.run_once():
        pass
    adapter.send_text.assert_awaited_once()
    assert "已执行成功" in adapter.send_text.call_args.args[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["authorization", "resource", "timeout"])
async def test_worker_rechecks_approval_and_preconditions(action_system, change):
    _, service, route, execution = action_system
    action = proposal(service, route, execution)
    approve(service, route, action)
    definition = spec()
    if change == "authorization":
        with service.database.sessions.begin() as session:
            session.get(IMMembershipRow, (route.tenant_id, route.channel_binding_id, route.actor_id)).authz_epoch += 1
    elif change == "resource":
        definition.check.return_value = False
    else:
        definition.execute.side_effect = TimeoutError("secret external error")
    assert await worker(service, route, definition).run_once()
    with service.database.sessions() as session:
        row = session.get(PendingActionRow, (route.tenant_id, action["action_id"]))
        assert row.status == ("unknown_outcome" if change == "timeout" else "cancelled")
    if change != "timeout":
        definition.execute.assert_not_awaited()
    assert not await worker(service, route, definition).run_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("proof", ["complete", "absent", "corrupt"])
async def test_crash_recovery_uses_ledger_proof_without_replaying(action_system, proof):
    from trpc_service.reliability.tool_invocation import _hash
    _, service, route, execution = action_system
    action = proposal(service, route, execution)
    approve(service, route, action)
    definition = spec()
    consumer = worker(service, route, definition)
    key = (route.tenant_id, action["action_id"])
    consumer.begin(key)
    with service.database.sessions.begin() as session:
        session.get(ActionJobRow, key).lease_expires_at = utcnow() - timedelta(seconds=10)
        if proof != "absent":
            value = {"receipt": "proof"}
            session.add(
                ToolInvocationRow(tenant_id=route.tenant_id,
                                  execution_id=execution,
                                  tool_call_id="action:" + action["action_id"],
                                  tool_name="execute_refund",
                                  idempotency_key="test",
                                  request_hash=_hash({"order_id": "opaque-order"}),
                                  result_hash=_hash(value),
                                  status="succeeded"))
            session.add(
                ToolResultRow(tenant_id=route.tenant_id,
                              execution_id=execution,
                              tool_call_id="action:" + action["action_id"],
                              result_json={"value": value if proof == "complete" else {}}))
    assert await consumer.run_once()
    definition.execute.assert_not_awaited()
    with service.database.sessions() as session:
        assert session.get(PendingActionRow, key).status == ("succeeded" if proof == "complete" else "unknown_outcome")


@pytest.mark.asyncio
async def test_sending_notice_is_not_replayed_and_expired_confirmation_is_not_sent(action_system):
    system, service, route, execution = action_system
    db, _, _, cipher, _, lease, _ = system
    action = proposal(service, route, execution)
    adapter = type("Adapter", (), {"send_text": AsyncMock()})()
    delivery = ActionNoticeWorker(db, lease, adapter, cipher)
    work = delivery.prepare()
    assert work["text"].startswith("待确认")
    assert await delivery.run_once()  # interrupted sending is unknown
    assert not await delivery.run_once()
    adapter.send_text.assert_not_awaited()
    with db.sessions.begin() as session:
        session.get(PendingActionRow,
                    (route.tenant_id, action["action_id"])).expires_at = utcnow() - timedelta(seconds=1)
    service.expire()
    with db.sessions() as session:
        assert session.get(PendingActionRow, (route.tenant_id, action["action_id"])).status == "expired"


@pytest.mark.asyncio
async def test_pending_action_blocks_other_business_tools(action_system):
    from trpc_service.reliability.tool_execution import DurableTools
    _, service, route, execution = action_system
    proposal(service, route, execution)
    execute = AsyncMock()
    with pytest.raises(PermissionError):
        await DurableTools(service.database).invoke(route.tenant_id, execution, "next-call", "search_order", {},
                                                    execute)
    execute.assert_not_awaited()
