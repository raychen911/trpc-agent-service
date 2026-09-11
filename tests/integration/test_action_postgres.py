"""Real PostgreSQL approval contention and independent worker execution."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import select, func

from tests.integration.test_im_postgres import postgres as postgres_fixture
from tests.integration.test_im_transports import telegram_binding
from tests.reliability.test_im_ingress import register_test_binding
from tests.reliability.test_actions import action_system as action_fixture, proposal, approve, spec, worker
from trpc_service.channels.persistence import IMAccounts, ContextCipher, IMIngress
from trpc_service.tenant import SessionIdentityFactory, ChannelBindingRegistry, MessageRouter
from trpc_service.persistence.models import ActionJobRow, PendingActionRow

action_system, postgres = action_fixture, postgres_fixture


@pytest.fixture
def system(postgres, tenant):
    binding = telegram_binding(tenant)
    register_test_binding(postgres, tenant, binding)
    configured = tenant.model_copy(update={"channel_bindings": (binding, )})
    identities = SessionIdentityFactory(lambda _: b"k" * 32)
    router = MessageRouter(ChannelBindingRegistry([configured]), identities)
    cipher, accounts = ContextCipher([Fernet.generate_key()]), IMAccounts(postgres)
    lease = accounts.acquire(binding, "owner")
    return postgres, binding, router, cipher, accounts, lease, IMIngress(postgres, cipher, identities)


def test_concurrent_proposals_and_approvals_create_one_job(action_system):
    _, service, route, execution = action_system
    barrier = threading.Barrier(2)

    def propose(_):
        barrier.wait()
        return proposal(service, route, execution)

    with ThreadPoolExecutor(2) as pool:
        actions = list(pool.map(propose, range(2)))
    assert actions[0]["action_id"] == actions[1]["action_id"]

    def decide(_):
        barrier.wait()
        return approve(service, route, actions[0])

    with ThreadPoolExecutor(2) as pool:
        decisions = list(pool.map(decide, range(2)))
    assert all(item["status"] == "approved" and item["revision"] == 2 for item in decisions)
    with service.database.sessions() as session:
        assert session.scalar(select(func.count()).select_from(ActionJobRow)) == 1
        assert session.scalar(select(func.count()).select_from(PendingActionRow)) == 1


@pytest.mark.asyncio
async def test_two_action_workers_do_not_execute_same_action(action_system):
    _, service, route, execution = action_system
    action = proposal(service, route, execution)
    approve(service, route, action)
    definition = spec()
    await asyncio.gather(worker(service, route, definition).run_once(), worker(service, route, definition).run_once())
    definition.execute.assert_awaited_once()
    with service.database.sessions() as session:
        assert session.get(PendingActionRow, (route.tenant_id, action["action_id"])).status == "succeeded"
