"""Real PostgreSQL contention tests in a uniquely owned temporary schema."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import os
import threading
import uuid

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.schema import CreateSchema, DropSchema
from cryptography.fernet import Fernet

from trpc_service.persistence import Database
from trpc_service.persistence.models import IMAccountRow, InboundMessageRow, IMEventReceiptRow, utcnow
from trpc_service.channels.persistence import IMAccounts, IMIngress, ContextCipher, AccountOwnershipError
from trpc_service.tenant import MessageRouter, ChannelBindingRegistry, SessionIdentityFactory
from tests.integration.test_im_transports import telegram_binding
from tests.reliability.test_im_ingress import make_event, grant, register_test_binding


@pytest.fixture
def postgres():
    url = os.environ.get("TRPC_IM_TEST_DATABASE_URL")
    if not url:
        pytest.skip("set TRPC_IM_TEST_DATABASE_URL for real PostgreSQL IM contention tests")
    schema = "im_test_" + uuid.uuid4().hex
    admin = create_engine(url)
    with admin.begin() as connection:
        connection.execute(CreateSchema(schema))
    database = Database(url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        database.create_schema()
        yield database
    finally:
        database.close()
        with admin.begin() as connection:
            connection.execute(DropSchema(schema, cascade=True))
        admin.dispose()


def test_only_one_connection_owner_and_one_durable_input(postgres, tenant):
    binding = telegram_binding(tenant)
    register_test_binding(postgres, tenant, binding)
    tenant = tenant.model_copy(update={"channel_bindings": (binding, )})
    accounts = IMAccounts(postgres)
    barrier = threading.Barrier(2)

    def acquire(owner):
        barrier.wait()
        return accounts.acquire(binding, owner)

    with ThreadPoolExecutor(2) as pool:
        leases = list(pool.map(acquire, ["owner-a", "owner-b"]))
    assert sum(lease is not None for lease in leases) == 1
    lease = next(value for value in leases if value)
    identities = SessionIdentityFactory(lambda _: b"k" * 32)
    router = MessageRouter(ChannelBindingRegistry([tenant]), identities)
    ingress = IMIngress(postgres, ContextCipher([Fernet.generate_key()]), identities)
    event = make_event(binding)
    route = router.route_message(event.message)
    grant(postgres, route)

    def accept(_):
        barrier.wait()
        return ingress.record(make_event(binding), binding, lease, route=route, next_offset=51)

    with ThreadPoolExecutor(2) as pool:
        receipts = list(pool.map(accept, range(2)))
    assert sum(receipt.duplicate for receipt in receipts) == 1
    with postgres.sessions() as session:
        assert session.scalar(select(func.count()).select_from(InboundMessageRow)) == 1
        assert session.scalar(select(func.count()).select_from(IMEventReceiptRow)) == 1
    with postgres.sessions.begin() as session:
        session.get(IMAccountRow,
                    (lease.channel, lease.external_account_id)).lease_expires_at = utcnow() - timedelta(seconds=2)
    newer = accounts.acquire(binding, "replacement")
    assert newer.generation > lease.generation
    with pytest.raises(AccountOwnershipError):
        ingress.record(make_event(binding, 51), binding, lease, route=route, next_offset=52)
    assert accounts.offset(newer) == 51
