"""Budget reservations remain single-charge across a stale Redis connection."""

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from trpc_service.config import TenantConfig
from trpc_service.gateway import AgentRequest, AgentTaskEnvelope, RedisStreamAgentTaskQueue
from trpc_service.tenant.budget import BudgetBackendUnavailableError
from trpc_service.tenant.budget import RedisBudgetLedger


class FakePool:

    def __init__(self):
        self.disconnects = 0

    async def disconnect(self):
        self.disconnects += 1


class AppliedThenDisconnectedRedis:
    """Model a Lua commit whose response is lost with the old connection."""

    def __init__(self):
        self.connection_pool = FakePool()
        self.operations = set()
        self.charges = 0
        self.calls = 0

    async def eval(self, _script, key_count, _budget_key, _reservation_key, *arguments):
        assert key_count == 2
        self.calls += 1
        operation_id = arguments[-1]
        if operation_id not in self.operations:
            self.operations.add(operation_id)
            self.charges += int(arguments[0])
        if self.calls == 1:
            raise RedisConnectionError("connection lost after commit")
        return 2


@pytest.mark.asyncio
async def test_budget_retry_uses_same_operation_id_without_double_charge():
    client = AppliedThenDisconnectedRedis()
    ledger = RedisBudgetLedger("redis://unused", client=client)
    tenant = TenantConfig(tenant_id="tenant-a")

    await ledger.reserve(tenant, requests=1, request_id="request-1")

    assert client.calls == 2
    assert client.connection_pool.disconnects == 1
    assert client.operations == {"request-1"}
    assert client.charges == 1


@pytest.mark.asyncio
async def test_budget_reports_backend_unavailable_after_bounded_retry():
    client = AppliedThenDisconnectedRedis()

    async def always_fail(*_args):
        raise RedisConnectionError("still unavailable")

    client.eval = always_fail
    ledger = RedisBudgetLedger("redis://unused", client=client)

    with pytest.raises(BudgetBackendUnavailableError):
        await ledger.reserve(TenantConfig(tenant_id="tenant-a"), request_id="request-2")

    assert client.connection_pool.disconnects == 1


class EnqueueAppliedThenDisconnectedRedis:
    """Return the original Stream receipt after a lost enqueue response."""

    def __init__(self):
        self.connection_pool = FakePool()
        self.receipts = {}
        self.entries = []
        self.calls = 0

    async def xgroup_create(self, *_args, **_kwargs):
        return True

    async def eval(self, _script, key_count, _stream, marker, payload, _ttl):
        assert key_count == 2
        self.calls += 1
        receipt = self.receipts.get(marker)
        if receipt is None:
            receipt = "1-0"
            self.receipts[marker] = receipt
            self.entries.append(payload)
        if self.calls == 1:
            raise RedisConnectionError("enqueue response lost")
        return receipt


@pytest.mark.asyncio
async def test_queue_reconnect_does_not_duplicate_committed_enqueue():
    client = EnqueueAppliedThenDisconnectedRedis()
    queue = RedisStreamAgentTaskQueue("redis://unused", client=client)
    request = AgentRequest(request_id="request-1",
                           tenant_id="tenant-a",
                           config_version=1,
                           app_id="assistant",
                           user_id="user",
                           session_id="session",
                           text="hello")
    receipt = await queue.enqueue(AgentTaskEnvelope(task_id="task-1", request=request, idempotency_key="key"))

    assert receipt == "1-0"
    assert client.calls == 2
    assert client.connection_pool.disconnects == 1
    assert len(client.entries) == 1
