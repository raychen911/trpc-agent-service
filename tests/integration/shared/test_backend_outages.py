from datetime import datetime, timezone
from uuid import uuid4

import pytest

from trpc_service.storage.contracts import StateBackendUnavailable
from trpc_service.storage.models import IdempotencyKey
from trpc_service.storage.redis_idempotency import RedisIdempotencyRepository
from trpc_service.storage.redis_leases import RedisSessionLeaseManager
from trpc_service.storage.redis_session import FencedRedisSessionService
from trpc_service.storage.models import NodeIdentity
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.repositories import PostgresAuditRepository, PostgresConfigurationRepository
from trpc_service.audit.models import AuditDecision, AuditRecord, TenantScope
from trpc_service.storage.contracts import AuditUnavailable, ConfigurationUnavailable
from decimal import Decimal


class BrokenRedis:
    async def script_load(self, *_args): raise ConnectionError("private endpoint")


async def test_redis_claim_failure_is_safe_and_has_no_local_fallback() -> None:
    repository = RedisIdempotencyRepository(BrokenRedis(), node_id="node")
    with pytest.raises(StateBackendUnavailable, match="Shared state is unavailable"):
        await repository.claim(IdempotencyKey(tenant_id="tenant-alpha", binding_id="binding-alpha", external_message_id="id"),
                               "a" * 64, uuid4(), datetime.now(timezone.utc))


@pytest.mark.parametrize("stage", ["claim", "lock", "session"])
async def test_redis_outage_matrix_maps_every_pre_execution_stage_without_fallback(stage: str) -> None:
    broken = BrokenRedis()
    with pytest.raises(StateBackendUnavailable):
        if stage == "claim":
            await RedisIdempotencyRepository(broken, node_id="node").claim(
                IdempotencyKey(tenant_id="tenant-alpha", binding_id="binding-alpha", external_message_id="id"), "a" * 64, uuid4(), datetime.now(timezone.utc))
        elif stage == "lock":
            await RedisSessionLeaseManager(broken).acquire("tenant-alpha", "agent-alpha", "sess_" + "a" * 64, "b" * 64, NodeIdentity(node_id="node"), 100, 0)
        else:
            await FencedRedisSessionService(broken, tenant_id="tenant-alpha", agent_id="agent-alpha", namespace="test").get_session(app_name="a", user_id="u", session_id="sess_" + "a" * 64)


async def test_sql_configuration_and_audit_outages_are_domain_errors() -> None:
    database = PostgresDatabase("postgresql+asyncpg://invalid:invalid@127.0.0.1:1/unavailable")
    with pytest.raises(ConfigurationUnavailable):
        await PostgresConfigurationRepository(database).get_auth_material("binding-alpha", "local_http")
    record = AuditRecord(audit_id=uuid4(), trace_id=uuid4(), tenant_id="tenant-alpha", channel="local_http",
                         binding_id_digest="sha256:" + "a" * 64, decision=AuditDecision.SUCCEEDED,
                         latency_ms=0, cost=Decimal("0"), created_at=datetime.now(timezone.utc))
    with pytest.raises(AuditUnavailable):
        await PostgresAuditRepository(database, node_id="node").append(TenantScope(tenant_id="tenant-alpha"), record)
    await database.close()
