"""R3A real-PostgreSQL evidence for request usage and unified audit scope."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from trpc_service.storage.audit_query_repository import SqlAuditQueryRepository
from trpc_service.storage.usage_repository import SqlUsageRepository
from trpc_service.usage.models import UsageIncrement

from .pg_helpers import requires_docker, run_alembic

pytestmark = requires_docker

_GOV = ('{"allowed_channels":["web_console"],"allowed_user_ids":[],"tool_decisions":{},'
        '"content_policy":{"enabled":false,"input_action":"block","output_action":"block"},'
        '"limits":null}')
_PROFILE = ('{"state_backend":"sql","memory_backend":"sql","summary_backend":"sql",'
            '"artifact_backend":"s3","knowledge_backend":"pgvector"}')


def _tenant(pg, tenant: str, retention_days: int = 365) -> None:
    policy = f'{{"retention_days":{retention_days},"delivery_events":"all"}}'
    result = pg.run_sql(
        "INSERT INTO tenant_configs (tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,"
        "governance,backend_profile,audit_policy) "
        f"VALUES ('{tenant}',true,1,'app','i','default','[]'::jsonb,'{_GOV}'::jsonb,"
        f"'{_PROFILE}'::jsonb,'{policy}'::jsonb)")
    assert result.success, result.output


def _receipt_and_event(pg, tenant: str, request: uuid.UUID, *, age_days: int = 0) -> None:
    receipt, audit = uuid.uuid4(), uuid.uuid4()
    when = "now()" if age_days == 0 else f"now() - interval '{age_days} days'"
    result = pg.run_sql(
        "INSERT INTO message_receipts (receipt_id,tenant_id,channel,user_id,session_id,message_id,app_id,"
        "config_version,request_id,message_digest,state,started_at) VALUES "
        f"('{receipt}','{tenant}','wecom','internal-user','session','m-{uuid.uuid4().hex[:8]}','app',1,"
        f"'{request}',repeat('0',64),'processing',{when});"
        "INSERT INTO execution_audit_events (audit_id,tenant_id,receipt_id,request_id,config_version,trace_id,"
        "event_type,outcome,category,tool_name,error_code,latency_ms,occurred_at) VALUES "
        f"('{audit}','{tenant}','{receipt}','{request}',1,'{'a' * 32}','agent_result','success',"
        f"NULL,NULL,NULL,7,{when});")
    assert result.success, result.output


@pytest.mark.asyncio
async def test_request_fact_is_atomic_and_audit_is_tenant_retention_scoped(pg_container):
    run_alembic(pg_container.url, "upgrade", "head", check=True)
    tenant_a, tenant_b = f"t{uuid.uuid4().hex[:10]}", f"t{uuid.uuid4().hex[:10]}"
    _tenant(pg_container, tenant_a, retention_days=1)
    _tenant(pg_container, tenant_b)
    request_a, request_b, old_request = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    _receipt_and_event(pg_container, tenant_a, request_a)
    _receipt_and_event(pg_container, tenant_b, request_b)
    _receipt_and_event(pg_container, tenant_a, old_request, age_days=2)
    engine = create_async_engine(pg_container.url)
    usage = SqlUsageRepository(engine, owns_engine=False)
    audit = SqlAuditQueryRepository(engine, owns_engine=False)
    try:
        await usage.add_usage(
            UsageIncrement(usage_date=datetime.now(timezone.utc).date(),
                           tenant_id=tenant_a,
                           model_profile="default",
                           requests=1,
                           input_tokens=3,
                           output_tokens=5,
                           total_tokens=8,
                           cost_microunits=11,
                           cost_state="known",
                           request_id=request_a,
                           config_version=1,
                           occurred_at=datetime.now(timezone.utc)))
        # Both rows are visible after the one repository transaction; request
        # identity is idempotent and the audit projection can join its cost.
        fact = await usage.get_for_request(tenant_a, request_a)
        assert fact is not None and fact.cost_microunits == 11 and fact.receipt_id is not None
        page = await audit.list_for_tenant(tenant_a, request_id=request_a, trace_id="a" * 32)
        assert len(page) == 1 and page[0].tenant_id == tenant_a and page[0].cost_microunits == 11
        assert await audit.list_for_tenant(tenant_a, request_id=request_b) == ()
        assert await audit.list_for_tenant(tenant_a, request_id=old_request) == ()
    finally:
        await engine.dispose()
