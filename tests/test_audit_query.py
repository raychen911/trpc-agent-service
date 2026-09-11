from datetime import datetime, timezone
from uuid import uuid4

import pytest

from trpc_service.audit.query import UnifiedAuditRecord
from trpc_service.storage.audit_query_repository import AuditQueryRepositoryDataError, SqlAuditQueryRepository


def test_unified_record_exposes_only_safe_projection_fields():
    record = UnifiedAuditRecord(
        tenant_id="tenant-a",
        channel="wecom",
        user_id="u",
        session_id="s",
        agent_name="app",
        tool_name=None,
        decision="success",
        latency_ms=1,
        error_type=None,
        error_code=None,
        cost_microunits=None,
        trace_id=None,
        request_id=uuid4(),
        config_version=1,
        occurred_at=datetime.now(timezone.utc),
    )
    assert "message" not in record.model_dump()
    assert "digest" not in record.model_dump()


@pytest.mark.asyncio
async def test_query_limit_is_strict_before_database_access():
    repository = SqlAuditQueryRepository(None)  # type: ignore[arg-type]
    with pytest.raises(AuditQueryRepositoryDataError):
        await repository.list_for_tenant("tenant-a", limit=101)
