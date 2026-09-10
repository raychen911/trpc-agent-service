from uuid import UUID
import pytest
from trpc_service.storage.contracts import SummaryConflict, VersionConflict
from trpc_service.storage.data_models import DataScope, SessionEvent, SummaryRecord
from trpc_service.storage.memory import InMemoryDataRepository

@pytest.mark.asyncio
async def test_summary_watermark_does_not_regress() -> None:
    repo = InMemoryDataRepository(); scope = DataScope(tenant_id="tenant-alpha", trace_id=UUID(int=1))
    await repo.append(scope, SessionEvent(tenant_id="tenant-alpha", session_key="s", event_id="e1", sequence=1, payload={}), expected_watermark=0)
    summary = SummaryRecord(tenant_id="tenant-alpha", session_key="s", content={"s": 1}, event_watermark=1)
    assert (await repo.compare_and_set_summary(scope, summary, expected_version=None)).outcome == "CREATED"
    assert (await repo.compare_and_set_summary(scope, summary, expected_version=1)).outcome == "REPLAYED"
    with pytest.raises(SummaryConflict):
        await repo.compare_and_set_summary(scope, SummaryRecord(tenant_id="tenant-alpha", session_key="s", content={"s": 2}, event_watermark=1), expected_version=1)
    with pytest.raises(VersionConflict):
        await repo.compare_and_set_summary(scope, SummaryRecord(tenant_id="tenant-alpha", session_key="s", content={"s": 3}, event_watermark=2), expected_version=1)
