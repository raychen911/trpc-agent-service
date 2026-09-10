from uuid import UUID
import pytest

from trpc_service.storage.contracts import SummaryConflict, VersionConflict
from trpc_service.storage.data_models import DataScope, SessionEvent, SummaryRecord
from trpc_service.storage.memory import InMemoryDataRepository


@pytest.mark.asyncio
async def test_summary_cas_contract() -> None:
    repo = InMemoryDataRepository(); scope = DataScope(tenant_id="tenant-alpha", trace_id=UUID(int=1))
    await repo.append(scope, SessionEvent(tenant_id=scope.tenant_id, session_key="s", event_id="e", sequence=1, payload={}), expected_watermark=0)
    first = SummaryRecord(tenant_id=scope.tenant_id, session_key="s", content={"x": 1}, event_watermark=1)
    await repo.compare_and_set_summary(scope, first, expected_version=None)
    assert (await repo.get_summary_metadata(scope, "s")).event_watermark == 1
    with pytest.raises(SummaryConflict):
        await repo.compare_and_set_summary(scope, first.model_copy(update={"content": {"x": 2}, "content_digest": "b" * 64}), expected_version=1)
    with pytest.raises(VersionConflict):
        await repo.compare_and_set_summary(scope, first.model_copy(update={"event_watermark": 2, "version": 2}), expected_version=1)
