from uuid import UUID
import pytest

from trpc_service.storage.contracts import IdempotencyConflict, SequenceGap
from trpc_service.storage.data_models import DataScope, SessionEvent
from trpc_service.storage.memory import InMemoryDataRepository


async def event_contract(repo) -> None:
    scope = DataScope(tenant_id="tenant-alpha", trace_id=UUID(int=11))
    first = SessionEvent(tenant_id=scope.tenant_id, session_key="session", event_id="event-1", sequence=1, payload={"v": 1}, trace_id=scope.trace_id)
    assert (await repo.append(scope, first, expected_watermark=0)).outcome == "CREATED"
    assert (await repo.append(scope, first, expected_watermark=1)).outcome == "REPLAYED"
    assert await repo.get_watermark(scope, "session") == 1
    assert (await repo.list_metadata(scope, "session"))[0].content_digest == first.content_digest
    assert (await repo.read_content(scope, "session"))[0] == first
    with pytest.raises(IdempotencyConflict):
        await repo.append(scope, first.model_copy(update={"payload": {"v": 2}, "content_digest": "f" * 64}), expected_watermark=1)
    with pytest.raises(SequenceGap):
        await repo.append(scope, first.model_copy(update={"event_id": "event-3", "sequence": 3}), expected_watermark=1)


@pytest.mark.asyncio
async def test_inmemory_event_repository_contract() -> None:
    await event_contract(InMemoryDataRepository())
