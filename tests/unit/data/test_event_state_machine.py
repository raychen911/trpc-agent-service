from uuid import UUID
import pytest

from trpc_service.storage.contracts import IdempotencyConflict, SequenceGap
from trpc_service.storage.data_models import DataScope, SessionEvent
from trpc_service.storage.memory import InMemoryDataRepository


def event(seq: int, eid: str = "e1", payload=None) -> SessionEvent:
    return SessionEvent(tenant_id="tenant-alpha", session_key="s", event_id=eid, sequence=seq, payload=payload or {"n": seq}, trace_id=UUID(int=1))


@pytest.mark.asyncio
async def test_event_sequence_and_idempotency() -> None:
    repo = InMemoryDataRepository(); scope = DataScope(tenant_id="tenant-alpha", trace_id=UUID(int=1))
    assert (await repo.append(scope, event(1), expected_watermark=0)).outcome == "CREATED"
    assert (await repo.append(scope, event(1), expected_watermark=1)).outcome == "REPLAYED"
    with pytest.raises(IdempotencyConflict):
        await repo.append(scope, event(1, payload={"different": True}), expected_watermark=1)
    with pytest.raises(SequenceGap):
        await repo.append(scope, event(3, "e3"), expected_watermark=1)
