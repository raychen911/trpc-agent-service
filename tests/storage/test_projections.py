"""Summary and Memory projection monotonicity tests."""

from __future__ import annotations

import pytest

from trpc_service.storage import (
    Database,
    MemoryProjection,
    ProjectionConflictError,
    SqlProjectionStore,
    SummaryProjection,
)
from trpc_service.storage.models import AgentApp, ChannelBinding, Session, Tenant


@pytest.fixture
async def store(tmp_path):
    database = Database(f"sqlite+aiosqlite:///{tmp_path / 'projection.db'}")
    await database.create_schema()
    async with database.session_factory() as session, session.begin():
        session.add(
            Tenant(
                tenant_id="tenant-projection",
                display_name="Projection tenant",
                status="active",
                audit_policy={},
                budget_policy={},
            )
        )
        await session.flush()
        session.add(
            AgentApp(
                tenant_id="tenant-projection",
                app_id="assistant",
                revision=1,
                status="published",
                agent_name="Assistant",
                prompt="Safe",
                model_config={},
                tool_policy={},
                storage_config={},
            )
        )
        await session.flush()
        session.add(
            ChannelBinding(
                binding_id="binding-projection",
                tenant_id="tenant-projection",
                app_id="assistant",
                app_revision=1,
                config_revision=1,
                channel_type="telegram",
                external_account_id="bot-projection",
                callback_path="/projection",
                public_callback_id="projection",
                route_rule={},
                secret_refs={},
                identity_policy={},
                status="active",
            )
        )
        await session.flush()
        session.add(
            Session(
                tenant_id="tenant-projection",
                session_id="session-projection",
                app_id="assistant",
                app_revision=1,
                binding_id="binding-projection",
                scope="private",
                principal_id="principal-projection",
                state={},
            )
        )
    try:
        yield SqlProjectionStore(database)
    finally:
        await database.dispose()


async def test_summary_never_moves_backwards(store: SqlProjectionStore) -> None:
    first = SummaryProjection(
        tenant_id="tenant-projection",
        session_id="session-projection",
        through_seq=5,
        content="summary-v1",
        summarizer_version="v1",
    )
    assert await store.put_summary_if_newer(first) is True
    assert await store.put_summary_if_newer(first) is False
    assert (
        await store.put_summary_if_newer(
            SummaryProjection(
                tenant_id="tenant-projection",
                session_id="session-projection",
                through_seq=4,
                content="stale",
                summarizer_version="v1",
            )
        )
        is False
    )
    with pytest.raises(ProjectionConflictError):
        await store.put_summary_if_newer(
            SummaryProjection(
                tenant_id="tenant-projection",
                session_id="session-projection",
                through_seq=5,
                content="different",
                summarizer_version="v1",
            )
        )


async def test_memory_event_extraction_is_idempotent(store: SqlProjectionStore) -> None:
    memory = MemoryProjection(
        tenant_id="tenant-projection",
        principal_id="principal-projection",
        session_id="session-projection",
        source_event_id="event-1",
        extractor_version="extractor-v1",
        record_version=1,
        content="User prefers concise replies.",
    )
    assert await store.put_memory_once(memory) is True
    assert await store.put_memory_once(memory) is False
    with pytest.raises(ProjectionConflictError):
        await store.put_memory_once(
            MemoryProjection(
                tenant_id=memory.tenant_id,
                principal_id=memory.principal_id,
                session_id=memory.session_id,
                source_event_id=memory.source_event_id,
                extractor_version=memory.extractor_version,
                record_version=memory.record_version,
                content="Conflicting extraction",
            )
        )
