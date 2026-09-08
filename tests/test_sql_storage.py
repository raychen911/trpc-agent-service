import asyncio

import pytest
from sqlalchemy import func, select

from trpc_service.storage import Database
from trpc_service.storage.contracts import (
    AuditEntry,
    EventInput,
    MemoryInput,
    SessionIdentity,
    TurnCommit,
)
from trpc_service.storage.exceptions import DuplicateMessageError, VersionConflictError
from trpc_service.storage.models import (
    AgentApp,
    AgentSession,
    Memory,
    OutboxMessage,
    SessionEvent,
    Summary,
    Tenant,
)
from trpc_service.storage.outbox import OutboxWorker, VectorOutboxHandlers
from trpc_service.storage.sql_backend import SqlDataPlane
from trpc_service.storage.vector import InMemoryVectorStore, SemanticStore


def build_database() -> tuple[Database, str, str]:
    database = Database("sqlite+pysqlite:///:memory:")
    database.create_schema()
    with database.session_factory.begin() as session:
        tenant = Tenant(slug="sql-tenant", name="SQL Tenant", key_namespace="tenant/sql")
        session.add(tenant)
        session.flush()
        app = AgentApp(tenant_id=tenant.id, slug="sql-agent", name="SQL Agent")
        session.add(app)
        session.flush()
        return database, tenant.id, app.id


def test_sql_turn_transaction_and_outbox() -> None:
    database, tenant_id, app_id = build_database()

    async def scenario() -> None:
        store = SqlDataPlane(database.session_factory)
        identity = SessionIdentity(tenant_id, app_id, "user-1", "session-1")
        result = await store.commit_turn(
            TurnCommit(
                identity=identity,
                expected_version=0,
                event=EventInput(
                    "user_message",
                    "user",
                    {"text": "remember coffee"},
                    "trace-1",
                    "telegram",
                    "external-1",
                ),
                next_state={"turn": 1},
                summary_content="Coffee preference",
                memories=(
                    MemoryInput(
                        memory_key="coffee",
                        text="User prefers coffee",
                        metadata={"source": "conversation"},
                    ),
                ),
            )
        )
        assert result.session.version == 1
        assert result.event.sequence_no == 1
        assert result.summary is not None
        assert result.summary.through_sequence == 1

        with database.session_factory() as session:
            stored_session = session.scalar(select(AgentSession))
            assert stored_session is not None
            assert stored_session.version == 1
            assert stored_session.state == {"turn": 1}
            assert session.scalar(select(func.count()).select_from(SessionEvent)) == 1
            assert session.scalar(select(func.count()).select_from(Summary)) == 1
            assert session.scalar(select(func.count()).select_from(Memory)) == 1
            assert session.scalar(select(func.count()).select_from(OutboxMessage)) == 1

        with pytest.raises(DuplicateMessageError):
            await store.commit_turn(
                TurnCommit(
                    identity=identity,
                    expected_version=1,
                    event=EventInput(
                        "user_message",
                        "user",
                        {"text": "duplicate"},
                        "trace-2",
                        "telegram",
                        "external-1",
                    ),
                    next_state={"turn": 2},
                )
            )
        unchanged = await store.get_session(identity)
        assert unchanged is not None
        assert unchanged.version == 1

        with pytest.raises(VersionConflictError):
            await store.compare_and_swap_state(identity, 0, {"turn": 99})

        semantic = SemanticStore(InMemoryVectorStore())
        worker = OutboxWorker(
            store,
            VectorOutboxHandlers(semantic).handlers(),
            "sql-test-worker",
        )
        assert await worker.poll_once() == 1
        matches = await semantic.search_memories(tenant_id, app_id, "user-1", "coffee")
        assert matches[0].document.id == "coffee"

        audit_id = await store.append_audit(
            AuditEntry(
                tenant_id=tenant_id,
                agent_app_id=app_id,
                agent_name="sql_agent",
                decision="allow",
                trace_id="trace-1",
                request_id="request-1",
            )
        )
        assert audit_id

    try:
        asyncio.run(scenario())
    finally:
        database.dispose()
