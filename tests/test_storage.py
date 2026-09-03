"""
Unit tests for Storage Adapters (InMemory, SQL, and Redis).
"""

import asyncio
import pytest
import os
from trpc_service.config.models import (
    Session,
    SessionEvent,
    EventType,
    MemoryItem,
    SummaryItem,
    AuditLogEntry,
    AuditDecision,
)
from trpc_service.storage.memory_adapter import InMemoryStorageAdapter
from trpc_service.storage.sql_adapter import SQLStorageAdapter
from trpc_service.storage.redis_adapter import RedisStorageAdapter


def test_in_memory_storage_lock_and_events():
    async def _test():
        adapter = InMemoryStorageAdapter()

        # 1. Lock tests
        assert await adapter.acquire_session_lock("sess_1", 10.0) is True
        assert await adapter.acquire_session_lock("sess_1", 10.0) is False  # Contention
        assert await adapter.release_session_lock("sess_1") is True
        assert await adapter.acquire_session_lock("sess_1", 10.0) is True

        # 2. Idempotency test
        assert await adapter.check_and_set_idempotency("msg_100") is True
        assert await adapter.check_and_set_idempotency("msg_100") is False

        # 3. Session & Event Sourcing
        session = Session(
            session_id="sess_1",
            tenant_id="tenant_1",
            app_id="app_1",
            channel_type="wecom",
            external_user_id="u1",
        )
        await adapter.save_session(session)

        evt1 = SessionEvent(session_id="sess_1", tenant_id="tenant_1", event_type=EventType.USER_MESSAGE, payload={"text": "hello"})
        assert await adapter.append_events("tenant_1", "sess_1", [evt1], expected_version=0) is True

        # Concurrency conflict test
        evt2 = SessionEvent(session_id="sess_1", tenant_id="tenant_1", event_type=EventType.AGENT_MESSAGE, payload={"text": "hi"})
        assert await adapter.append_events("tenant_1", "sess_1", [evt2], expected_version=0) is False  # Version is now 1

        assert await adapter.append_events("tenant_1", "sess_1", [evt2], expected_version=1) is True

        evts = await adapter.get_events("tenant_1", "sess_1")
        assert len(evts) == 2
        assert evts[0].sequence_num == 1
        assert evts[1].sequence_num == 2

    asyncio.run(_test())


def test_sql_storage_adapter(tmp_path):
    async def _test():
        db_file = tmp_path / "test_trpc.db"
        adapter = SQLStorageAdapter(db_url=f"sqlite:///{db_file}")

        # Session & Events
        session = Session(
            session_id="sql_sess_1",
            tenant_id="tenant_sql",
            app_id="app_sql",
            channel_type="telegram",
            external_user_id="u_tg",
        )
        await adapter.save_session(session)

        retrieved = await adapter.get_session("tenant_sql", "sql_sess_1")
        assert retrieved is not None
        assert retrieved.external_user_id == "u_tg"

        evt = SessionEvent(session_id="sql_sess_1", tenant_id="tenant_sql", event_type=EventType.USER_MESSAGE, payload={"msg": "ping"})
        assert await adapter.append_events("tenant_sql", "sql_sess_1", [evt], expected_version=0) is True

        events = await adapter.get_events("tenant_sql", "sql_sess_1")
        assert len(events) == 1

        # Memory test
        mem = MemoryItem(
            memory_id="mem_1",
            tenant_id="tenant_sql",
            user_id="u_tg",
            content="User prefers Chinese language.",
        )
        await adapter.add_memory(mem)

        mems = await adapter.search_memory("tenant_sql", "u_tg", "language")
        assert len(mems) >= 1
        assert mems[0].content == "User prefers Chinese language."

        # Audit log test
        audit = AuditLogEntry(
            log_id="log_1",
            trace_id="trace_sql",
            tenant_id="tenant_sql",
            channel="telegram",
            user_id="u_tg",
            session_id="sql_sess_1",
            agent_name="Assistant",
            decision=AuditDecision.ALLOWED,
            prompt_tokens=100,
            completion_tokens=50,
        )
        await adapter.record_audit_log(audit)
        logs = await adapter.query_audit_logs("tenant_sql")
        assert len(logs) == 1
        assert logs[0].trace_id == "trace_sql"

    asyncio.run(_test())
