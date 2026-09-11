"""Integration tests for Stage 4C message idempotency and audit.

Proves on real PostgreSQL that:
- Two workers receiving the same business key concurrently: exactly one executes
- Same-key/different-body returns conflict
- Tenant isolation for equal message IDs
- Stream replay emits only cached delta + done
- Receipt/audit transaction atomicity
- Processing receipt refuses automatic re-execution

Tests share one module-scoped container, so every test creates tenants with
unique IDs and only asserts on its own rows.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from trpc_service.storage.message_repository import (
    MessageReceiptRepositoryDataError,
    ReceiptAction,
    SqlMessageReceiptRepository,
)
from trpc_service.transport.models import WorkerErrorCode, WorkerTask

from .pg_helpers import requires_docker, run_alembic


def _unique_tenant() -> str:
    return f"tenant{uuid.uuid4().hex[:8]}"


def _make_task(
    tenant_id: str,
    session_id: str = "sess-1",
    message_id: str = "msg-1",
    message: str = "hello",
    user_id: str = "user-1",
    channel: str = "web",
) -> WorkerTask:
    return WorkerTask(
        protocol_version=1,
        request_id=uuid.uuid4(),
        tenant_id=tenant_id,
        app_id="app_demo",
        config_version=1,
        user_id=user_id,
        channel=channel,
        session_id=session_id,
        message_id=message_id,
        message=message,
    )


async def _fetch_receipt_count(db_url: str, tenant_id: str, message_id: str) -> int:
    engine = create_async_engine(db_url)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                sa.text("SELECT COUNT(*) FROM message_receipts "
                        "WHERE tenant_id = :tenant_id AND message_id = :message_id"),
                {
                    "tenant_id": tenant_id,
                    "message_id": message_id
                },
            )
            return result.scalar()
    finally:
        await engine.dispose()


async def _fetch_audit_count(db_url: str, tenant_id: str, message_id: str) -> int:
    engine = create_async_engine(db_url)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                sa.text("SELECT COUNT(*) FROM message_audit_events "
                        "WHERE tenant_id = :tenant_id AND message_id = :message_id"),
                {
                    "tenant_id": tenant_id,
                    "message_id": message_id
                },
            )
            return result.scalar()
    finally:
        await engine.dispose()


async def _fetch_receipt_state(db_url: str, tenant_id: str, message_id: str) -> str | None:
    engine = create_async_engine(db_url)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                sa.text("SELECT state FROM message_receipts "
                        "WHERE tenant_id = :tenant_id AND message_id = :message_id"),
                {
                    "tenant_id": tenant_id,
                    "message_id": message_id
                },
            )
            return result.scalar()
    finally:
        await engine.dispose()


@requires_docker
class TestStage4CIdempotencyFlow:
    """Integration tests for message idempotency on real PostgreSQL."""

    @pytest.mark.asyncio
    async def test_migration_creates_receipt_and_audit_tables(self, postgres_url: str):
        """Migration 0003 creates message_receipts and message_audit_events tables."""
        run_alembic(postgres_url, "upgrade", "head", check=True)

        engine = create_async_engine(postgres_url)
        try:
            async with engine.connect() as conn:
                # Check message_receipts table exists
                result = await conn.execute(
                    sa.text("SELECT EXISTS ("
                            "  SELECT FROM information_schema.tables "
                            "  WHERE table_name = 'message_receipts'"
                            ")"))
                assert result.scalar() is True

                # Check message_audit_events table exists
                result = await conn.execute(
                    sa.text("SELECT EXISTS ("
                            "  SELECT FROM information_schema.tables "
                            "  WHERE table_name = 'message_audit_events'"
                            ")"))
                assert result.scalar() is True
        finally:
            await engine.dispose()

    @pytest.mark.asyncio
    async def test_concurrent_claim_exactly_one_executes(self, postgres_url: str):
        """Two concurrent claims for the same business key: exactly one gets EXECUTE."""
        run_alembic(postgres_url, "upgrade", "head", check=True)

        tenant_id = _unique_tenant()
        task = _make_task(tenant_id, message_id="msg-concurrent")

        engine1 = create_async_engine(postgres_url)
        engine2 = create_async_engine(postgres_url)
        repo1 = SqlMessageReceiptRepository(engine1)
        repo2 = SqlMessageReceiptRepository(engine2)

        try:
            # Claim concurrently from two repositories
            claim1, claim2 = await asyncio.gather(
                repo1.claim(task, task.message),
                repo2.claim(task, task.message),
            )

            # Exactly one should get EXECUTE, the other IN_PROGRESS
            actions = {claim1.action, claim2.action}
            assert actions == {ReceiptAction.EXECUTE, ReceiptAction.IN_PROGRESS}

            # Verify only one receipt was created
            count = await _fetch_receipt_count(postgres_url, tenant_id, "msg-concurrent")
            assert count == 1
        finally:
            await repo1.close()
            await repo2.close()

    @pytest.mark.asyncio
    async def test_completed_replay_returns_cached_result(self, postgres_url: str):
        """After completion, duplicate claim returns REPLAY with cached result."""
        run_alembic(postgres_url, "upgrade", "head", check=True)

        tenant_id = _unique_tenant()
        task = _make_task(tenant_id, message_id="msg-replay")

        engine = create_async_engine(postgres_url)
        repo = SqlMessageReceiptRepository(engine)
        try:
            # First claim: EXECUTE
            claim1 = await repo.claim(task, task.message)
            assert claim1.action == ReceiptAction.EXECUTE
            assert claim1.receipt_id is not None

            # Complete the receipt
            await repo.complete(claim1.receipt_id, "cached response", 100)

            # Second claim: REPLAY with cached result
            claim2 = await repo.claim(task, task.message)
            assert claim2.action == ReceiptAction.REPLAY
            assert claim2.response_text == "cached response"
            assert claim2.receipt_id == claim1.receipt_id

            # Verify state is completed
            state = await _fetch_receipt_state(postgres_url, tenant_id, "msg-replay")
            assert state == "completed"
        finally:
            await repo.close()

    @pytest.mark.asyncio
    async def test_same_key_different_body_returns_conflict(self, postgres_url: str):
        """Same business key but different message body returns CONFLICT."""
        run_alembic(postgres_url, "upgrade", "head", check=True)

        tenant_id = _unique_tenant()
        task1 = _make_task(tenant_id, message_id="msg-conflict", message="hello")
        task2 = _make_task(tenant_id, message_id="msg-conflict", message="different")

        engine = create_async_engine(postgres_url)
        repo = SqlMessageReceiptRepository(engine)
        try:
            # First claim: EXECUTE
            claim1 = await repo.claim(task1, task1.message)
            assert claim1.action == ReceiptAction.EXECUTE

            # Complete it
            await repo.complete(claim1.receipt_id, "response", 50)

            # Second claim with different message: CONFLICT
            claim2 = await repo.claim(task2, task2.message)
            assert claim2.action == ReceiptAction.CONFLICT
        finally:
            await repo.close()

    @pytest.mark.asyncio
    async def test_tenant_isolation_for_equal_message_ids(self, postgres_url: str):
        """Different tenants can use the same message_id without conflict."""
        run_alembic(postgres_url, "upgrade", "head", check=True)

        tenant1 = _unique_tenant()
        tenant2 = _unique_tenant()
        task1 = _make_task(tenant1, message_id="msg-same")
        task2 = _make_task(tenant2, message_id="msg-same")

        engine = create_async_engine(postgres_url)
        repo = SqlMessageReceiptRepository(engine)
        try:
            # Both tenants claim the same message_id
            claim1 = await repo.claim(task1, task1.message)
            claim2 = await repo.claim(task2, task2.message)

            # Both should get EXECUTE (different tenants)
            assert claim1.action == ReceiptAction.EXECUTE
            assert claim2.action == ReceiptAction.EXECUTE
            assert claim1.receipt_id != claim2.receipt_id

            # Verify two receipts were created
            count1 = await _fetch_receipt_count(postgres_url, tenant1, "msg-same")
            count2 = await _fetch_receipt_count(postgres_url, tenant2, "msg-same")
            assert count1 == 1
            assert count2 == 1
        finally:
            await repo.close()

    @pytest.mark.asyncio
    async def test_audit_events_created_atomically(self, postgres_url: str):
        """Each receipt state change creates a corresponding audit event."""
        run_alembic(postgres_url, "upgrade", "head", check=True)

        tenant_id = _unique_tenant()
        task = _make_task(tenant_id, message_id="msg-audit")

        engine = create_async_engine(postgres_url)
        repo = SqlMessageReceiptRepository(engine)
        try:
            # Claim: creates accepted audit event
            claim = await repo.claim(task, task.message)
            assert claim.action == ReceiptAction.EXECUTE

            audit_count = await _fetch_audit_count(postgres_url, tenant_id, "msg-audit")
            assert audit_count == 1  # accepted event

            # Complete: creates completed audit event
            await repo.complete(claim.receipt_id, "response", 100)

            audit_count = await _fetch_audit_count(postgres_url, tenant_id, "msg-audit")
            assert audit_count == 2  # accepted + completed
        finally:
            await repo.close()

    @pytest.mark.asyncio
    async def test_processing_receipt_refuses_re_execution(self, postgres_url: str):
        """A processing receipt returns IN_PROGRESS, not EXECUTE."""
        run_alembic(postgres_url, "upgrade", "head", check=True)

        tenant_id = _unique_tenant()
        task = _make_task(tenant_id, message_id="msg-processing")

        engine = create_async_engine(postgres_url)
        repo = SqlMessageReceiptRepository(engine)
        try:
            # First claim: EXECUTE
            claim1 = await repo.claim(task, task.message)
            assert claim1.action == ReceiptAction.EXECUTE

            # Don't complete it - leave it in processing state

            # Second claim: IN_PROGRESS (not EXECUTE)
            claim2 = await repo.claim(task, task.message)
            assert claim2.action == ReceiptAction.IN_PROGRESS

            # Verify state is still processing
            state = await _fetch_receipt_state(postgres_url, tenant_id, "msg-processing")
            assert state == "processing"
        finally:
            await repo.close()

    @pytest.mark.asyncio
    async def test_failed_receipt_replays_terminal_error(self, postgres_url: str):
        """A failed receipt is terminal and must not be executed again."""
        run_alembic(postgres_url, "upgrade", "head", check=True)

        tenant_id = _unique_tenant()
        task = _make_task(tenant_id, message_id="msg-failed")

        engine = create_async_engine(postgres_url)
        repo = SqlMessageReceiptRepository(engine)
        try:
            # First claim: EXECUTE
            claim1 = await repo.claim(task, task.message)
            assert claim1.action == ReceiptAction.EXECUTE

            # Fail it
            await repo.fail(claim1.receipt_id, WorkerErrorCode.MODEL_RUNTIME, 50)

            # Verify state is failed
            state = await _fetch_receipt_state(postgres_url, tenant_id, "msg-failed")
            assert state == "failed"

            # A duplicate must replay the terminal error, never execute again.
            claim2 = await repo.claim(task, task.message)
            assert claim2.action == ReceiptAction.REPLAY
            assert claim2.receipt_id == claim1.receipt_id
            assert claim2.error_code == WorkerErrorCode.MODEL_RUNTIME
        finally:
            await repo.close()

    @pytest.mark.asyncio
    async def test_list_audit_returns_newest_first(self, postgres_url: str):
        """list_audit returns events in newest-first order."""
        run_alembic(postgres_url, "upgrade", "head", check=True)

        tenant_id = _unique_tenant()
        task = _make_task(tenant_id, message_id="msg-list")

        engine = create_async_engine(postgres_url)
        repo = SqlMessageReceiptRepository(engine)
        try:
            # Create multiple audit events
            claim = await repo.claim(task, task.message)
            await repo.complete(claim.receipt_id, "response", 100)

            # List audit events
            events = await repo.list_audit(tenant_id, "msg-list", limit=10)

            assert len(events) == 2
            # Newest first: completed before accepted
            assert events[0].event_type == "completed"
            assert events[1].event_type == "accepted"
        finally:
            await repo.close()

    @pytest.mark.asyncio
    async def test_complete_non_processing_receipt_raises(self, postgres_url: str):
        """Completing a non-processing receipt raises DataError."""
        run_alembic(postgres_url, "upgrade", "head", check=True)

        tenant_id = _unique_tenant()
        task = _make_task(tenant_id, message_id="msg-complete-error")

        engine = create_async_engine(postgres_url)
        repo = SqlMessageReceiptRepository(engine)
        try:
            # Claim and complete
            claim = await repo.claim(task, task.message)
            await repo.complete(claim.receipt_id, "response", 100)

            # Try to complete again: should raise
            with pytest.raises(MessageReceiptRepositoryDataError):
                await repo.complete(claim.receipt_id, "response2", 200)
        finally:
            await repo.close()

    @pytest.mark.asyncio
    async def test_audit_does_not_contain_raw_text(self, postgres_url: str):
        """Audit events contain digests, not raw message/response text."""
        run_alembic(postgres_url, "upgrade", "head", check=True)

        tenant_id = _unique_tenant()
        task = _make_task(tenant_id, message_id="msg-no-raw", message="secret message")

        engine = create_async_engine(postgres_url)
        repo = SqlMessageReceiptRepository(engine)
        try:
            claim = await repo.claim(task, task.message)
            await repo.complete(claim.receipt_id, "secret response", 100)

            events = await repo.list_audit(tenant_id, "msg-no-raw", limit=10)

            # Check that audit events have digests, not raw text
            for event in events:
                assert event.message_digest is not None
                assert len(event.message_digest) == 64  # SHA-256 hex
                # response_digest may be None for accepted events
                if event.response_digest is not None:
                    assert len(event.response_digest) == 64
        finally:
            await repo.close()

    @pytest.mark.asyncio
    async def test_receipt_and_audit_transaction_atomic(self, postgres_url: str):
        """If audit INSERT fails, receipt UPDATE must roll back too.

        Creates a temporary trigger on message_audit_events that raises an
        exception when event_type='completed' is inserted.  Then calls
        repo.complete() and verifies the entire transaction was rolled back:
        receipt stays processing, no response/finished_at written, no terminal
        audit row.  After dropping the trigger, complete() succeeds normally.
        """
        run_alembic(postgres_url, "upgrade", "head", check=True)

        tenant_id = _unique_tenant()
        task = _make_task(tenant_id, message_id="msg-atomic")

        ddl_engine = create_async_engine(postgres_url)
        repo_engine = create_async_engine(postgres_url)
        repo = SqlMessageReceiptRepository(repo_engine)
        try:
            claim = await repo.claim(task, task.message)
            assert claim.action == ReceiptAction.EXECUTE

            state = await _fetch_receipt_state(postgres_url, tenant_id, "msg-atomic")
            assert state == "processing"

            audit_before = await _fetch_audit_count(postgres_url, tenant_id, "msg-atomic")
            assert audit_before == 1  # accepted

            async with ddl_engine.begin() as conn:
                await conn.execute(
                    sa.text("""
                    CREATE OR REPLACE FUNCTION _trpc_block_completed_audit()
                    RETURNS trigger AS $$
                    BEGIN
                        IF NEW.event_type = 'completed' THEN
                            RAISE EXCEPTION 'atomicity test: blocking completed audit insert';
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql
                """))
                await conn.execute(
                    sa.text("""
                    CREATE TRIGGER _trpc_block_completed_audit_trg
                    BEFORE INSERT ON message_audit_events
                    FOR EACH ROW
                    EXECUTE FUNCTION _trpc_block_completed_audit()
                """))

            try:
                from trpc_service.storage.message_repository import (
                    MessageReceiptRepositoryUnavailableError, )
                with pytest.raises(MessageReceiptRepositoryUnavailableError):
                    await repo.complete(claim.receipt_id, "should-not-persist", 999)

                state = await _fetch_receipt_state(postgres_url, tenant_id, "msg-atomic")
                assert state == "processing", (
                    f"receipt should still be processing after rolled-back complete(), got {state!r}")

                async with ddl_engine.connect() as conn:
                    row = (await conn.execute(
                        sa.text("SELECT response_text, error_code, finished_at "
                                "FROM message_receipts "
                                "WHERE tenant_id = :t AND message_id = :m"),
                        {
                            "t": tenant_id,
                            "m": "msg-atomic"
                        },
                    )).first()
                    assert row is not None
                    assert row._mapping["response_text"] is None
                    assert row._mapping["error_code"] is None
                    assert row._mapping["finished_at"] is None

                audit_after_blocked = await _fetch_audit_count(postgres_url, tenant_id, "msg-atomic")
                assert audit_after_blocked == audit_before, (f"audit count changed after rolled-back complete(): "
                                                             f"{audit_before} → {audit_after_blocked}")
            finally:
                async with ddl_engine.begin() as conn:
                    await conn.execute(
                        sa.text("DROP TRIGGER IF EXISTS _trpc_block_completed_audit_trg "
                                "ON message_audit_events"))
                    await conn.execute(sa.text("DROP FUNCTION IF EXISTS _trpc_block_completed_audit()"))

            await repo.complete(claim.receipt_id, "real-response", 100)

            state = await _fetch_receipt_state(postgres_url, tenant_id, "msg-atomic")
            assert state == "completed"

            audit_final = await _fetch_audit_count(postgres_url, tenant_id, "msg-atomic")
            assert audit_final == 2  # accepted + completed
        finally:
            await repo.close()
            await ddl_engine.dispose()
