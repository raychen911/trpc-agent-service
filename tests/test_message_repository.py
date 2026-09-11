"""Tests for message receipt repository with real PostgreSQL."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from trpc_service.storage.database import DatabaseSettings, create_database_engine
from trpc_service.storage.message_repository import (
    MessageReceiptRepositoryDataError,
    MessageReceiptRepositoryUnavailableError,
    ReceiptAction,
    SqlMessageReceiptRepository,
    compute_message_digest,
    compute_response_digest,
)
from trpc_service.transport.models import WorkerErrorCode, WorkerTask


def _make_task(
    tenant_id: str = "tenant_test",
    channel: str = "web",
    user_id: str = "user_test",
    session_id: str = "session_test",
    message_id: str = "msg_test",
    app_id: str = "app_test",
    config_version: int = 1,
    message: str = "hello world",
) -> WorkerTask:
    return WorkerTask(
        protocol_version=1,
        request_id=uuid.uuid4(),
        tenant_id=tenant_id,
        app_id=app_id,
        config_version=config_version,
        user_id=user_id,
        channel=channel,
        session_id=session_id,
        message_id=message_id,
        message=message,
    )


def _run_async(coro):
    """Helper to run async code in sync tests."""
    return asyncio.run(coro)


def test_compute_message_digest_is_sha256() -> None:
    digest = compute_message_digest("hello world")
    assert len(digest) == 64
    assert digest == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"


def test_compute_response_digest_is_sha256() -> None:
    digest = compute_response_digest("response text")
    assert len(digest) == 64


def _skip_if_no_db():
    try:
        DatabaseSettings.from_env()
        return False
    except Exception:
        return True


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_claim_first_time_returns_execute() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        try:
            await repo.check_ready()
            task = _make_task(message_id=f"msg_{uuid.uuid4()}")
            claim = await repo.claim(task, "hello world")
            assert claim.action == ReceiptAction.EXECUTE
            assert claim.receipt_id is not None
            assert claim.response_text is None
            assert claim.error_code is None
        finally:
            await repo.close()

    _run_async(_test())


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_claim_completed_returns_replay() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        try:
            await repo.check_ready()
            task = _make_task(message_id=f"msg_{uuid.uuid4()}")
            claim1 = await repo.claim(task, "hello world")
            assert claim1.action == ReceiptAction.EXECUTE
            await repo.complete(claim1.receipt_id, "response text", 100)

            claim2 = await repo.claim(task, "hello world")
            assert claim2.action == ReceiptAction.REPLAY
            assert claim2.receipt_id == claim1.receipt_id
            assert claim2.response_text == "response text"
            assert claim2.error_code is None
        finally:
            await repo.close()

    _run_async(_test())


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_claim_processing_returns_in_progress() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        try:
            await repo.check_ready()
            task = _make_task(message_id=f"msg_{uuid.uuid4()}")
            claim1 = await repo.claim(task, "hello world")
            assert claim1.action == ReceiptAction.EXECUTE

            claim2 = await repo.claim(task, "hello world")
            assert claim2.action == ReceiptAction.IN_PROGRESS
            assert claim2.receipt_id == claim1.receipt_id
            assert claim2.response_text is None
            assert claim2.error_code == WorkerErrorCode.MESSAGE_IN_PROGRESS
        finally:
            await repo.close()

    _run_async(_test())


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_claim_failed_returns_replay_with_error() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        try:
            await repo.check_ready()
            task = _make_task(message_id=f"msg_{uuid.uuid4()}")
            claim1 = await repo.claim(task, "hello world")
            assert claim1.action == ReceiptAction.EXECUTE
            await repo.fail(claim1.receipt_id, WorkerErrorCode.MODEL_RUNTIME, 50)

            claim2 = await repo.claim(task, "hello world")
            assert claim2.action == ReceiptAction.REPLAY
            assert claim2.receipt_id == claim1.receipt_id
            assert claim2.response_text is None
            assert claim2.error_code == WorkerErrorCode.MODEL_RUNTIME
        finally:
            await repo.close()

    _run_async(_test())


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_claim_different_digest_returns_conflict() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        try:
            await repo.check_ready()
            task = _make_task(message_id=f"msg_{uuid.uuid4()}")
            claim1 = await repo.claim(task, "hello world")
            assert claim1.action == ReceiptAction.EXECUTE
            await repo.complete(claim1.receipt_id, "response", 100)

            claim2 = await repo.claim(task, "different message")
            assert claim2.action == ReceiptAction.CONFLICT
            assert claim2.receipt_id is None
            assert claim2.error_code == WorkerErrorCode.IDEMPOTENCY_CONFLICT
        finally:
            await repo.close()

    _run_async(_test())


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_complete_non_processing_raises() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        try:
            await repo.check_ready()
            task = _make_task(message_id=f"msg_{uuid.uuid4()}")
            claim = await repo.claim(task, "hello")
            await repo.complete(claim.receipt_id, "response", 100)

            with pytest.raises(MessageReceiptRepositoryDataError):
                await repo.complete(claim.receipt_id, "response2", 200)
        finally:
            await repo.close()

    _run_async(_test())


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_fail_non_processing_raises() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        try:
            await repo.check_ready()
            task = _make_task(message_id=f"msg_{uuid.uuid4()}")
            claim = await repo.claim(task, "hello")
            await repo.fail(claim.receipt_id, WorkerErrorCode.MODEL_RUNTIME, 50)

            with pytest.raises(MessageReceiptRepositoryDataError):
                await repo.fail(claim.receipt_id, WorkerErrorCode.MODEL_RUNTIME, 100)
        finally:
            await repo.close()

    _run_async(_test())


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_complete_missing_receipt_raises() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        try:
            await repo.check_ready()
            with pytest.raises(MessageReceiptRepositoryDataError):
                await repo.complete(uuid.uuid4(), "response", 100)
        finally:
            await repo.close()

    _run_async(_test())


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_list_audit_returns_events_newest_first() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        try:
            await repo.check_ready()
            task = _make_task(message_id=f"msg_{uuid.uuid4()}")
            claim = await repo.claim(task, "hello")
            await repo.complete(claim.receipt_id, "response", 100)

            events = await repo.list_audit(task.tenant_id, task.message_id, 50)
            assert len(events) == 2
            assert events[0].event_type == "completed"
            assert events[1].event_type == "accepted"
            assert events[0].response_digest is not None
            assert events[1].response_digest is None
        finally:
            await repo.close()

    _run_async(_test())


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_audit_does_not_contain_raw_text() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        try:
            await repo.check_ready()
            task = _make_task(message_id=f"msg_{uuid.uuid4()}")
            claim = await repo.claim(task, "secret message")
            await repo.complete(claim.receipt_id, "secret response", 100)

            events = await repo.list_audit(task.tenant_id, task.message_id, 50)
            for event in events:
                assert event.message_digest != "secret message"
                if event.response_digest:
                    assert event.response_digest != "secret response"
        finally:
            await repo.close()

    _run_async(_test())


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_different_tenants_isolated() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        try:
            await repo.check_ready()
            task1 = _make_task(tenant_id="tenant_a", message_id=f"msg_{uuid.uuid4()}")
            task2 = _make_task(tenant_id="tenant_b", message_id=task1.message_id)

            claim1 = await repo.claim(task1, "hello")
            claim2 = await repo.claim(task2, "hello")

            assert claim1.action == ReceiptAction.EXECUTE
            assert claim2.action == ReceiptAction.EXECUTE
            assert claim1.receipt_id != claim2.receipt_id
        finally:
            await repo.close()

    _run_async(_test())


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_closed_repository_raises() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        await repo.check_ready()
        await repo.close()
        task = _make_task(message_id=f"msg_{uuid.uuid4()}")
        with pytest.raises(MessageReceiptRepositoryUnavailableError):
            await repo.claim(task, "hello")

    _run_async(_test())


@pytest.mark.skipif(_skip_if_no_db(), reason="TRPC_DATABASE_URL not set")
def test_concurrent_claim_exactly_one_executes() -> None:

    async def _test():
        settings = DatabaseSettings.from_env()
        engine = create_database_engine(settings)
        repo = SqlMessageReceiptRepository(engine)
        try:
            await repo.check_ready()
            task = _make_task(message_id=f"msg_{uuid.uuid4()}")

            async def claim_task():
                return await repo.claim(task, "hello")

            results = await asyncio.gather(claim_task(), claim_task(), claim_task())
            execute_count = sum(1 for r in results if r.action == ReceiptAction.EXECUTE)
            in_progress_count = sum(1 for r in results if r.action == ReceiptAction.IN_PROGRESS)

            assert execute_count == 1
            assert in_progress_count == 2
        finally:
            await repo.close()

    _run_async(_test())
