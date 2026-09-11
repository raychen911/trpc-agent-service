"""Focused tests for the shared receipt lifecycle (worker/lifecycle.py).

These pin the claim→terminal mapping and the fail-transaction contract
that chat() and stream() both rely on: repository failures become the
fixed TENANT_REPOSITORY_UNAVAILABLE terminal, never a half-written one.
"""

from __future__ import annotations

import time
import uuid

import pytest

from trpc_service.agent.errors import TenantAgentConfigurationError
from trpc_service.agent.execution_coordinator import SessionBusyError, SessionExecutionLostError
from trpc_service.config import ModelConfigurationError
from trpc_service.storage.message_repository import (
    MessageClaim,
    MessageReceiptRepositoryDataError,
    MessageReceiptRepositoryUnavailableError,
    ReceiptAction,
)
from trpc_service.transport.models import WorkerErrorCode, WorkerTask
from trpc_service.worker.lifecycle import (
    ClaimResult,
    ReceiptLifecycle,
    TerminalOutcome,
    execution_error_code,
)


def _task(**overrides) -> WorkerTask:
    defaults = {
        "protocol_version": 1,
        "request_id": uuid.uuid4(),
        "tenant_id": "tenant_default",
        "app_id": "app_demo",
        "config_version": 1,
        "user_id": "user_default",
        "channel": "web",
        "session_id": "sess-1",
        "message_id": "msg-1",
        "message": "hello",
    }
    defaults.update(overrides)
    return WorkerTask(**defaults)


class _FakeRepository:
    """Minimal claim/fail fake: no complete/list_audit needed by lifecycle."""

    def __init__(self,
                 claim: MessageClaim | None = None,
                 claim_error: Exception | None = None,
                 fail_error: Exception | None = None) -> None:
        self._claim = claim
        self._claim_error = claim_error
        self._fail_error = fail_error
        self.claim_calls: list[tuple[WorkerTask, str]] = []
        self.fail_calls: list[tuple[uuid.UUID, WorkerErrorCode, int, tuple]] = []

    async def claim(self, task: WorkerTask, message_text: str) -> MessageClaim:
        self.claim_calls.append((task, message_text))
        if self._claim_error is not None:
            raise self._claim_error
        assert self._claim is not None
        return self._claim

    async def fail(self, receipt_id, error_code, latency_ms, execution_events=()):
        self.fail_calls.append((receipt_id, error_code, latency_ms, tuple(execution_events)))
        if self._fail_error is not None:
            raise self._fail_error

    async def complete(self, receipt_id, response_text, latency_ms, execution_events=()):
        raise AssertionError("complete is not part of the lifecycle contract")


def _execute_claim(receipt_id: uuid.UUID | None = None) -> MessageClaim:
    return MessageClaim(action=ReceiptAction.EXECUTE,
                        receipt_id=receipt_id or uuid.uuid4(),
                        response_text=None,
                        error_code=None)


class TestClaimOrTerminal:
    pytestmark = pytest.mark.asyncio

    async def test_disabled_lifecycle_is_empty(self):
        lifecycle = ReceiptLifecycle(None)
        assert lifecycle.enabled is False
        result = await lifecycle.claim_or_terminal(_task())
        assert result == ClaimResult(receipt_id=None, terminal=None)

    async def test_execute_claim_returns_receipt_id(self):
        receipt_id = uuid.uuid4()
        lifecycle = ReceiptLifecycle(_FakeRepository(claim=_execute_claim(receipt_id)))
        assert lifecycle.enabled is True
        result = await lifecycle.claim_or_terminal(_task())
        assert result.receipt_id == receipt_id
        assert result.terminal is None

    async def test_replay_of_failed_receipt_carries_its_error(self):
        lifecycle = ReceiptLifecycle(
            _FakeRepository(claim=MessageClaim(
                action=ReceiptAction.REPLAY,
                receipt_id=None,
                response_text=None,
                error_code=WorkerErrorCode.CONTENT_INPUT_BLOCKED,
            )))
        result = await lifecycle.claim_or_terminal(_task())
        assert result.terminal == TerminalOutcome(
            error_code=WorkerErrorCode.CONTENT_INPUT_BLOCKED,
            response_text="",
            replayed=True,
        )

    async def test_replay_of_completed_receipt_carries_its_text(self):
        lifecycle = ReceiptLifecycle(
            _FakeRepository(claim=MessageClaim(
                action=ReceiptAction.REPLAY,
                receipt_id=None,
                response_text="prior answer",
                error_code=None,
            )))
        result = await lifecycle.claim_or_terminal(_task())
        assert result.terminal == TerminalOutcome(error_code=None, response_text="prior answer", replayed=True)

    @pytest.mark.parametrize(
        "action,expected",
        [
            (ReceiptAction.IN_PROGRESS, WorkerErrorCode.MESSAGE_IN_PROGRESS),
            (ReceiptAction.CONFLICT, WorkerErrorCode.IDEMPOTENCY_CONFLICT),
        ],
    )
    async def test_in_progress_and_conflict_map_to_fixed_codes(self, action, expected):
        lifecycle = ReceiptLifecycle(
            _FakeRepository(claim=MessageClaim(action=action, receipt_id=None, response_text=None, error_code=None)))
        result = await lifecycle.claim_or_terminal(_task())
        assert result.terminal == TerminalOutcome(error_code=expected, response_text="", replayed=False)

    @pytest.mark.parametrize(
        "error,expected",
        [
            (MessageReceiptRepositoryUnavailableError(), WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE),
            (MessageReceiptRepositoryDataError(), WorkerErrorCode.MODEL_RUNTIME),
        ],
    )
    async def test_claim_repository_failures_map_to_fixed_codes(self, error, expected):
        lifecycle = ReceiptLifecycle(_FakeRepository(claim_error=error))
        result = await lifecycle.claim_or_terminal(_task())
        assert result.terminal == TerminalOutcome(error_code=expected, response_text="", replayed=False)


class TestFailTransaction:
    pytestmark = pytest.mark.asyncio

    async def test_successful_fail_returns_requested_code(self):
        repository = _FakeRepository()
        lifecycle = ReceiptLifecycle(repository)
        receipt_id = uuid.uuid4()
        events = ("event-1", )
        code = await lifecycle.fail(receipt_id, WorkerErrorCode.MODEL_RUNTIME, time.monotonic(), events)
        assert code == WorkerErrorCode.MODEL_RUNTIME
        (called_id, called_code, _latency, called_events), = repository.fail_calls
        assert called_id == receipt_id
        assert called_code == WorkerErrorCode.MODEL_RUNTIME
        assert called_events == events

    @pytest.mark.parametrize(
        "error",
        [MessageReceiptRepositoryUnavailableError(),
         MessageReceiptRepositoryDataError()],
    )
    async def test_repository_failure_returns_none_not_half_written_terminal(self, error):
        repository = _FakeRepository(fail_error=error)
        lifecycle = ReceiptLifecycle(repository)
        code = await lifecycle.fail(uuid.uuid4(), WorkerErrorCode.MODEL_RUNTIME, time.monotonic(), ())
        assert code is None
        assert len(repository.fail_calls) == 1


class TestExecutionErrorCode:

    @pytest.mark.parametrize(
        "exc,expected",
        [
            (TenantAgentConfigurationError(), WorkerErrorCode.TENANT_AGENT_CONFIGURATION),
            (ModelConfigurationError("bad model"), WorkerErrorCode.MODEL_CONFIGURATION),
            (SessionBusyError(), WorkerErrorCode.SESSION_BUSY),
            (SessionExecutionLostError(), WorkerErrorCode.MODEL_RUNTIME),
            (RuntimeError("boom"), WorkerErrorCode.MODEL_RUNTIME),
        ],
    )
    def test_exception_mapping_preserves_handler_order(self, exc, expected):
        assert execution_error_code(exc, "chat") == expected
