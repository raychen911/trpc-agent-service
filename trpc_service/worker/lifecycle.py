"""Receipt-claim and terminal-transaction lifecycle shared by chat/stream.

Extracted from ``WorkerService`` so the sync chat and SSE stream paths
share ONE implementation of:

- the message-receipt claim (replay / in-progress / conflict / repository
  failures) and its fixed terminal mapping;
- the failing terminal transaction (``fail`` with the recorded execution
  events), mapping repository write failures to the fixed
  ``TENANT_REPOSITORY_UNAVAILABLE`` code;
- the execution exception → ``WorkerErrorCode`` mapping.

Every repository failure maps to a fixed safe code: the caller translates
a single value into either a ``WorkerChatResult`` or ``WorkerEvent``s.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass

from trpc_service.agent.errors import TenantAgentConfigurationError
from trpc_service.agent.execution_coordinator import SessionBusyError, SessionExecutionLostError
from trpc_service.config import ModelConfigurationError
from trpc_service.storage.message_repository import (
    MessageReceiptRepository,
    MessageReceiptRepositoryDataError,
    MessageReceiptRepositoryUnavailableError,
    ReceiptAction,
)
from trpc_service.transport.models import WorkerErrorCode, WorkerTask

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TerminalOutcome:
    """A fixed terminal for the request — no execution happens after it.

    ``replayed`` marks a REPLAY claim: chat returns the stored response
    and its recorded error code (a failed receipt replays its failure);
    stream turns the same facts into either an error event or a delta
    followed by ``done``.
    """

    error_code: WorkerErrorCode | None = None
    response_text: str = ""
    replayed: bool = False


@dataclass(frozen=True)
class ClaimResult:
    """Result of the receipt claim.

    ``terminal`` set → return it immediately; otherwise ``receipt_id`` is
    the claimed receipt whose terminal transactions this request owns.
    Both empty → legacy in-memory mode (no receipt repository wired).
    """

    receipt_id: uuid.UUID | None = None
    terminal: TerminalOutcome | None = None


def execution_error_code(exc: BaseException, operation: str) -> WorkerErrorCode:
    """Map an execution exception to its fixed protocol error code.

    The isinstance order replicates the previous per-except ordering of
    the chat/stream handlers (tenant config → model config → session busy
    → session lost → unexpected runtime).
    """
    if isinstance(exc, TenantAgentConfigurationError):
        return WorkerErrorCode.TENANT_AGENT_CONFIGURATION
    if isinstance(exc, ModelConfigurationError):
        return WorkerErrorCode.MODEL_CONFIGURATION
    if isinstance(exc, SessionBusyError):
        return WorkerErrorCode.SESSION_BUSY
    if isinstance(exc, SessionExecutionLostError):
        logger.warning("WorkerService.%s session execution lost", operation)
        return WorkerErrorCode.MODEL_RUNTIME
    logger.warning("WorkerService.%s unexpected error", operation)
    return WorkerErrorCode.MODEL_RUNTIME


class ReceiptLifecycle:
    """Message-receipt claim and failing terminal transaction."""

    def __init__(self, receipt_repository: MessageReceiptRepository | None) -> None:
        self._receipt_repository = receipt_repository

    @property
    def enabled(self) -> bool:
        return self._receipt_repository is not None

    async def claim_or_terminal(self, task: WorkerTask) -> ClaimResult:
        """Claim the receipt for ``task``.

        Duplicate/in-progress/conflicting deliveries and repository
        failures all become fixed terminals — none of them re-runs the
        model or a tool.
        """
        repository = self._receipt_repository
        if repository is None:
            return ClaimResult()
        try:
            claim = await repository.claim(task, task.message)
        except MessageReceiptRepositoryUnavailableError:
            logger.warning("WorkerService receipt repository unavailable")
            return ClaimResult(terminal=TerminalOutcome(error_code=WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE))
        except MessageReceiptRepositoryDataError:
            logger.warning("WorkerService receipt claim data error")
            return ClaimResult(terminal=TerminalOutcome(error_code=WorkerErrorCode.MODEL_RUNTIME))
        if claim.action == ReceiptAction.REPLAY:
            return ClaimResult(terminal=TerminalOutcome(
                error_code=claim.error_code,
                response_text=claim.response_text or "",
                replayed=True,
            ))
        if claim.action == ReceiptAction.IN_PROGRESS:
            return ClaimResult(terminal=TerminalOutcome(error_code=WorkerErrorCode.MESSAGE_IN_PROGRESS))
        if claim.action == ReceiptAction.CONFLICT:
            return ClaimResult(terminal=TerminalOutcome(error_code=WorkerErrorCode.IDEMPOTENCY_CONFLICT))
        return ClaimResult(receipt_id=claim.receipt_id)

    async def fail(
        self,
        receipt_id: uuid.UUID,
        code: WorkerErrorCode,
        start_time: float,
        execution_events,
    ) -> WorkerErrorCode | None:
        """Fail the receipt in one terminal transaction.

        Returns ``code`` when the write succeeded; ``None`` when the
        repository write failed (the caller terminally reports
        ``TENANT_REPOSITORY_UNAVAILABLE`` instead — never the requested
        code, so a half-written terminal cannot be replayed as success).
        """
        latency_ms = int((time.monotonic() - start_time) * 1000)
        try:
            await self._receipt_repository.fail(
                receipt_id,
                code,
                latency_ms,
                execution_events=execution_events,
            )
        except (MessageReceiptRepositoryUnavailableError, MessageReceiptRepositoryDataError) as exc:
            logger.warning("WorkerService receipt fail failed type=%s", type(exc).__name__)
            return None
        return code


__all__ = [
    "ClaimResult",
    "ReceiptLifecycle",
    "TerminalOutcome",
    "execution_error_code",
]
