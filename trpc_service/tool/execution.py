"""Stable execution keys and non-idempotent ambiguity handling."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from trpc_service.faults import FaultStage, FaultStageController, FaultStageEvent
from trpc_service.tenant.models import TenantContext, ToolRisk
from trpc_service.tool.confirmation import arguments_hash


class ExecutionStatus(StrEnum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True, slots=True)
class ExecutionRecord:
    execution_key: str
    status: ExecutionStatus
    result: Any = None
    fresh: bool = False
    # A terminal success without a persisted result may be re-run only when
    # the storage ledger has validated the current turn fence.  The ledger
    # remains terminal and must not be finished/overwritten by the retry.
    replay_terminal: bool = False


class ExecutionLedger(Protocol):
    async def begin(
        self,
        execution_key: str,
        *,
        tenant_id: str,
        turn_id: str,
        tool_name: str,
        risk: ToolRisk,
        arguments_hash: str,
        owner_id: str | None = None,
        fencing_token: int | None = None,
    ) -> ExecutionRecord: ...

    async def finish(
        self,
        execution_key: str,
        *,
        tenant_id: str,
        status: ExecutionStatus,
        result: Any = None,
        owner_id: str | None = None,
        fencing_token: int | None = None,
    ) -> None: ...


class InMemoryExecutionLedger:
    def __init__(self) -> None:
        self.records: dict[str, ExecutionRecord] = {}

    async def begin(
        self,
        execution_key: str,
        *,
        tenant_id: str,
        turn_id: str,
        tool_name: str,
        risk: ToolRisk,
        arguments_hash: str,
        owner_id: str | None = None,
        fencing_token: int | None = None,
    ) -> ExecutionRecord:
        existing = self.records.get(execution_key)
        if existing:
            if (
                existing.status == ExecutionStatus.SUCCEEDED
                and existing.result is None
                and risk == ToolRisk.IDEMPOTENT
            ):
                return ExecutionRecord(
                    execution_key,
                    existing.status,
                    replay_terminal=True,
                )
            return existing
        record = ExecutionRecord(execution_key, ExecutionStatus.STARTED, fresh=True)
        self.records[execution_key] = ExecutionRecord(execution_key, ExecutionStatus.STARTED)
        return record

    async def finish(
        self,
        execution_key: str,
        *,
        tenant_id: str,
        status: ExecutionStatus,
        result: Any = None,
        owner_id: str | None = None,
        fencing_token: int | None = None,
    ) -> None:
        self.records[execution_key] = ExecutionRecord(execution_key, status, result)


class HumanReviewRequired(RuntimeError):
    pass


class ToolExecutor:
    def __init__(
        self,
        key: bytes,
        ledger: ExecutionLedger,
        *,
        fault_stages: FaultStageController | None = None,
        worker_id: str | None = None,
        fault_stage_delay_seconds: float = 0.0,
    ) -> None:
        if len(key) < 32:
            raise ValueError("tool execution key must contain at least 32 bytes")
        if not 0 <= fault_stage_delay_seconds <= 5:
            raise ValueError("fault-stage delay must be between 0 and 5 seconds")
        self._key = key
        self._ledger = ledger
        # Fault-stage observation is opt-in so existing SDK/worker callers keep
        # the same hot path and do not open an additional marker connection.
        self._fault_stages = fault_stages
        self._worker_id = worker_id or "tool-executor"
        self._fault_stage_delay_seconds = fault_stage_delay_seconds

    def key_for(
        self,
        context: TenantContext,
        *,
        turn_id: str,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> str:
        canonical = json.dumps(
            [
                context.tenant_id,
                context.session_id,
                turn_id,
                tool_name,
                arguments_hash(arguments),
            ],
            separators=(",", ":"),
        )
        return hmac.new(self._key, canonical.encode(), hashlib.sha256).hexdigest()

    async def execute(
        self,
        context: TenantContext,
        *,
        turn_id: str,
        tool_name: str,
        arguments: dict[str, Any],
        risk: ToolRisk,
        call: Callable[[], Awaitable[Any]],
        owner_id: str | None = None,
        fencing_token: int | None = None,
    ) -> Any:
        key = self.key_for(
            context,
            turn_id=turn_id,
            tool_name=tool_name,
            arguments=arguments,
        )
        record = await self._ledger.begin(
            key,
            tenant_id=context.tenant_id,
            turn_id=turn_id,
            tool_name=tool_name,
            risk=risk,
            arguments_hash=arguments_hash(arguments),
            owner_id=owner_id,
            fencing_token=fencing_token,
        )
        replay_terminal = record.status == ExecutionStatus.SUCCEEDED and record.replay_terminal
        if record.status == ExecutionStatus.SUCCEEDED:
            if record.result is not None:
                return record.result
            if risk != ToolRisk.IDEMPOTENT:
                raise HumanReviewRequired("tool succeeded previously; result is unavailable")
        if (
            not record.fresh
            and record.status in {ExecutionStatus.STARTED, ExecutionStatus.AMBIGUOUS}
            and risk != ToolRisk.IDEMPOTENT
        ):
            raise HumanReviewRequired("non-idempotent tool outcome is unknown")
        if self._fault_stages is not None:
            if self._fault_stage_delay_seconds:
                # Only the explicit TEST fault runtime supplies this delay.
                # It creates a deterministic window after the durable
                # started ledger row and before the marker checkpoint;
                # ordinary tool execution remains unchanged.
                await asyncio.sleep(self._fault_stage_delay_seconds)
            event = FaultStageEvent(
                stage=FaultStage.TOOL,
                tenant_id=context.tenant_id,
                turn_id=turn_id,
                execution_key=key,
                worker_id=self._worker_id,
            )
            await self._fault_stages.checkpoint(event)
        try:
            result = await call()
        except BaseException:
            if replay_terminal:
                # The terminal success is authoritative.  A best-effort
                # result reconstruction failure must not downgrade it to
                # failed/ambiguous or reopen a side-effect attempt.
                raise
            status = (
                ExecutionStatus.FAILED if risk == ToolRisk.IDEMPOTENT else ExecutionStatus.AMBIGUOUS
            )
            await self._ledger.finish(
                key,
                tenant_id=context.tenant_id,
                status=status,
                owner_id=owner_id,
                fencing_token=fencing_token,
            )
            if status == ExecutionStatus.AMBIGUOUS:
                raise HumanReviewRequired("non-idempotent tool outcome is unknown") from None
            raise
        if not replay_terminal:
            await self._ledger.finish(
                key,
                tenant_id=context.tenant_id,
                status=ExecutionStatus.SUCCEEDED,
                result=result,
                owner_id=owner_id,
                fencing_token=fencing_token,
            )
        return result


__all__ = [
    "ExecutionLedger",
    "ExecutionRecord",
    "ExecutionStatus",
    "HumanReviewRequired",
    "InMemoryExecutionLedger",
    "ToolExecutor",
]
