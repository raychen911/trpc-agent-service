"""Idempotent tool execution and ambiguity handling."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any


class ExecutionStatus(str, Enum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True)
class ExecutionRecord:
    key: str
    status: ExecutionStatus
    result: Any = None
    fresh: bool = False


class HumanReviewRequired(RuntimeError):
    pass


class InMemoryExecutionLedger:
    def __init__(self) -> None:
        self.records: dict[str, ExecutionRecord] = {}

    async def begin(self, key: str) -> ExecutionRecord:
        record = self.records.get(key)
        if record is not None:
            return record
        record = ExecutionRecord(key, ExecutionStatus.STARTED, fresh=True)
        self.records[key] = record
        return record

    async def finish(self, key: str, status: ExecutionStatus, result: Any = None) -> None:
        self.records[key] = ExecutionRecord(key, status, result)


class ToolExecutor:
    def __init__(self, key: bytes, ledger: Any | None = None) -> None:
        if len(key) < 32:
            raise ValueError("tool execution key must contain at least 32 bytes")
        self.key = key
        self.ledger = ledger or InMemoryExecutionLedger()

    def execution_key(
        self, tenant_id: str, session_id: str, turn_id: str, tool_name: str, arguments: dict[str, Any]
    ) -> str:
        canonical = json.dumps(
            [tenant_id, session_id, turn_id, tool_name, arguments], sort_keys=True, separators=(",", ":"), default=str
        )
        return hmac.new(self.key, canonical.encode(), hashlib.sha256).hexdigest()

    async def execute(
        self,
        tenant_id: str,
        session_id: str,
        turn_id: str,
        tool_name: str,
        arguments: dict[str, Any],
        call: Callable[[], Awaitable[Any]],
        *,
        idempotent: bool,
    ) -> Any:
        key = self.execution_key(tenant_id, session_id, turn_id, tool_name, arguments)
        record = await self.ledger.begin(key)
        if record.status == ExecutionStatus.SUCCEEDED:
            return record.result
        if not record.fresh and record.status in {ExecutionStatus.STARTED, ExecutionStatus.AMBIGUOUS} and not idempotent:
            raise HumanReviewRequired("non-idempotent tool outcome is unknown")
        try:
            result = await call()
        except BaseException:
            status = ExecutionStatus.FAILED if idempotent else ExecutionStatus.AMBIGUOUS
            await self.ledger.finish(key, status)
            if not idempotent:
                raise HumanReviewRequired("non-idempotent tool outcome is unknown") from None
            raise
        await self.ledger.finish(key, ExecutionStatus.SUCCEEDED, result)
        return result
